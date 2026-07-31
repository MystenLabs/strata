use super::*;
use strata_core::SegmentGcLifetimeRange;

fn controller_config(max_workers: usize, initial_workers: usize) -> GcConcurrencyConfig {
    GcConcurrencyConfig {
        max_workers,
        initial_workers,
        tuning_window_cycles: 2,
        sync_impact_threshold: Duration::from_millis(100),
        max_io_bytes_per_sec: 32 * 1024 * 1024,
        min_io_bytes_per_sec: 4 * 1024 * 1024,
    }
}

fn gc_test_range(offset: u64, len: u64) -> SegmentGcRecordRange {
    SegmentGcRecordRange { offset, len }
}

fn gc_test_lifecycle(epoch: u64) -> BlobLifecycle {
    BlobLifecycle {
        logical_end_epoch: epoch,
        extension_count: 0,
    }
}

#[test]
fn overlay_record_classifier_advances_through_sorted_ranges() {
    let overlay = SegmentGcOverlay {
        expired: vec![gc_test_range(100, 50)],
        retired: vec![gc_test_range(300, 50)],
        lifetimes: vec![
            SegmentGcLifetimeRange {
                range: gc_test_range(0, 50),
                lifecycle: gc_test_lifecycle(10),
            },
            SegmentGcLifetimeRange {
                range: gc_test_range(200, 50),
                lifecycle: gc_test_lifecycle(20),
            },
        ],
        ..SegmentGcOverlay::default()
    };
    let mut classifier = OverlayRecordClassifier::new(7, &overlay);

    assert_eq!(
        classifier.classify(gc_test_range(0, 50)).unwrap(),
        OverlayRecordState::CopyEligible {
            lifecycle: Some(gc_test_lifecycle(10)),
        }
    );
    assert_eq!(
        classifier.classify(gc_test_range(50, 50)).unwrap(),
        OverlayRecordState::CopyEligible { lifecycle: None }
    );
    assert_eq!(
        classifier.classify(gc_test_range(100, 50)).unwrap(),
        OverlayRecordState::Skip
    );
    assert_eq!(
        classifier.classify(gc_test_range(200, 50)).unwrap(),
        OverlayRecordState::CopyEligible {
            lifecycle: Some(gc_test_lifecycle(20)),
        }
    );
    assert_eq!(
        classifier.classify(gc_test_range(300, 50)).unwrap(),
        OverlayRecordState::Skip
    );
    assert_eq!(
        classifier.classify(gc_test_range(350, 50)).unwrap(),
        OverlayRecordState::CopyEligible { lifecycle: None }
    );
}

#[test]
fn overlay_record_classifier_rejects_partial_overlap() {
    let overlay = SegmentGcOverlay {
        expired: vec![gc_test_range(25, 50)],
        ..SegmentGcOverlay::default()
    };
    let mut classifier = OverlayRecordClassifier::new(7, &overlay);

    let error = classifier.classify(gc_test_range(0, 50)).unwrap_err();
    assert!(matches!(
        error,
        Error::GcOverlayPartialRecordRange {
            segment_id: 7,
            offset: 0,
            len: 50,
        }
    ));
}

#[test]
fn gc_concurrency_controller_limits_admitted_workers() {
    let controller = Arc::new(GcConcurrencyController::new(
        controller_config(2, 1),
        StrataStoreMetrics::default(),
    ));

    let permit = controller.try_admit().unwrap();

    assert!(controller.try_admit().is_none());
    assert_eq!(controller.active_limit(), 1);
    drop(permit);
}

#[test]
fn shard_drop_reservation_blocks_new_claims_while_existing_claim_drains() {
    let claims = Arc::new(GcSourceClaims::default());
    let held = claims.try_claim(BTreeSet::from([5])).unwrap();
    let waiter_claims = Arc::clone(&claims);
    let (result_tx, result_rx) = mpsc::channel();
    let waiter = std::thread::spawn(move || {
        let claim = waiter_claims
            .claim_when_available(BTreeSet::from([1, 2, 3, 4, 5]), Duration::from_secs(2));
        result_tx.send(claim.is_some()).unwrap();
        drop(claim);
    });

    let reservation_deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match claims.try_claim(BTreeSet::from([1])) {
            Some(claim) if Instant::now() < reservation_deadline => {
                drop(claim);
                std::thread::yield_now();
            }
            Some(_) => panic!("drop reservation was not active"),
            None => break,
        }
    }
    for segment_id in 1..=4 {
        assert!(claims.try_claim(BTreeSet::from([segment_id])).is_none());
    }

    drop(held);
    assert!(result_rx.recv_timeout(Duration::from_secs(1)).unwrap());
    waiter.join().unwrap();
}

#[test]
fn shard_drop_claim_timeout_releases_draining_reservation() {
    let claims = Arc::new(GcSourceClaims::default());
    let held = claims.try_claim(BTreeSet::from([1])).unwrap();

    assert!(
        claims
            .claim_when_available(BTreeSet::from([1, 2]), Duration::from_millis(10))
            .is_none()
    );
    assert!(claims.try_claim(BTreeSet::from([2])).is_some());
    drop(held);
}

#[test]
fn gc_concurrency_controller_increases_after_healthy_window() {
    let controller = Arc::new(GcConcurrencyController::new(
        controller_config(2, 1),
        StrataStoreMetrics::default(),
    ));

    drop(controller.try_admit().unwrap());
    drop(controller.try_admit().unwrap());

    assert_eq!(controller.active_limit(), 2);
}

#[test]
fn gc_failure_backoff_keeps_cadence_then_doubles_up_to_cap() {
    let interval = Duration::from_secs(60);

    assert_eq!(gc_failure_backoff(interval, 0), interval);
    assert_eq!(gc_failure_backoff(interval, 1), interval);
    assert_eq!(gc_failure_backoff(interval, 2), interval * 2);
    assert_eq!(gc_failure_backoff(interval, 3), interval * 4);
    assert_eq!(gc_failure_backoff(interval, 5), interval * 16);
    assert_eq!(gc_failure_backoff(interval, u32::MAX), interval * 16);
}

#[test]
fn gc_concurrency_controller_decreases_after_sync_pressure() {
    let controller = Arc::new(GcConcurrencyController::new(
        controller_config(2, 2),
        StrataStoreMetrics::default(),
    ));

    let permit = controller.try_admit().unwrap();
    controller.observe_sync(Duration::from_millis(300), 0);
    drop(permit);
    drop(controller.try_admit().unwrap());

    assert_eq!(controller.active_limit(), 1);
}

#[test]
fn gc_concurrency_controller_decreases_io_budget_after_sync_pressure() {
    let controller = Arc::new(GcConcurrencyController::new(
        controller_config(2, 2),
        StrataStoreMetrics::default(),
    ));

    let permit = controller.try_admit().unwrap();
    controller.observe_sync(Duration::from_millis(300), 0);
    drop(permit);
    drop(controller.try_admit().unwrap());

    assert_eq!(controller.active_io_bytes_per_sec(), 16 * 1024 * 1024);
}

#[test]
fn gc_concurrency_controller_does_not_back_off_for_pressure_before_gc_activity() {
    let controller = Arc::new(GcConcurrencyController::new(
        controller_config(2, 2),
        StrataStoreMetrics::default(),
    ));
    controller.observe_sync(Duration::from_millis(300), 0);

    drop(controller.try_admit().unwrap());
    drop(controller.try_admit().unwrap());

    assert_eq!(controller.active_limit(), 2);
    assert_eq!(controller.active_io_bytes_per_sec(), 32 * 1024 * 1024);
}

#[test]
fn gc_concurrency_controller_increases_io_budget_after_healthy_window() {
    let controller = Arc::new(GcConcurrencyController::new(
        controller_config(2, 2),
        StrataStoreMetrics::default(),
    ));
    let permit = controller.try_admit().unwrap();
    controller.observe_sync(Duration::from_millis(300), 0);
    drop(permit);
    drop(controller.try_admit().unwrap());
    assert_eq!(controller.active_io_bytes_per_sec(), 16 * 1024 * 1024);

    observe_sync_millis(&controller, 80, 128);
    drop(controller.try_admit().unwrap());
    assert_eq!(controller.active_io_bytes_per_sec(), 16 * 1024 * 1024);
    drop(controller.try_admit().unwrap());
    drop(controller.try_admit().unwrap());

    assert_eq!(controller.active_io_bytes_per_sec(), 32 * 1024 * 1024);
}

#[test]
fn tuned_signal_baseline_decays_after_sustained_workload_shift() {
    let mut signal = TunedSignal::default();
    observe_millis(&mut signal, 50, 32);

    observe_millis(&mut signal, 120, 96);

    assert!(!signal.degraded(Duration::from_millis(100)));
}

#[test]
fn tuned_signal_still_detects_short_latency_regression() {
    let mut signal = TunedSignal::default();
    observe_millis(&mut signal, 120, 64);

    observe_millis(&mut signal, 240, 6);

    assert!(signal.degraded(Duration::from_millis(100)));
}

#[test]
fn tuned_signal_baseline_drops_immediately_after_improvement() {
    let mut signal = TunedSignal::default();
    observe_millis(&mut signal, 120, 64);
    observe_millis(&mut signal, 50, 64);

    observe_millis(&mut signal, 80, 8);

    assert!(signal.degraded(Duration::from_millis(40)));
}

fn observe_millis(signal: &mut TunedSignal, millis: u64, count: usize) {
    for _ in 0..count {
        signal.observe(Duration::from_millis(millis).as_nanos());
    }
}

fn observe_sync_millis(controller: &GcConcurrencyController, millis: u64, count: usize) {
    for _ in 0..count {
        controller.observe_sync(Duration::from_millis(millis), 0);
    }
}
