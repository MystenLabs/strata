use super::*;

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
