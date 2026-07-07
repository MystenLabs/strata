use super::*;

fn controller_config(max_workers: usize, initial_workers: usize) -> GcConcurrencyConfig {
    GcConcurrencyConfig {
        max_workers,
        initial_workers,
        tuning_window_cycles: 2,
        sync_impact_threshold: Duration::from_millis(100),
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
    controller.observe_sync(Duration::from_millis(300), 0);

    drop(controller.try_admit().unwrap());
    drop(controller.try_admit().unwrap());

    assert_eq!(controller.active_limit(), 1);
}
