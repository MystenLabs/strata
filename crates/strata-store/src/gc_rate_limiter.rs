use std::{
    sync::Mutex,
    thread,
    time::{Duration, Instant},
};

const NANOS_PER_SEC: u128 = 1_000_000_000;

/// Shared token bucket for background GC disk I/O.
///
/// The limiter charges bytes before GC issues blocking reads or writes. It is intentionally shared
/// by every GC worker in a store namespace, so increasing worker concurrency cannot multiply the
/// configured byte rate.
#[derive(Debug)]
pub(crate) struct GcIoLimiter {
    state: Mutex<GcIoLimiterState>,
}

impl GcIoLimiter {
    pub(crate) fn new(bytes_per_sec: u64) -> Self {
        assert!(bytes_per_sec > 0, "GC I/O budget must be non-zero");
        Self {
            state: Mutex::new(GcIoLimiterState::new(
                bytes_per_sec,
                bytes_per_sec,
                Instant::now(),
            )),
        }
    }

    pub(crate) fn acquire(&self, bytes: u64) {
        let mut remaining = bytes;
        while remaining > 0 {
            let wait = {
                let mut state = self.state.lock().expect("gc I/O limiter lock poisoned");
                let chunk = remaining.min(state.burst_bytes);
                match state.reserve(Instant::now(), chunk) {
                    GcIoReservation::Acquired => {
                        remaining -= chunk;
                        None
                    }
                    GcIoReservation::Wait(wait) => Some(wait),
                }
            };
            if let Some(wait) = wait {
                thread::sleep(wait);
            }
        }
    }

    pub(crate) fn set_bytes_per_sec(&self, bytes_per_sec: u64) {
        assert!(bytes_per_sec > 0, "GC I/O budget must be non-zero");
        let mut state = self.state.lock().expect("gc I/O limiter lock poisoned");
        state.set_bytes_per_sec(Instant::now(), bytes_per_sec);
    }
}

#[derive(Debug)]
struct GcIoLimiterState {
    bytes_per_sec: u64,
    burst_bytes: u64,
    available_scaled: u128,
    last_refill: Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GcIoReservation {
    Acquired,
    Wait(Duration),
}

impl GcIoLimiterState {
    fn new(bytes_per_sec: u64, burst_bytes: u64, now: Instant) -> Self {
        assert!(bytes_per_sec > 0, "GC I/O budget must be non-zero");
        assert!(burst_bytes > 0, "GC I/O burst must be non-zero");
        Self {
            bytes_per_sec,
            burst_bytes,
            available_scaled: scaled_bytes(burst_bytes),
            last_refill: now,
        }
    }

    fn reserve(&mut self, now: Instant, bytes: u64) -> GcIoReservation {
        debug_assert!(bytes <= self.burst_bytes);
        if bytes == 0 {
            return GcIoReservation::Acquired;
        }

        self.refill(now);
        let needed = scaled_bytes(bytes);
        if self.available_scaled >= needed {
            self.available_scaled -= needed;
            return GcIoReservation::Acquired;
        }

        let deficit = needed - self.available_scaled;
        GcIoReservation::Wait(duration_for_scaled_bytes(deficit, self.bytes_per_sec))
    }

    fn refill(&mut self, now: Instant) {
        if now <= self.last_refill {
            return;
        }

        let elapsed_nanos = now.duration_since(self.last_refill).as_nanos();
        let added_scaled = elapsed_nanos.saturating_mul(self.bytes_per_sec as u128);
        let capacity_scaled = scaled_bytes(self.burst_bytes);
        self.available_scaled = self
            .available_scaled
            .saturating_add(added_scaled)
            .min(capacity_scaled);
        self.last_refill = now;
    }

    fn set_bytes_per_sec(&mut self, now: Instant, bytes_per_sec: u64) {
        assert!(bytes_per_sec > 0, "GC I/O budget must be non-zero");
        self.refill(now);
        self.bytes_per_sec = bytes_per_sec;
        self.burst_bytes = bytes_per_sec;
        self.available_scaled = self.available_scaled.min(scaled_bytes(self.burst_bytes));
    }
}

fn scaled_bytes(bytes: u64) -> u128 {
    (bytes as u128).saturating_mul(NANOS_PER_SEC)
}

fn duration_for_scaled_bytes(scaled_bytes: u128, bytes_per_sec: u64) -> Duration {
    let nanos = scaled_bytes.div_ceil(bytes_per_sec as u128);
    Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserve_consumes_initial_burst_without_waiting() {
        let now = Instant::now();
        let mut state = GcIoLimiterState::new(100, 100, now);

        assert_eq!(state.reserve(now, 40), GcIoReservation::Acquired);
        assert_eq!(state.reserve(now, 60), GcIoReservation::Acquired);
        assert_eq!(
            state.reserve(now, 1),
            GcIoReservation::Wait(Duration::from_millis(10))
        );
    }

    #[test]
    fn reserve_refills_with_elapsed_time() {
        let now = Instant::now();
        let mut state = GcIoLimiterState::new(100, 100, now);

        assert_eq!(state.reserve(now, 100), GcIoReservation::Acquired);
        assert_eq!(
            state.reserve(now + Duration::from_millis(250), 25),
            GcIoReservation::Acquired
        );
        assert_eq!(
            state.reserve(now + Duration::from_millis(250), 1),
            GcIoReservation::Wait(Duration::from_millis(10))
        );
    }

    #[test]
    fn reserve_caps_refill_at_burst() {
        let now = Instant::now();
        let mut state = GcIoLimiterState::new(100, 50, now);

        assert_eq!(
            state.reserve(now + Duration::from_secs(10), 50),
            GcIoReservation::Acquired
        );
        assert_eq!(
            state.reserve(now + Duration::from_secs(10), 1),
            GcIoReservation::Wait(Duration::from_millis(10))
        );
    }

    #[test]
    fn rate_decrease_clamps_available_tokens_and_future_waits() {
        let now = Instant::now();
        let mut state = GcIoLimiterState::new(100, 100, now);

        state.set_bytes_per_sec(now, 20);

        assert_eq!(state.reserve(now, 20), GcIoReservation::Acquired);
        assert_eq!(
            state.reserve(now, 1),
            GcIoReservation::Wait(Duration::from_millis(50))
        );
    }

    #[test]
    fn rate_increase_uses_new_budget_for_future_refill() {
        let now = Instant::now();
        let mut state = GcIoLimiterState::new(100, 100, now);

        assert_eq!(state.reserve(now, 100), GcIoReservation::Acquired);
        state.set_bytes_per_sec(now, 200);

        assert_eq!(
            state.reserve(now + Duration::from_millis(250), 50),
            GcIoReservation::Acquired
        );
        assert_eq!(
            state.reserve(now + Duration::from_millis(250), 1),
            GcIoReservation::Wait(Duration::from_millis(5))
        );
    }
}
