use std::fmt::Debug;

/// Receives successful physical I/O byte counts for segment files.
///
/// The segment crate deliberately does not depend on a metrics implementation. Store owners can
/// attach an observer backed by Prometheus, tracing, or test counters when opening readers and
/// writers.
pub trait SegmentIoObserver: Debug + Send + Sync {
    fn record_read(&self, bytes: u64);
    fn record_write(&self, bytes: u64);
}
