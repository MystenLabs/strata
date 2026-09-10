//! Runtime API of `StrataStore`: writes and batches, shard management, epochs,
//! durability sync, accessors, and ordered shutdown.

use std::{
    sync::{Arc, mpsc},
    time::{Duration, Instant},
};

use core_types::{BlobKey, Epoch, SegmentId, ShardId, ShardInfo, ShardKey, StrataLsn};
use index::StrataIndex;
use lsm::Lsm;
use tokio::sync::watch;

#[cfg(test)]
use crate::STANDALONE_SHARD;

use crate::{
    AddShardRequest, BatchOp, BatchWriteRequest, BatchWriteResult, DropShardRequest, Error,
    ProfileRequest, Result, StoreSyncProfile, StoreWriteProfile, StrataBatch, StrataStore,
    StrataStoreConfig, StrataStoreMetrics, SyncRequest, WriteCommand, gc::GcCommand,
    maintenance::flush_relocation_lsm,
};

impl StrataStore {
    pub fn config(&self) -> &StrataStoreConfig {
        &self.config
    }

    pub fn index(&self) -> &StrataIndex {
        &self.index
    }

    pub(crate) fn lsm(&self) -> Result<Arc<Lsm>> {
        self.lsm.upgrade().ok_or_else(|| Error::StoreHalted {
            reason: "LSM writer has stopped".to_owned(),
        })
    }

    pub fn metrics(&self) -> &StrataStoreMetrics {
        &self.metrics
    }

    /// Current immutable data-block cache statistics for the main LSM.
    pub fn main_lsm_block_cache_stats(&self) -> Result<lsm::BlockCacheStats> {
        Ok(self.lsm()?.block_cache_stats())
    }

    /// Current immutable data-block cache statistics for the relocation LSM.
    pub fn relocation_lsm_block_cache_stats(&self) -> lsm::BlockCacheStats {
        self.relocations.lsm().block_cache_stats()
    }

    /// Clears resolved relocation pointers without evicting immutable relocation SST blocks.
    pub fn clear_relocation_cache(&self) {
        self.relocation_cache.clear();
    }

    /// Current number of background GC workers the runtime tuner may admit concurrently.
    pub fn gc_active_worker_limit(&self) -> usize {
        if self.config.gc_workers_enabled {
            self.gc_concurrency.active_limit()
        } else {
            0
        }
    }

    /// Current store-wide background GC I/O budget selected by the runtime tuner.
    pub fn gc_active_io_bytes_per_sec(&self) -> u64 {
        if self.config.gc_workers_enabled {
            self.gc_concurrency.active_io_bytes_per_sec()
        } else {
            0
        }
    }

    #[cfg(test)]
    pub(crate) fn shard(&self) -> ShardKey {
        STANDALONE_SHARD
    }

    /// Starts a client-side batch whose operations commit under one store-global LSN allocation.
    ///
    /// Callers that need "put blob, then increment epoch" should not issue
    /// separate commands and hope no other writer interleaves. Without this batch wrapper another
    /// put could land between them and the LSM would record a different history than intended.
    pub fn batch(&self) -> StrataBatch<'_> {
        StrataBatch {
            store: self,
            ops: Vec::new(),
        }
    }

    /// Reads the current shard registry entry.
    ///
    /// Writers must observe generation changes after drop/re-add. A caller
    /// that cached only `shard_id = 7` would otherwise be unable to tell old generation 0 data from
    /// newly-created generation 1 data.
    pub fn shard_info(&self, shard_id: ShardId) -> Result<Option<ShardInfo>> {
        Ok(self.index.get_shard_info(shard_id)?)
    }

    /// Registers a logical shard, or returns its current active generation.
    ///
    /// Shard creation is serialized through the writer so two concurrent
    /// creators cannot both decide that shard 12 starts at generation 0 and race to publish
    /// conflicting registry rows.
    pub fn add_shard(&self, shard_id: ShardId) -> Result<ShardKey> {
        let (response_tx, response_rx) = mpsc::channel();
        self.send_write_command(WriteCommand::AddShard(AddShardRequest {
            shard_id,
            response_tx,
        }))?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)?
    }

    /// Fences a logical shard generation and schedules asynchronous reclamation.
    ///
    /// Like `put`, this returns once the drop is visible, not necessarily crash-durable. Call
    /// `sync` before deleting separately stored shard control state. Keeping sync separate lets
    /// callers batch multiple drops and writes into one durable checkpoint.
    pub fn drop_shard(&self, shard_id: ShardId) -> Result<()> {
        let (response_tx, response_rx) = mpsc::channel();
        self.send_write_command(WriteCommand::DropShard(DropShardRequest {
            shard_id,
            response_tx,
        }))?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)??;
        Ok(())
    }

    /// Writes a blob and returns its LSN. Returning means *visible*, not durable: the bytes are
    /// in the segment file and the index points at them, but only `sync` (or the periodic sync)
    /// makes them crash-safe. Callers that need durability gate on `published_lsn() >= lsn`.
    /// For example, if a caller acknowledges an upstream event immediately after `put` and the
    /// machine loses power before `sync`, recovery may roll the blob back while the upstream event
    /// cursor has already advanced.
    ///
    /// All mutations are funneled through one writer thread (see `WriteCoordinator`), so this
    /// just packages the request and blocks on the response channel.
    pub fn put(&self, shard_id: ShardId, key: &BlobKey, payload: &[u8]) -> Result<StrataLsn> {
        self.put_arc(shard_id, key.clone(), Arc::from(payload))
    }

    /// Writes a blob from shared bytes without forcing the caller to copy them first.
    ///
    /// Write queue can hold the payload until the writer thread reaches
    /// it. Passing borrowed bytes across that boundary would let the caller mutate or drop the
    /// buffer before the segment append actually happens.
    pub fn put_arc(
        &self,
        shard_id: ShardId,
        key: BlobKey,
        payload: Arc<[u8]>,
    ) -> Result<StrataLsn> {
        let result = self.write_batch(vec![BatchOp::Put {
            shard_id,
            key,
            payload,
        }])?;
        result.first_lsn().ok_or(Error::WriteResponseDropped)
    }

    /// Records or updates a blob's logical lifetime without rewriting its payload.
    ///
    /// Lifetime changes are metadata-only LSNs. Blob-LSM compaction applies them during merge and
    /// emits expiration garbage without touching segment bytes. Rewriting the blob just to change
    /// its lifetime would create an unnecessary second payload record.
    pub fn set_blob_lifetime(&self, key: &BlobKey, logical_end_epoch: Epoch) -> Result<StrataLsn> {
        let result = self.write_batch(vec![BatchOp::SetBlobLifetime {
            key: key.clone(),
            logical_end_epoch,
        }])?;
        result.first_lsn().ok_or(Error::WriteResponseDropped)
    }

    /// Appends a logical delete for one blob shard generation.
    ///
    /// A tombstone is an ordered LSN, not an in place removal. If we deleted
    /// the version row immediately, recovery after a crash could resurrect an older payload because
    /// there would be no durable delete marker to hide it. Other shards holding the same key remain
    /// visible.
    pub fn tombstone(&self, shard_id: ShardId, key: &BlobKey) -> Result<StrataLsn> {
        let result = self.write_batch(vec![BatchOp::Tombstone {
            shard_id,
            key: key.clone(),
        }])?;
        Ok(result.first_lsn().unwrap_or(0))
    }

    /// Sends a prepared list of operations to the single writer and waits for the committed result.
    ///
    /// LSNs, segment offsets, and epoch rows must be allocated together by
    /// the owner of the active writer. If callers wrote directly to the index from many threads,
    /// two puts could both publish `next_lsn = 42` while their bytes landed at different offsets.
    pub(crate) fn write_batch(&self, ops: Vec<BatchOp>) -> Result<BatchWriteResult> {
        let (response_tx, response_rx) = mpsc::channel();
        let (profile, profile_rx) = self.profile_channel();
        let command = WriteCommand::Batch(BatchWriteRequest {
            ops,
            response_tx,
            profile,
        });
        let queue_send = self.send_write_command(command)?;
        let result = response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)??;
        self.finish_write_profile(profile_rx, queue_send)?;
        Ok(result)
    }

    /// Drops the cached file descriptor for a segment.
    ///
    /// Segment cleanup must call this before unlinking or reusing a segment path. The read path
    /// checks indexed segment state before serving refs, so an old cached descriptor cannot bypass
    /// a published `Deleted` state.
    pub fn evict_segment_reader(&self, segment_id: SegmentId) {
        self.reader_cache.evict(segment_id);
        self.metrics.record_reader_cache_eviction();
    }

    /// Returns the persisted current epoch.
    pub fn current_epoch(&self) -> Result<Epoch> {
        self.index
            .get_current_epoch()?
            .ok_or(Error::EpochNotInitialized)
    }

    /// Resolves the epoch that was active at a specific LSN.
    ///
    /// Failure mode avoided: snapshot compaction must classify an old write under the epoch that
    /// was true when it happened. Using today's epoch for LSN 25 after several increments would
    /// expire or pin bytes in the wrong bucket.
    pub fn epoch_at_lsn(&self, lsn: StrataLsn) -> Result<Option<Epoch>> {
        Ok(self.index.latest_epoch_at_lsn(lsn)?.map(|(_, epoch)| epoch))
    }

    /// Appends an epoch change operation and returns the new epoch with its LSN.
    ///
    /// Epoch increments consume LSNs so they are ordered with blob writes.
    /// Without that, crash recovery and later compaction could disagree about whether blob A was
    /// written before epoch 9.
    pub fn increment_epoch(&self) -> Result<(Epoch, StrataLsn)> {
        let result = self.write_batch(vec![BatchOp::IncrementEpoch])?;
        let Some(lsn) = result.first_lsn() else {
            return Err(Error::WriteResponseDropped);
        };
        let Some(epoch) = result.epoch_for_op(0) else {
            return Err(Error::WriteResponseDropped);
        };
        Ok((epoch, lsn))
    }

    /// Makes everything written so far crash-safe. Writes are visible immediately but only
    /// durable after a sync — fsyncing per put would destroy throughput on spinning disks, so
    /// durability is batched here. See `WriteCoordinator::start_sync_and_commit` for the
    /// ordering invariant.
    pub fn sync(&self) -> Result<()> {
        let (response_tx, response_rx) = mpsc::channel();
        let (profile, profile_rx) = self.profile_channel();
        let command = WriteCommand::Sync(SyncRequest {
            response_tx,
            profile,
        });
        let queue_send = self.send_write_command(command)?;
        response_rx
            .recv()
            .map_err(|_| Error::WriteResponseDropped)??;
        self.finish_sync_profile(profile_rx, queue_send)?;
        Ok(())
    }

    /// Flushes relocation entries recovered from a legacy shared-WAL database once their active
    /// memtable reaches its normal age or size threshold.
    ///
    /// New GC publications already write immutable relocation L0 tables directly.
    pub fn flush_relocation_memtable_if_due(&self) -> Result<bool> {
        let mut rolled = false;
        for partition in 0..self.config.lsm_partition_count {
            rolled |= self
                .relocations
                .lsm()
                .roll_memtable_if_due(partition)?
                .is_some();
        }
        if !rolled {
            return Ok(false);
        }
        flush_relocation_lsm(
            &self.index,
            &self.relocations,
            &self.relocation_cache,
            &self.metrics,
        )?;
        self.durable_relocation_lsn.fetch_max(
            self.relocations.lsm().last_lsn()?.unwrap_or_default(),
            std::sync::atomic::Ordering::Release,
        );
        Ok(true)
    }

    /// Every operation with `lsn <= published_lsn` survives a crash. This is the value callers
    /// (e.g. the Walrus event cursor) gate on before acknowledging work as done.
    pub fn published_lsn(&self) -> Result<StrataLsn> {
        Ok(self.index.get_committed_lsn()?)
    }

    /// Subscribes to the crash-durable LSN frontier without polling the index.
    ///
    /// The receiver starts with the current frontier, including after recovery. A write with
    /// LSN `n` is durable once `published_lsn >= n`. If the store halts, `halt_reason` is set;
    /// an already-running sync may still advance the durable frontier. The channel closes on
    /// store shutdown.
    pub fn subscribe_durability_progress(&self) -> watch::Receiver<crate::DurabilityProgress> {
        self.store_halt.subscribe_durability_progress()
    }

    /// Enqueues work for the writer and records queue metrics around the send.
    ///
    /// Failure mode avoided: if the writer has exited, this converts the broken channel into a
    /// store error and immediately undoes the queued metric. Otherwise a caller could block on a
    /// response that will never be sent while dashboards show phantom queued work.
    fn send_write_command(&self, command: WriteCommand) -> Result<Duration> {
        self.store_halt.check()?;
        let started = Instant::now();
        self.metrics.enqueue_write_command();
        let result = self
            .write_tx
            .as_ref()
            .ok_or(Error::WriteQueueClosed)
            .and_then(|write_tx| write_tx.send(command).map_err(|_| Error::WriteQueueClosed));
        if result.is_err() {
            self.metrics.dequeue_write_command();
        }
        let elapsed = started.elapsed();
        self.metrics
            .record_write_queue_send(result.is_ok(), elapsed);
        self.gc_concurrency.observe_write_queue_send(elapsed);
        result.map(|_| elapsed)
    }

    fn profile_channel<P>(&self) -> (ProfileRequest<P>, Option<mpsc::Receiver<P>>) {
        if !self.metrics.internal_profile_enabled() {
            return (ProfileRequest::default(), None);
        }

        let (tx, rx) = mpsc::channel();
        (ProfileRequest::enabled(tx), Some(rx))
    }

    fn finish_write_profile(
        &self,
        profile_rx: Option<mpsc::Receiver<StoreWriteProfile>>,
        queue_send: Duration,
    ) -> Result<()> {
        let Some(profile_rx) = profile_rx else {
            return Ok(());
        };
        let mut profile = profile_rx.recv().map_err(|_| Error::WriteResponseDropped)?;
        profile.queue_send = queue_send;
        profile.queue_wait = profile.queue_wait.saturating_sub(queue_send);
        self.metrics.record_write_profile(profile);
        Ok(())
    }

    fn finish_sync_profile(
        &self,
        profile_rx: Option<mpsc::Receiver<StoreSyncProfile>>,
        queue_send: Duration,
    ) -> Result<()> {
        let Some(profile_rx) = profile_rx else {
            return Ok(());
        };
        let mut profile = profile_rx.recv().map_err(|_| Error::WriteResponseDropped)?;
        profile.queue_send = queue_send;
        profile.queue_wait = profile.queue_wait.saturating_sub(queue_send);
        self.metrics.record_sync_profile(profile);
        Ok(())
    }
}

impl Drop for StrataStore {
    fn drop(&mut self) {
        for gc_tx in self.gc_txs.drain(..) {
            let _ = gc_tx.send(GcCommand::Shutdown);
        }
        for gc_handle in self.gc_handles.drain(..) {
            let _ = gc_handle.join();
        }
        if let Some(garbage_sweep_tx) = self.garbage_sweep_tx.take() {
            let _ = garbage_sweep_tx.send(());
        }
        if let Some(garbage_sweep_handle) = self.garbage_sweep_handle.take() {
            let _ = garbage_sweep_handle.join();
        }
        if let Some(write_tx) = self.write_tx.take() {
            let _ = write_tx.send(WriteCommand::Shutdown);
        }
        if let Some(writer_handle) = self.writer_handle.take() {
            let _ = writer_handle.join();
        }
        self.wal_reclaim_tx.take();
        if let Some(wal_reclaim_handle) = self.wal_reclaim_handle.take() {
            let _ = wal_reclaim_handle.join();
        }
        self.lsm_flush_tx.take();
        if let Some(lsm_flush_handle) = self.lsm_flush_handle.take() {
            let _ = lsm_flush_handle.join();
        }
        self.lsm_compact_tx.take();
        if let Some(lsm_compact_handle) = self.lsm_compact_handle.take() {
            let _ = lsm_compact_handle.join();
        }
        for sync_handle in self.lsm_sync_handles.drain(..) {
            let _ = sync_handle.join();
        }
    }
}
