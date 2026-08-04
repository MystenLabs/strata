//! Durability sync: segment fsync, published-LSN advancement, and store-WAL
//! reclamation.

use std::time::Instant;

use strata_core::{StoreCheckpoint, StrataLsn};

use crate::{
    Error, Result, StoreSyncProfile, WriteCoordinator, maintenance::publish_blob_lsm_edit,
    profile_phase, publish_segment_allocation_baseline,
};

impl WriteCoordinator {
    /// Syncs the two store-owned append streams and returns their physical coordinates.
    ///
    /// The returned value has no logical frontier. Its LSN is the `PublishedLsn` written beside it
    /// in the same RocksDB batch by the caller.
    pub(crate) fn sync_store_files(&mut self) -> Result<StoreCheckpoint> {
        self.segment.sync_data()?;
        let wal_position = self.wal.sync()?;
        self.wal.wait_for_sync(wal_position)?;
        Ok(StoreCheckpoint {
            wal_position,
            active_segment_id: self.segment.segment_id(),
            active_segment_offset: self.segment.write_offset(),
        })
    }

    /// Advances the blob projection, then reclaims the store WAL it alone consumes.
    fn reclaim_store_wal(&mut self, published_lsn: StrataLsn) -> Result<()> {
        self.lsm.materialize_through(published_lsn, |edit| {
            publish_blob_lsm_edit(&self.index, edit)
        })?;
        let reclaim_through = self.lsm.manifest().materialized_through.unwrap_or_default();
        if reclaim_through == 0 {
            return Ok(());
        }

        let retained_from = self.wal.retained_from_after(reclaim_through)?;
        let persisted = self.index.get_store_wal_retained_from()?;
        let current = persisted.unwrap_or(self.lsm.manifest().wal_retained_from);
        if retained_from > current || persisted.is_none() {
            // Publish the store-owned deletion boundary before unlinking anything. Falling back
            // to the manifest migrates databases written when this value lived on the blob LSM.
            let mut batch = self.index.batch();
            self.index
                .put_store_wal_retained_from_batch(&mut batch, retained_from.max(current))?;
            batch
                .write_with_sync(true)
                .map_err(strata_index::Error::from)?;
        }
        self.wal.reclaim_through(reclaim_through)?;
        Ok(())
    }

    /// The durability step. The ordering here is the single most load bearing thing in this
    /// file:
    ///
    /// 1. fsync the store-owned payload segment, then the store WAL,
    /// 2. then write durable offsets, allocation baseline, and published_lsn to the index,
    /// 3. then fsync the RocksDB WAL.
    ///
    /// Bytes become durable strictly before the metadata that claims they are. A crash between
    /// any two steps leaves the index claiming *less* than what's on disk — never more — and
    /// recovery re-derives the frontier (it can even promote bytes the crash interrupted us from
    /// claiming). Reversing 1 and 2 would let a persisted published_lsn point at bytes that never
    /// reached the platter, which is the one lie this design must never tell, because the Walrus
    /// event cursor advances based on it.
    ///
    /// The allocation baseline, store checkpoint, and published_lsn share one RocksDB batch, so
    /// compaction cannot retire a published record before GC knows that record started live.
    pub(crate) fn sync_data(&mut self, mut profile: Option<&mut StoreSyncProfile>) -> Result<()> {
        let started = Instant::now();
        let previous_durable_offset = self.durable_offset;
        let durable_offset = self.active_segment_state.write_offset;
        let durability_result = profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.segment_sync += elapsed,
            || self.sync_store_files(),
        );
        let store_checkpoint = match durability_result {
            Ok(checkpoint) => checkpoint,
            Err(error) => {
                self.metrics.record_sync(Err(()), started.elapsed());
                return Err(error.into());
            }
        };
        let committed_lsn = self.index.get_next_lsn()?.saturating_sub(1);
        if self.wal.last_lsn().unwrap_or_default() != committed_lsn
            || store_checkpoint.active_segment_id != self.active_segment_state.segment_id
            || store_checkpoint.active_segment_offset != durable_offset
        {
            let error = Error::InvariantViolation {
                reason: format!(
                    "store checkpoint {store_checkpoint:?} does not match store LSN {committed_lsn} and active segment {} at {durable_offset}",
                    self.active_segment_state.segment_id
                ),
            };
            self.metrics.record_sync(Err(()), started.elapsed());
            self.halt_writer_error("sync store checkpoint", &error);
            return Err(error);
        }
        let _publish_guard = self
            .durability_publish_lock
            .lock()
            .expect("durability publish lock poisoned");
        let durable_relocation_lsn = self.relocations.lsm().last_lsn()?.unwrap_or_default();
        let (state, batch, published_lsn) = profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.published_lsn_compute += elapsed,
            || {
                let mut state = self.active_segment_state.clone();
                if let Some(existing) = self
                    .index
                    .get_segment_state(self.active_segment_state.segment_id)?
                {
                    state.volume_id = existing.volume_id;
                    state.path = existing.path;
                    state.placement_class = existing.placement_class;
                    state.state = existing.state;
                    state.min_lsn = existing.min_lsn;
                    state.max_lsn = existing.max_lsn;
                    state.sealed_before_lsn = existing.sealed_before_lsn;
                    state.sealed_len = existing.sealed_len;
                    state.sealed_sha256 = existing.sealed_sha256;
                }
                state.write_offset = self.active_segment_state.write_offset;
                state.durable_offset = durable_offset;
                let mut batch = self.index.batch();
                self.index.put_segment_state_batch(&mut batch, &state)?;
                publish_segment_allocation_baseline(
                    &self.index,
                    &mut batch,
                    state.segment_id,
                    durable_offset,
                    self.pending_allocation_records,
                )?;
                let current_published_lsn = self.index.get_published_lsn()?;
                if current_published_lsn > committed_lsn {
                    return Err(Error::InvariantViolation {
                        reason: format!(
                            "published LSN {current_published_lsn} follows committed LSN {committed_lsn}"
                        ),
                    });
                }
                let published_lsn = committed_lsn;
                self.index
                    .put_published_lsn_batch(&mut batch, published_lsn)?;
                self.index
                    .put_store_checkpoint_batch(&mut batch, store_checkpoint)?;
                Ok::<_, Error>((state, batch, published_lsn))
            },
        )?;
        let commit_result = profile_phase(
            profile.as_deref_mut(),
            |profile, elapsed| profile.index_batch_commit += elapsed,
            || {
                batch
                    .write_with_sync(true)
                    .map_err(strata_index::Error::from)
            },
        );
        if let Err(error) = commit_result {
            let error = Error::from(error);
            self.metrics.record_sync(Err(()), started.elapsed());
            self.halt_writer_error("sync metadata commit", &error);
            return Err(error);
        }
        self.active_segment_state = state;
        self.durable_relocation_lsn
            .fetch_max(durable_relocation_lsn, std::sync::atomic::Ordering::Release);
        self.pending_allocation_records = 0;
        self.last_durability_publish_at = Instant::now();
        profile_phase(
            profile,
            |profile, elapsed| profile.state_update += elapsed,
            || {
                self.durable_offset = durable_offset;
                self.active_segment_state.durable_offset = durable_offset;
                self.metrics.set_active_segment(
                    self.active_segment_state.segment_id,
                    self.active_segment_state.write_offset,
                    self.durable_offset,
                );
                self.metrics.set_published_lsn(published_lsn);
            },
        );
        // Reclaiming the WAL may publish LSM materialization frontiers. Those writes do not need
        // to share the durability-publication mutex: published_lsn and the store checkpoint are
        // already committed, and materialization can only lag that durable frontier.
        drop(_publish_guard);
        if let Err(error) = self.reclaim_store_wal(published_lsn) {
            self.halt_writer_error("reclaim store WAL", &error);
            return Err(error);
        }
        self.metrics.record_sync(
            Ok(durable_offset.saturating_sub(previous_durable_offset)),
            started.elapsed(),
        );
        self.gc_concurrency.observe_sync(
            started.elapsed(),
            durable_offset.saturating_sub(previous_durable_offset),
        );
        self.request_lsm_compaction();
        Ok(())
    }
}
