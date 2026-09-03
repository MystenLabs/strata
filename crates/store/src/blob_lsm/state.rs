//! The materialized per-blob state machine applied during full merges.

use core_types::{BlobLifecycle, Epoch, GarbageEvent, RecordRef, ShardKey};
use lsm::{GarbageRecord, Result, StrataLsn};

use super::format::{
    BlobLifetime, BlobMutation, BlobMutationWithLSN, BlobState, BlobVersion, invalid,
};
use super::garbage::{emit_lifetime_change, emit_record};
use super::snapshot::BlobCompactionSnapshot;

impl BlobState {
    /// Resolves one shard at the current epoch.
    pub fn resolve(
        &self,
        shard: ShardKey,
        current_epoch: Epoch,
    ) -> Option<(&BlobVersion, Option<BlobLifecycle>)> {
        let version = self.versions.get(&shard)?;
        let lifecycle = effective_lifecycle(self.lifetime, version);
        if lifecycle.is_some_and(|lifecycle| lifecycle.logical_end_epoch <= current_epoch) {
            return None;
        }
        Some((version, lifecycle))
    }

    pub(crate) fn apply(
        &mut self,
        key: &[u8],
        mutation: BlobMutationWithLSN,
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<()> {
        let BlobMutationWithLSN { lsn, mutation } = mutation;
        match mutation {
            BlobMutation::Put {
                shard,
                write_epoch,
                record_ref,
            } => self.put(key, lsn, shard, write_epoch, record_ref, emit),
            BlobMutation::SetLifetime {
                logical_end_epoch,
                current_epoch,
            } => self.set_lifetime(key, lsn, logical_end_epoch, current_epoch, emit),
            BlobMutation::Tombstone { shard } => self.tombstone(key, lsn, shard, emit),
            BlobMutation::Relocate {
                shard,
                payload_lsn,
                to,
            } => self.relocate(key, lsn, shard, payload_lsn, to, emit),
        }
    }

    pub(crate) fn prune_with_snapshot(
        &mut self,
        key: &[u8],
        snapshot: &BlobCompactionSnapshot,
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<()> {
        let shards = self.versions.keys().copied().collect::<Vec<_>>();
        for shard in shards {
            let version = self
                .versions
                .get(&shard)
                .copied()
                .expect("shard key was collected from this state");
            let lifecycle = effective_lifecycle(self.lifetime, &version);

            if let Some(transition) = snapshot.shard_retirement(shard) {
                self.versions.remove(&shard);
                let bulk_reclaimed = snapshot
                    .reclaimed_shard_segments
                    .get(&version.record_ref.segment_id)
                    .is_some_and(|owner| *owner == shard);
                if transition.emit_garbage && !bulk_reclaimed {
                    emit_record(
                        key,
                        transition.lsn,
                        version,
                        lifecycle,
                        GarbageEvent::Retired {
                            record: version.record_ref,
                        },
                        emit,
                    )?;
                }
                continue;
            }

            let Some(lifecycle) = lifecycle else {
                continue;
            };
            if snapshot.expiry(lifecycle.logical_end_epoch).is_none() {
                continue;
            }
            // Epoch expiry emits no per-record event. The record's end epoch reached its segment
            // summary as a lifetime hint when the put was first merged, and GC judges that bucket
            // against the clock behind the write-merge frontier. Reporting each expired record
            // here made every full pass over a random-key base scatter an event into every live
            // segment, which is what the sweeper could not keep up with.
            self.versions.remove(&shard);
        }
        Ok(())
    }

    fn put(
        &mut self,
        key: &[u8],
        lsn: StrataLsn,
        shard: ShardKey,
        write_epoch: Epoch,
        record_ref: RecordRef,
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<()> {
        let next = BlobVersion {
            lsn,
            write_epoch,
            record_ref,
        };
        if let Some(previous) = self.versions.insert(shard, next) {
            emit_record(
                key,
                lsn,
                previous,
                effective_lifecycle(self.lifetime, &previous),
                GarbageEvent::Retired {
                    record: previous.record_ref,
                },
                emit,
            )?;
        }
        if let Some(lifecycle) = effective_lifecycle(self.lifetime, &next) {
            emit_lifetime_change(key, lsn, next, None, Some(lifecycle), emit)?;
        }
        Ok(())
    }

    fn set_lifetime(
        &mut self,
        key: &[u8],
        lsn: StrataLsn,
        logical_end_epoch: Epoch,
        current_epoch: Epoch,
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<()> {
        let previous = self.lifetime;
        let previous_expired =
            previous.is_some_and(|head| head.lifecycle.logical_end_epoch <= current_epoch);
        let extension_count = if previous_expired {
            0
        } else {
            previous.map_or(0, |head| head.lifecycle.extension_count.saturating_add(1))
        };
        let next = BlobLifetime {
            lsn,
            lifecycle: BlobLifecycle {
                logical_end_epoch,
                extension_count,
            },
        };

        if previous_expired {
            // A lifetime written after the previous one ended does not revive the ended
            // versions. This is a write, not an epoch transition, and it happens once per key, so
            // it may say so per record: the partial merge reports the same versions as retired
            // when a later put replaces them, and the two paths must agree.
            for version in self.versions.values() {
                emit_record(
                    key,
                    lsn,
                    *version,
                    effective_lifecycle(previous, version),
                    GarbageEvent::Retired {
                        record: version.record_ref,
                    },
                    emit,
                )?;
            }
            self.versions.clear();
        } else {
            for version in self.versions.values() {
                let before = effective_lifecycle(previous, version);
                let after = effective_lifecycle(Some(next), version);
                if before != after {
                    emit_lifetime_change(key, lsn, *version, before, after, emit)?;
                }
            }
        }
        self.lifetime = Some(next);
        Ok(())
    }

    /// Applies a GC relocation pushed through the foreground writer.
    ///
    /// This mirrors compaction healing: only the version whose LSN still matches is moved, and
    /// moving it republishes the lifecycle for the destination bytes so the output segment's
    /// overlay learns the lifetime exactly once. A relocation for a superseded, tombstoned, or
    /// expired version is silently dropped; the compaction relocation scan retires that
    /// born-dead destination, exactly as it does without write-back.
    fn relocate(
        &mut self,
        key: &[u8],
        lsn: StrataLsn,
        shard: ShardKey,
        payload_lsn: StrataLsn,
        to: RecordRef,
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<()> {
        let Some(version) = self
            .versions
            .get_mut(&shard)
            .filter(|version| version.lsn == payload_lsn)
        else {
            return Ok(());
        };
        if version.record_ref.len != to.len {
            return Err(invalid("relocation changed the payload length"));
        }
        if version.record_ref == to {
            return Ok(());
        }
        version.record_ref = to;
        let version = *version;
        if let Some(lifecycle) = effective_lifecycle(self.lifetime, &version) {
            emit_lifetime_change(key, lsn, version, None, Some(lifecycle), emit)?;
        }
        Ok(())
    }

    fn tombstone(
        &mut self,
        key: &[u8],
        lsn: StrataLsn,
        shard: ShardKey,
        emit: &mut dyn FnMut(GarbageRecord) -> Result<()>,
    ) -> Result<()> {
        if let Some(version) = self.versions.remove(&shard) {
            emit_record(
                key,
                lsn,
                version,
                effective_lifecycle(self.lifetime, &version),
                GarbageEvent::Retired {
                    record: version.record_ref,
                },
                emit,
            )?;
        }
        Ok(())
    }
}

pub(crate) fn effective_lifecycle(
    lifetime: Option<BlobLifetime>,
    version: &BlobVersion,
) -> Option<BlobLifecycle> {
    let lifetime = lifetime?;
    if lifetime.lsn <= version.lsn && lifetime.lifecycle.logical_end_epoch <= version.write_epoch {
        None
    } else {
        Some(lifetime.lifecycle)
    }
}
