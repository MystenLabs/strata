//! The materialized per-blob state machine applied during full merges.

use strata_core::{BlobLifecycle, Epoch, GarbageEvent, RecordRef, ShardKey};
use strata_lsm::{GarbageRecord, Result, StrataLsn};

use super::format::{BlobLifetime, BlobMutation, BlobMutationWithLSN, BlobState, BlobVersion};
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
            let Some(transition) = snapshot.expiry(lifecycle.logical_end_epoch) else {
                continue;
            };
            self.versions.remove(&shard);
            if transition.emit_garbage {
                emit_record(
                    key,
                    transition.lsn,
                    version,
                    Some(lifecycle),
                    GarbageEvent::Expired {
                        record: version.record_ref,
                    },
                    emit,
                )?;
            }
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
            for version in self.versions.values() {
                emit_record(
                    key,
                    lsn,
                    *version,
                    effective_lifecycle(previous, version),
                    GarbageEvent::Expired {
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

fn effective_lifecycle(
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
