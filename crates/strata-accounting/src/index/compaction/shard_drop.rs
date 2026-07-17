use super::super::*;

impl AccountingIndex {
    pub fn pending_shard_drops(&self) -> Vec<crate::ShardDrop> {
        self.manifest
            .shard_drops
            .iter()
            .copied()
            .filter(|drop| !drop.materialized)
            .collect()
    }

    pub fn prepare_materialize_shard_drop(
        &self,
        drop: crate::ShardDrop,
    ) -> Result<PreparedShardDrop> {
        let mut manifest = self.manifest.clone();
        let mut event_batch = CompactionEventBatch {
            max_lsn: drop.lsn,
            ..CompactionEventBatch::default()
        };
        let mut obsolete_runs = Vec::new();

        for partition in 0..manifest.partition_count {
            let partition_manifest = self.partition(partition)?;
            if !partition_manifest.deltas.is_empty() || !partition_manifest.patches.is_empty() {
                return Err(Error::ShardDropRequiresCompaction {
                    shard: drop.shard,
                    lsn: drop.lsn,
                });
            }

            let old_base = partition_manifest.base.clone();
            let mut changed = false;
            let mut records = Vec::new();
            if let Some(base) = old_base.as_ref() {
                for record in self.state_run_records(base)? {
                    let mut record = record?;
                    if let Some(payload) = record.state.payloads.remove(&drop.shard) {
                        changed = true;
                        record.state.head_lsn = record.state.head_lsn.max(drop.lsn);
                        if payload.owner == SegmentOwner::Store {
                            event_batch.record_event(RefEvent::Retired {
                                lsn: drop.lsn,
                                key: record.key.clone(),
                                shard: drop.shard,
                                record_ref: payload.record_ref,
                                lifecycle: record.state.lifecycle_value(),
                                reason: RetireReason::ShardDropped,
                            });
                        }
                    }
                    records.push(record);
                }
            }

            let replacement = if changed {
                let run_id = allocate_run_id(&mut manifest);
                Some(self.write_framed_run(
                    run_id,
                    RunKind::Base,
                    partition,
                    records.into_iter().map(Ok),
                )?)
            } else {
                old_base.clone()
            };
            let partition_manifest = partition_mut(&mut manifest, partition)?;
            partition_manifest.base = replacement;
            partition_manifest.materialized_through_lsn =
                partition_manifest.materialized_through_lsn.max(drop.lsn);
            if changed && let Some(old_base) = old_base {
                obsolete_runs.push(old_base);
            }
        }

        let Some(manifest_drop) = manifest
            .shard_drops
            .iter_mut()
            .find(|candidate| candidate.shard == drop.shard && candidate.lsn == drop.lsn)
        else {
            return Err(Error::CorruptRun {
                path: self.config.root_dir.clone(),
                reason: format!(
                    "pending shard drop {:?} at LSN {} is absent from manifest",
                    drop.shard, drop.lsn
                ),
            });
        };
        manifest_drop.materialized = true;
        advance_manifest_generation(&mut manifest)?;

        Ok(PreparedShardDrop {
            drop,
            event_batch,
            base_generation: self.manifest.generation,
            manifest,
            obsolete_runs,
        })
    }

    pub fn apply_prepared_shard_drop(&mut self, prepared: PreparedShardDrop) -> Result<()> {
        self.apply_prepared_manifest(prepared.base_generation, prepared.manifest)?;
        self.remove_runs(&prepared.obsolete_runs);
        Ok(())
    }
}
