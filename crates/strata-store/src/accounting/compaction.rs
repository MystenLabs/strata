use std::time::Instant;

use strata_accounting::{CompactionEventBatch, Manifest};

use crate::{Result, metrics::AccountingStage};

use super::AccountingProcessor;

impl AccountingProcessor {
    pub(super) fn materialize_shard_drops(&mut self) -> Result<bool> {
        let drops = self.accounting_index.pending_shard_drops();
        let mut materialized = false;
        for drop in drops {
            let prepared = match self.accounting_index.prepare_materialize_shard_drop(drop) {
                Ok(prepared) => prepared,
                Err(strata_accounting::Error::ShardDropRequiresCompaction { .. }) => continue,
                Err(error) => return Err(error.into()),
            };
            let manifest = prepared.manifest().clone();
            self.publish_accounting_transition(Some(&manifest), None, Some(&prepared.event_batch))?;
            self.accounting_index.apply_prepared_shard_drop(prepared)?;
            materialized = true;
        }
        Ok(materialized)
    }

    /// Compacts accounting delta and patch runs partition by partition.
    pub(super) fn compact_accounting_index(
        &mut self,
        force_delta_compaction: bool,
        force_major_compaction: bool,
    ) -> Result<bool> {
        let partitions = self
            .accounting_index
            .manifest()
            .partitions
            .keys()
            .copied()
            .collect::<Vec<_>>();
        let mut should_nudge_gc = false;

        // Collect keys first because applying each prepared change updates the manifest.
        for partition in partitions {
            if self.should_compact_deltas(partition, force_delta_compaction) {
                let input_bytes = self.accounting_index.manifest().partitions[&partition]
                    .deltas
                    .iter()
                    .map(|run| run.file_len)
                    .sum::<u64>();
                let started = Instant::now();
                let prepared = match self.accounting_index.prepare_delta_compaction(partition) {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        self.metrics.record_accounting_stage(
                            AccountingStage::DeltaCompaction,
                            false,
                            started.elapsed(),
                            input_bytes,
                            0,
                        );
                        return Err(error.into());
                    }
                };
                if !prepared.event_batch.input_run_ids.is_empty() {
                    let manifest = prepared.manifest().clone();
                    let output_bytes = output_run_bytes(&manifest, &prepared.event_batch);
                    let committed = self.publish_accounting_transition(
                        Some(&manifest),
                        None,
                        Some(&prepared.event_batch),
                    );
                    let published = committed.is_ok();
                    self.metrics.record_accounting_stage(
                        AccountingStage::DeltaCompaction,
                        published,
                        started.elapsed(),
                        input_bytes,
                        output_bytes,
                    );
                    should_nudge_gc |= committed?;
                    self.accounting_index
                        .apply_prepared_delta_compaction(prepared)?;
                }
            }

            if self.should_major_compact(partition, force_major_compaction) {
                let input = &self.accounting_index.manifest().partitions[&partition];
                let input_bytes = input
                    .base
                    .iter()
                    .chain(input.patches.iter())
                    .map(|run| run.file_len)
                    .sum::<u64>();
                let started = Instant::now();
                let prepared = match self
                    .accounting_index
                    .prepare_major_compact_partition(partition)
                {
                    Ok(prepared) => prepared,
                    Err(error) => {
                        self.metrics.record_accounting_stage(
                            AccountingStage::MajorCompaction,
                            false,
                            started.elapsed(),
                            input_bytes,
                            0,
                        );
                        return Err(error.into());
                    }
                };
                if prepared.output.is_some() {
                    let output_bytes = prepared.output.as_ref().map_or(0, |run| run.file_len);
                    let manifest = prepared.manifest().clone();
                    let committed = self.publish_accounting_transition(
                        Some(&manifest),
                        None,
                        Some(&prepared.event_batch),
                    );
                    let published = committed.is_ok();
                    self.metrics.record_accounting_stage(
                        AccountingStage::MajorCompaction,
                        published,
                        started.elapsed(),
                        input_bytes,
                        output_bytes,
                    );
                    should_nudge_gc |= committed?;
                    self.accounting_index
                        .apply_prepared_major_compaction(prepared)?;
                }
            }
        }
        Ok(should_nudge_gc)
    }

    fn should_compact_deltas(&self, partition: u32, force: bool) -> bool {
        let partition = self
            .accounting_index
            .manifest()
            .partitions
            .get(&partition)
            .expect("partition was read from manifest");
        if partition.deltas.is_empty() {
            return false;
        }
        let delta_bytes = partition.deltas.iter().map(|run| run.file_len).sum::<u64>();
        force
            || count_threshold_reached(
                partition.deltas.len(),
                self.config.accounting_delta_run_count_threshold,
            )
            || bytes_threshold_reached(
                delta_bytes,
                self.config.accounting_delta_run_bytes_threshold,
            )
    }

    fn should_major_compact(&self, partition: u32, force: bool) -> bool {
        let partition = self
            .accounting_index
            .manifest()
            .partitions
            .get(&partition)
            .expect("partition was read from manifest");
        if partition.patches.is_empty() {
            return false;
        }
        let patch_bytes = partition
            .patches
            .iter()
            .map(|run| run.file_len)
            .sum::<u64>();
        force
            || count_threshold_reached(
                partition.patches.len(),
                self.config.accounting_major_patch_count_threshold,
            )
            || bytes_threshold_reached(
                patch_bytes,
                self.config.accounting_major_patch_bytes_threshold,
            )
    }
}

fn output_run_bytes(manifest: &Manifest, event_batch: &CompactionEventBatch) -> u64 {
    let Some(output_run_id) = event_batch.output_run_id else {
        return 0;
    };
    manifest
        .partitions
        .values()
        .flat_map(|partition| {
            partition
                .base
                .iter()
                .chain(partition.patches.iter())
                .chain(partition.deltas.iter())
        })
        .find(|run| run.id == output_run_id)
        .map_or(0, |run| run.file_len)
}

/// Interprets a zero count threshold as disabled.
pub(super) fn count_threshold_reached(value: usize, threshold: usize) -> bool {
    threshold != 0 && value >= threshold
}

/// Interprets a zero byte threshold as disabled.
fn bytes_threshold_reached(value: u64, threshold: u64) -> bool {
    threshold != 0 && value >= threshold
}
