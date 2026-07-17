use super::super::*;
use super::{delta_updates::compact_delta_updates, merge};

impl AccountingIndex {
    pub fn compact_delta_runs(&mut self, partition: PartitionId) -> Result<CompactionEventBatch> {
        let prepared = self.prepare_delta_compaction(partition)?;
        let event_batch = prepared.event_batch.clone();
        self.apply_prepared_delta_compaction(prepared)?;
        Ok(event_batch)
    }

    pub fn prepare_delta_compaction(
        &self,
        partition: PartitionId,
    ) -> Result<PreparedDeltaCompaction> {
        self.validate_partition(partition)?;
        let input_deltas = self.partition(partition)?.deltas.clone();
        if input_deltas.is_empty() {
            return Ok(PreparedDeltaCompaction {
                event_batch: CompactionEventBatch {
                    partition,
                    ..CompactionEventBatch::default()
                },
                base_generation: self.manifest.generation,
                manifest: self.manifest.clone(),
                obsolete_runs: Vec::new(),
            });
        }

        let mut batch = CompactionEventBatch {
            partition,
            input_run_ids: input_deltas.iter().map(|meta| meta.id).collect(),
            ..CompactionEventBatch::default()
        };
        let mut delta_inputs = Vec::new();
        for run in &input_deltas {
            batch.max_lsn = batch.max_lsn.max(run.max_lsn.unwrap_or(0));
            delta_inputs.push(self.delta_run_records(run)?);
        }

        let base_generation = self.manifest.generation;
        let mut manifest = self.manifest.clone();
        let output_run_id = allocate_run_id(&mut manifest);
        let mut merged = merge::DeltaRunMerger::new(delta_inputs)?;
        let output_records = std::iter::from_fn(|| {
            let group = match merged.next()? {
                Ok(group) => group,
                Err(error) => return Some(Err(error)),
            };
            // Delta compaction is intentionally shallow. It can reorder sorted delta runs into one
            // key-grouped patch and close lifetimes that are fully visible inside those deltas, but
            // it does not read the older base/patch layers. Any residual operation that could affect
            // older materialized state must remain in the patch run for major compaction.
            let updates = compact_delta_updates(&group.key, &group.updates, &mut batch);
            Some(Ok(PatchRecord {
                key: group.key,
                updates,
            }))
        });

        let output_meta =
            self.write_framed_run(output_run_id, RunKind::Patch, partition, output_records)?;
        batch.output_run_id = Some(output_meta.id);

        {
            let partition_manifest = partition_mut(&mut manifest, partition)?;
            // The new patch run is a physical replacement for all input delta runs in this
            // partition. Existing base and patch runs stay in place because their state was not read
            // and therefore cannot be rewritten by this compaction level.
            partition_manifest.deltas.clear();
            partition_manifest.patches.push(output_meta);
        }
        advance_manifest_generation(&mut manifest)?;

        Ok(PreparedDeltaCompaction {
            event_batch: batch,
            base_generation,
            manifest,
            obsolete_runs: input_deltas,
        })
    }

    pub fn apply_prepared_delta_compaction(
        &mut self,
        prepared: PreparedDeltaCompaction,
    ) -> Result<()> {
        self.apply_prepared_manifest(prepared.base_generation, prepared.manifest)?;
        // File cleanup is sequenced after the generation guard. If this prepared compaction was
        // derived from an old manifest, its input files may still be reachable from the current root.
        self.remove_runs(&prepared.obsolete_runs);
        Ok(())
    }
}
