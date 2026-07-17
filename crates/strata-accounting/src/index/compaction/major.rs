use super::super::*;
use super::merge;

impl AccountingIndex {
    pub fn major_compact_partition(&mut self, partition: PartitionId) -> Result<Option<RunMeta>> {
        let prepared = self.prepare_major_compact_partition(partition)?;
        let output = prepared.output.clone();
        self.apply_prepared_major_compaction(prepared)?;
        Ok(output)
    }

    pub fn prepare_major_compact_partition(
        &self,
        partition: PartitionId,
    ) -> Result<PreparedMajorCompaction> {
        self.validate_partition(partition)?;
        let old_base = self.partition(partition)?.base.clone();
        let old_patches = self.partition(partition)?.patches.clone();
        if old_base.is_none() && old_patches.is_empty() {
            return Ok(PreparedMajorCompaction {
                output: None,
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
            input_run_ids: old_base
                .iter()
                .chain(old_patches.iter())
                .map(|meta| meta.id)
                .collect(),
            max_lsn: old_base
                .iter()
                .chain(old_patches.iter())
                .filter_map(|meta| meta.max_lsn)
                .max()
                .unwrap_or_default(),
            ..CompactionEventBatch::default()
        };
        let base_records = old_base
            .as_ref()
            .map(|base| self.state_run_records(base))
            .transpose()?;
        let mut patch_inputs = Vec::new();
        for (index, patch) in old_patches.iter().enumerate() {
            patch_inputs.push(merge::PatchRunInput {
                precedence: index + 1,
                records: self.patch_run_records(patch)?,
            });
        }
        let mut merged = merge::BasePatchMerger::new(base_records, patch_inputs)?;

        let base_generation = self.manifest.generation;
        let mut manifest = self.manifest.clone();
        let output_run_id = allocate_run_id(&mut manifest);
        let output_records = std::iter::from_fn(|| {
            let merged_key = match merged.next()? {
                Ok(merged_key) => merged_key,
                Err(error) => return Some(Err(error)),
            };
            // Major compaction is the first point where residual patch history can be interpreted
            // against the older materialized row. Folding here is what turns "there was a tombstone
            // in a patch" into "the base ref is now retired" and what resolves pending MapRef
            // rewrites whose source ref lived in the base.
            let mut state = merged_key.base_state;
            for update in &merged_key.patch_updates {
                fold_patch_update(&mut state, update, &mut |event| {
                    batch.record_event(event);
                });
            }
            Some(Ok(StateRecord {
                key: merged_key.key,
                state: state.unwrap_or_default(),
            }))
        });
        let output_meta =
            self.write_framed_run(output_run_id, RunKind::Base, partition, output_records)?;
        batch.output_run_id = Some(output_meta.id);
        batch.max_lsn = batch.max_lsn.max(output_meta.max_lsn.unwrap_or_default());

        {
            let partition_manifest = partition_mut(&mut manifest, partition)?;
            // The output base run is the new materialized image of base plus every patch. Once the
            // manifest points at it, old base/patch files are obsolete, and future materialized reads
            // no longer need to replay those patches.
            partition_manifest.base = Some(output_meta.clone());
            partition_manifest.patches.clear();
            partition_manifest.materialized_through_lsn = partition_manifest
                .materialized_through_lsn
                .max(batch.max_lsn);
        }
        advance_manifest_generation(&mut manifest)?;

        let mut obsolete_runs = old_patches;
        if let Some(base) = old_base.as_ref() {
            obsolete_runs.push(base.clone());
        }
        Ok(PreparedMajorCompaction {
            output: Some(output_meta),
            event_batch: batch,
            base_generation,
            manifest,
            obsolete_runs,
        })
    }

    pub fn apply_prepared_major_compaction(
        &mut self,
        prepared: PreparedMajorCompaction,
    ) -> Result<()> {
        self.apply_prepared_manifest(prepared.base_generation, prepared.manifest)?;
        // The replacement base is live only after the manifest swap. Until then, old base/patch
        // files remain the readable state of the partition and must not be deleted.
        self.remove_runs(&prepared.obsolete_runs);
        Ok(())
    }
}
