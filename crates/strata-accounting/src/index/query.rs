use super::*;

impl AccountingIndex {
    pub fn materialized_state(&self, key: &BlobKey) -> Result<Option<MaterializedBlobState>> {
        let partition = self.partition_for_key(key);
        self.materialized_state_in_partition(partition, key)
    }

    pub fn current_state(&self, key: &BlobKey) -> Result<Option<MaterializedBlobState>> {
        let partition = self.partition_for_key(key);
        let mut state = self.materialized_state_in_partition(partition, key)?;
        // Current reads add the newest delta runs on top of the materialized base+patch image, but
        // they deliberately discard emitted RefEvents. Reads answer "what is live now"; only
        // compaction publishes the physical accounting transitions that make old bytes collectable.
        for run in &self.partition(partition)?.deltas {
            for update in self.read_delta_run(run)? {
                if update.key() == key {
                    fold_update(&mut state, &update, &mut |_| {});
                }
            }
        }
        Ok(state)
    }

    fn materialized_state_in_partition(
        &self,
        partition: PartitionId,
        key: &BlobKey,
    ) -> Result<Option<MaterializedBlobState>> {
        let partition_manifest = self.partition(partition)?;
        let mut state = if let Some(base) = &partition_manifest.base {
            self.find_state_in_run(base, key)?
        } else {
            None
        };
        // Patch runs are residual history above the base. Folding them here must use manifest order
        // because each patch may contain tombstones or pending maps whose meaning depends on all
        // older materialized state having already been applied.
        for patch in &partition_manifest.patches {
            if let Some(record) = self.find_patch_in_run(patch, key)? {
                for update in record.updates {
                    fold_patch_update(&mut state, &update, &mut |_| {});
                }
            }
        }
        Ok(state)
    }

    fn find_state_in_run(
        &self,
        meta: &RunMeta,
        key: &BlobKey,
    ) -> Result<Option<MaterializedBlobState>> {
        for record in self.state_run_records(meta)? {
            let record = record?;
            match record.key.cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(record.state)),
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less => {}
            }
        }
        Ok(None)
    }

    fn find_patch_in_run(&self, meta: &RunMeta, key: &BlobKey) -> Result<Option<PatchRecord>> {
        for record in self.patch_run_records(meta)? {
            let record = record?;
            match record.key.cmp(key) {
                std::cmp::Ordering::Equal => return Ok(Some(record)),
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Less => {}
            }
        }
        Ok(None)
    }
}
