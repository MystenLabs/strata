use super::*;

impl AccountingIndex {
    pub fn append_delta_run(&mut self, updates: Vec<BlobUpdate>) -> Result<Vec<RunMeta>> {
        let prepared = self.prepare_delta_runs(updates)?;
        let metas = prepared.metas.clone();
        self.apply_prepared_delta_runs(prepared)?;
        Ok(metas)
    }

    pub fn prepare_delta_runs(&self, updates: Vec<BlobUpdate>) -> Result<PreparedDeltaRuns> {
        if updates.is_empty() {
            return Err(Error::EmptyDeltaRun);
        }

        // A delta run is made visible by the manifest, not by the file write. The file is written
        // first so the candidate manifest never points at missing bytes, but readers must ignore it
        // until the owner publishes the manifest that names the new `RunMeta`.
        let base_generation = self.manifest.generation;
        let mut manifest = self.manifest.clone();
        let metas = self.write_delta_runs(&mut manifest, updates)?;
        advance_manifest_generation(&mut manifest)?;
        Ok(PreparedDeltaRuns {
            metas,
            base_generation,
            manifest,
        })
    }

    pub fn apply_prepared_delta_runs(&mut self, prepared: PreparedDeltaRuns) -> Result<()> {
        self.apply_prepared_manifest(prepared.base_generation, prepared.manifest)
    }

    pub fn prepare_accounting_deltas(
        &self,
        entries: Vec<AccountingLogEntry>,
    ) -> Result<PreparedAccountingDeltas> {
        let mut blob_updates = Vec::new();
        let mut epoch_changes = Vec::new();
        let mut shard_drops = Vec::new();
        for entry in entries {
            match entry {
                AccountingLogEntry::Blob(update) => blob_updates.push(update),
                AccountingLogEntry::Epoch(change) => epoch_changes.push(change),
                AccountingLogEntry::GcMapRefBatch { base_lsn, maps } => {
                    for (index, map) in maps.into_iter().enumerate() {
                        let offset = index as u64;
                        let lsn = base_lsn
                            .checked_add(offset)
                            .ok_or(Error::LsnOverflow { base_lsn, offset })?;
                        blob_updates.push(BlobUpdate::MapRef {
                            lsn,
                            key: map.key,
                            from: map.from,
                            to: map.to,
                        });
                    }
                }
                AccountingLogEntry::ShardDropped { lsn, shard } => {
                    shard_drops.push(crate::ShardDrop {
                        lsn,
                        shard,
                        materialized: false,
                    });
                }
            }
        }
        if blob_updates.is_empty() && epoch_changes.is_empty() && shard_drops.is_empty() {
            return Err(Error::EmptyDeltaRun);
        }

        let base_generation = self.manifest.generation;
        let mut manifest = self.manifest.clone();
        let delta_metas = if blob_updates.is_empty() {
            Vec::new()
        } else {
            self.write_delta_runs(&mut manifest, blob_updates)?
        };
        for change in &epoch_changes {
            push_epoch_change(&mut manifest, *change);
        }
        for drop in &shard_drops {
            push_shard_drop(&mut manifest, drop.lsn, drop.shard);
        }
        advance_manifest_generation(&mut manifest)?;
        Ok(PreparedAccountingDeltas {
            delta_metas,
            epoch_changes,
            shard_drops,
            base_generation,
            manifest,
        })
    }

    pub fn apply_prepared_accounting_deltas(
        &mut self,
        prepared: PreparedAccountingDeltas,
    ) -> Result<()> {
        self.apply_prepared_manifest(prepared.base_generation, prepared.manifest)
    }

    pub fn record_epoch_change(&mut self, change: EpochChange) -> Result<()> {
        let prepared = self.prepare_epoch_change(change)?;
        self.apply_prepared_epoch_change(prepared)
    }

    pub fn prepare_epoch_change(&self, change: EpochChange) -> Result<PreparedEpochChange> {
        let base_generation = self.manifest.generation;
        let mut manifest = self.manifest.clone();
        push_epoch_change(&mut manifest, change);
        advance_manifest_generation(&mut manifest)?;
        Ok(PreparedEpochChange {
            change,
            base_generation,
            manifest,
        })
    }

    pub fn apply_prepared_epoch_change(&mut self, prepared: PreparedEpochChange) -> Result<()> {
        self.apply_prepared_manifest(prepared.base_generation, prepared.manifest)
    }
}
