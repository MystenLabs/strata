use super::*;

impl AccountingIndex {
    pub(super) fn state_run_records(&self, meta: &RunMeta) -> Result<RunRecordReader<StateRecord>> {
        let reader = self.open_run_record_reader(meta)?;
        if reader.header.kind != RunKind::Base {
            return Err(Error::UnexpectedRunKind {
                run_id: meta.id,
                actual: reader.header.kind,
                expected: RunKind::Base,
            });
        }
        Ok(reader)
    }

    pub(super) fn patch_run_records(&self, meta: &RunMeta) -> Result<RunRecordReader<PatchRecord>> {
        let reader = self.open_run_record_reader(meta)?;
        if reader.header.kind != RunKind::Patch {
            return Err(Error::UnexpectedRunKind {
                run_id: meta.id,
                actual: reader.header.kind,
                expected: RunKind::Patch,
            });
        }
        Ok(reader)
    }

    pub(super) fn read_delta_run(&self, meta: &RunMeta) -> Result<Vec<BlobUpdate>> {
        self.delta_run_records(meta)?.collect()
    }

    pub(super) fn delta_run_records(&self, meta: &RunMeta) -> Result<RunRecordReader<BlobUpdate>> {
        let reader = self.open_run_record_reader(meta)?;
        if reader.header.kind != RunKind::Delta {
            return Err(Error::UnexpectedRunKind {
                run_id: meta.id,
                actual: reader.header.kind,
                expected: RunKind::Delta,
            });
        }
        Ok(reader)
    }

    pub(super) fn open_run_record_reader<T>(&self, meta: &RunMeta) -> Result<RunRecordReader<T>>
    where
        T: DeserializeOwned,
    {
        let path = self.config.root_dir.join(&meta.path);
        let reader = open_run_record_reader(&path)?;
        if reader.header.kind != meta.kind {
            return Err(Error::UnexpectedRunKind {
                run_id: meta.id,
                actual: reader.header.kind,
                expected: meta.kind,
            });
        }
        if reader.header.partition != meta.partition {
            return Err(Error::UnexpectedRunPartition {
                run_id: meta.id,
                actual: reader.header.partition,
                expected: meta.partition,
            });
        }
        Ok(reader)
    }

    pub(super) fn write_run(&self, run_id: RunId, run: &RunFile) -> Result<RunMeta> {
        match &run.records {
            RunRecords::State(records) => {
                self.write_framed_run(run_id, run.kind, run.partition, records.iter().map(Ok))
            }
            RunRecords::Patch(records) => {
                self.write_framed_run(run_id, run.kind, run.partition, records.iter().map(Ok))
            }
            RunRecords::Delta(records) => {
                self.write_framed_run(run_id, run.kind, run.partition, records.iter().map(Ok))
            }
        }
    }

    pub(super) fn write_delta_runs(
        &self,
        manifest: &mut Manifest,
        updates: Vec<BlobUpdate>,
    ) -> Result<Vec<RunMeta>> {
        let mut by_partition = BTreeMap::<PartitionId, Vec<BlobUpdate>>::new();
        for update in updates {
            let partition = self.partition_for_key(update.key());
            by_partition.entry(partition).or_default().push(update);
        }

        let mut metas = Vec::new();
        for (partition, mut updates) in by_partition {
            sort_updates(&mut updates);
            let run_id = allocate_run_id(manifest);
            let run = RunFile {
                format_version: FORMAT_VERSION,
                kind: RunKind::Delta,
                partition,
                records: RunRecords::Delta(updates),
            };
            let meta = self.write_run(run_id, &run)?;
            partition_mut(manifest, partition)?
                .deltas
                .push(meta.clone());
            metas.push(meta);
        }
        Ok(metas)
    }

    pub(super) fn write_framed_run<I, T>(
        &self,
        run_id: RunId,
        kind: RunKind,
        partition: PartitionId,
        records: I,
    ) -> Result<RunMeta>
    where
        I: IntoIterator<Item = Result<T>>,
        T: Serialize + RecordLsn,
    {
        let partition_dir = partition_dir(&self.config.root_dir, partition);
        fs::create_dir_all(&partition_dir).map_err(|source| Error::Io {
            path: partition_dir.clone(),
            source,
        })?;

        let file_name = run_file_name(kind, run_id);
        let relative_path = format!("partition-{partition:05}/{file_name}");
        let path = self.config.root_dir.join(&relative_path);
        let (max_lsn, file_len) = write_run_file_atomic(&path, kind, partition, records)?;

        Ok(RunMeta {
            id: run_id,
            kind,
            partition,
            path: relative_path,
            max_lsn,
            file_len,
        })
    }

    pub(super) fn remove_runs(&self, runs: &[RunMeta]) {
        for run in runs {
            self.remove_run(run);
        }
    }

    fn remove_run(&self, run: &RunMeta) {
        let _ = fs::remove_file(self.config.root_dir.join(&run.path));
    }
}
