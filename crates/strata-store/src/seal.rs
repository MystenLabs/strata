use std::{fs, io::Read, path::Path, sync::mpsc};

use sha2::{Digest, Sha256};
use strata_core::{BlobKey, BlobVersionKey, SegmentFileState, SegmentId, SegmentState, StrataLsn};
use strata_index::StrataIndex;
use typed_store::Map;

use crate::{
    Error, Result, SealedSegmentIntegrityPolicy, StrataStoreConfig, StrataStoreMetrics,
    active_segment_state_from_path, layout::segment_path, unsealed_ingest_segment_count,
    unsealed_ingest_segment_ids,
};

#[derive(Debug)]
pub(crate) enum SealCommand {
    Seal(SegmentSealTask),
    Shutdown,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct SegmentSealTask {
    pub(crate) segment_id: SegmentId,
    pub(crate) sealed_len: u64,
}

#[derive(Debug)]
pub(crate) struct SealWorker {
    pub(crate) config: StrataStoreConfig,
    pub(crate) index: StrataIndex,
    pub(crate) seal_rx: mpsc::Receiver<SealCommand>,
    pub(crate) metrics: StrataStoreMetrics,
}

impl SealWorker {
    pub(crate) fn run(self) {
        while let Ok(command) = self.seal_rx.recv() {
            match command {
                SealCommand::Seal(task) => {
                    if self.seal_segment(task).is_err() {
                        self.metrics.record_seal_error();
                        let _ = self.mark_seal_failed(task.segment_id);
                    }
                }
                SealCommand::Shutdown => break,
            }
        }
    }

    pub(crate) fn seal_segment(&self, task: SegmentSealTask) -> Result<()> {
        if let Some(existing) = self.index.get_segment_state(task.segment_id)?
            && existing.state == SegmentFileState::Sealed
        {
            return Ok(());
        }

        let path = segment_path(&self.config, task.segment_id);
        let file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|source| Error::Io {
                path: path.clone(),
                source,
            })?;
        file.sync_data().map_err(|source| Error::Io {
            path: path.clone(),
            source,
        })?;
        let sealed_sha256 = sha256_file_prefix(&path, task.sealed_len)?;

        let mut state = active_segment_state_from_path(
            &self.config,
            task.segment_id,
            task.sealed_len,
            task.sealed_len,
        );
        if let Some(existing) = self.index.get_segment_state(task.segment_id)? {
            state.volume_id = existing.volume_id;
            state.placement_class = existing.placement_class;
            state.min_lsn = existing.min_lsn;
            state.max_lsn = existing.max_lsn;
        }
        state.state = SegmentFileState::Sealed;
        state.sealed_len = Some(task.sealed_len);
        state.sealed_sha256 = Some(sealed_sha256);

        let durable_lsn = {
            let mut batch = self.index.batch();
            batch
                .insert_batch(self.index.segment_states(), [(&state.segment_id, &state)])
                .map_err(strata_index::Error::from)?;
            let durable_lsn = durable_lsn_with_advanced_frontier(&self.index, Some(&state))?;
            self.index.put_durable_lsn_batch(&mut batch, durable_lsn)?;
            batch.write().map_err(strata_index::Error::from)?;
            durable_lsn
        };
        self.index.flush_wal(true)?;
        self.metrics.record_segment_sealed();
        self.metrics.set_durable_lsn(durable_lsn);
        self.metrics
            .set_unsealed_segments(unsealed_ingest_segment_count(&self.index)?);
        Ok(())
    }

    pub(crate) fn mark_seal_failed(&self, segment_id: SegmentId) -> Result<()> {
        let Some(mut state) = self.index.get_segment_state(segment_id)? else {
            return Ok(());
        };
        if state.state == SegmentFileState::Sealed {
            return Ok(());
        }
        state.state = SegmentFileState::SealFailed;
        let mut batch = self.index.batch();
        batch
            .insert_batch(self.index.segment_states(), [(&state.segment_id, &state)])
            .map_err(strata_index::Error::from)?;
        batch.write().map_err(strata_index::Error::from)?;
        self.index.flush_wal(true)?;
        Ok(())
    }
}

pub(crate) fn enqueue_unsealed_segments_for_sealing(
    index: &StrataIndex,
    active_segment_id: SegmentId,
    seal_tx: &mpsc::Sender<SealCommand>,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    for segment_id in unsealed_ingest_segment_ids(index)? {
        if segment_id < active_segment_id
            && let Some(state) = index.get_segment_state(segment_id)?
        {
            seal_tx
                .send(SealCommand::Seal(SegmentSealTask {
                    segment_id,
                    sealed_len: state.write_offset,
                }))
                .map_err(|_| Error::SealQueueClosed)?;
            metrics.record_seal_enqueued();
        }
    }
    Ok(())
}

pub(crate) fn verify_sealed_segments(
    config: &StrataStoreConfig,
    index: &StrataIndex,
) -> Result<()> {
    for (_, state) in index.iter_segment_states()? {
        if state.state == SegmentFileState::Sealed {
            verify_sealed_segment(config, &state)?;
        }
    }
    Ok(())
}

fn verify_sealed_segment(config: &StrataStoreConfig, state: &SegmentState) -> Result<()> {
    let segment_id = state.segment_id;
    let path = segment_path(config, segment_id);
    let expected_len = state
        .sealed_len
        .ok_or(Error::SealedSegmentMissingLength { segment_id })?;
    let metadata = match fs::metadata(&path) {
        Ok(metadata) => metadata,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(Error::SealedSegmentMissing { segment_id, path });
        }
        Err(source) => {
            return Err(Error::Io {
                path: path.clone(),
                source,
            });
        }
    };
    let actual_len = metadata.len();
    if actual_len != expected_len {
        return Err(Error::SealedSegmentLengthMismatch {
            segment_id,
            path,
            expected_len,
            actual_len,
        });
    }

    if config.sealed_segment_integrity_policy == SealedSegmentIntegrityPolicy::Checksum {
        let expected = state
            .sealed_sha256
            .ok_or(Error::SealedSegmentMissingChecksum { segment_id })?;
        let actual = sha256_file_prefix(&path, expected_len)?;
        if actual != expected {
            return Err(Error::SealedSegmentChecksumMismatch {
                segment_id,
                path,
                expected,
                actual,
            });
        }
    }

    Ok(())
}

pub(crate) fn sha256_file_prefix(path: &Path, len: u64) -> Result<[u8; 32]> {
    let mut file = fs::File::open(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    let mut remaining = len;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];

    while remaining > 0 {
        let to_read = remaining.min(buffer.len() as u64) as usize;
        let read = file
            .read(&mut buffer[..to_read])
            .map_err(|source| Error::Io {
                path: path.to_path_buf(),
                source,
            })?;
        if read == 0 {
            return Err(Error::Io {
                path: path.to_path_buf(),
                source: std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "segment ended before sealed length",
                ),
            });
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }

    Ok(hasher.finalize().into())
}

pub(crate) fn active_segment_durable_offset(
    index: &StrataIndex,
    segment_id: SegmentId,
) -> Result<u64> {
    Ok(index
        .get_segment_state(segment_id)?
        .map_or(0, |state| state.durable_offset))
}

pub(crate) fn durable_lsn_with_advanced_frontier(
    index: &StrataIndex,
    override_state: Option<&SegmentState>,
) -> Result<StrataLsn> {
    compute_durable_lsn(index, index.get_durable_lsn()?, override_state)
}

fn compute_durable_lsn(
    index: &StrataIndex,
    current_durable_lsn: StrataLsn,
    override_state: Option<&SegmentState>,
) -> Result<StrataLsn> {
    let states = segment_states_with_override(index, override_state)?;
    let mut durable_lsn = current_durable_lsn;

    loop {
        let Some(next_lsn) = durable_lsn.checked_add(1) else {
            break;
        };
        if let Some(key) = index
            .unaccounted_lsn_ops()
            .get(&next_lsn)
            .map_err(strata_index::Error::from)?
        {
            if !unaccounted_lsn_is_durable(index, next_lsn, &key, &states)? {
                break;
            }
        } else if index.get_epoch_change(next_lsn)?.is_none() {
            break;
        }
        durable_lsn = next_lsn;
    }

    Ok(durable_lsn)
}

fn segment_states_with_override(
    index: &StrataIndex,
    override_state: Option<&SegmentState>,
) -> Result<Vec<(SegmentId, SegmentState)>> {
    let mut states = index.iter_segment_states()?;
    if let Some(override_state) = override_state {
        let mut replaced = false;
        for (_, state) in &mut states {
            if state.segment_id == override_state.segment_id {
                *state = override_state.clone();
                replaced = true;
                break;
            }
        }
        if !replaced {
            states.push((override_state.segment_id, override_state.clone()));
        }
    }
    Ok(states)
}

fn unaccounted_lsn_is_durable(
    index: &StrataIndex,
    lsn: StrataLsn,
    key: &BlobKey,
    states: &[(SegmentId, SegmentState)],
) -> Result<bool> {
    let Some(entry) = index.get_blob_version(&BlobVersionKey {
        key: key.clone(),
        lsn,
    })?
    else {
        return Ok(false);
    };
    let Some(record_ref) = entry.record_ref else {
        return Ok(true);
    };
    let Some(record_end_offset) = record_ref.end_offset() else {
        return Err(strata_segment::Error::RangeOverflow.into());
    };
    Ok(states
        .iter()
        .find(|(candidate, _)| *candidate == record_ref.segment_id)
        .is_some_and(|(_, state)| {
            !matches!(
                state.state,
                SegmentFileState::SealFailed
                    | SegmentFileState::Deleting
                    | SegmentFileState::Deleted
            ) && state.durable_offset >= record_end_offset
        }))
}
