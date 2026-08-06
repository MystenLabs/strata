use std::{fs, io::Read, path::Path};

use sha2::{Digest, Sha256};
use strata_core::{SegmentFileState, SegmentId, SegmentState};
use strata_index::StrataIndex;

use crate::{
    Error, Result, SealedSegmentIntegrityPolicy, StrataStoreConfig, StrataStoreMetrics,
    layout::segment_state_path, unsealed_ingest_segment_count, unsealed_ingest_segment_ids,
};

pub(crate) fn prepare_synced_seal(
    config: &StrataStoreConfig,
    segment_id: SegmentId,
    path: &Path,
    sealed_len: u64,
) -> Result<Option<[u8; 32]>> {
    let file_len = fs::metadata(path)
        .map_err(|source| Error::Io {
            path: path.to_path_buf(),
            source,
        })?
        .len();
    if file_len != sealed_len {
        return Err(Error::SealedSegmentLengthMismatch {
            segment_id,
            path: path.to_path_buf(),
            expected_len: sealed_len,
            actual_len: file_len,
        });
    }
    match config.sealed_segment_integrity_policy {
        SealedSegmentIntegrityPolicy::Checksum => Ok(Some(sha256_file_prefix(path, sealed_len)?)),
        SealedSegmentIntegrityPolicy::MetadataOnly => Ok(None),
    }
}

pub(crate) fn seal_recovered_segments(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    active_segment_id: SegmentId,
    metrics: &StrataStoreMetrics,
) -> Result<()> {
    let committed_before_lsn = index.get_next_lsn()?;
    for segment_id in unsealed_ingest_segment_ids(index)? {
        if segment_id >= active_segment_id {
            continue;
        }
        let Some(mut state) = index.get_segment_state(segment_id)? else {
            continue;
        };
        let path = segment_state_path(config, &state);
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
        state.sealed_sha256 = prepare_synced_seal(config, segment_id, &path, state.write_offset)?;
        state.durable_offset = state.write_offset;
        state.state = SegmentFileState::Sealed;
        state.sealed_before_lsn = Some(
            state
                .sealed_before_lsn
                .unwrap_or_else(|| {
                    state
                        .max_lsn
                        .and_then(|lsn| lsn.checked_add(1))
                        .unwrap_or(1)
                })
                .min(committed_before_lsn),
        );
        state.sealed_len = Some(state.write_offset);
        let mut batch = index.batch();
        index.put_segment_state_batch(&mut batch, &state)?;
        batch
            .write_with_sync(true)
            .map_err(strata_index::Error::from)?;
        metrics.record_segment_sealed();
    }
    metrics.set_unsealed_segments(unsealed_ingest_segment_count(index)?);
    Ok(())
}

pub(crate) fn verify_sealed_segments(
    config: &StrataStoreConfig,
    index: &StrataIndex,
) -> Result<()> {
    for (_, state) in index.iter_segment_states()? {
        if matches!(
            state.state,
            SegmentFileState::Sealed | SegmentFileState::GcRelocating
        ) {
            verify_sealed_segment(config, &state)?;
        }
    }
    Ok(())
}

fn verify_sealed_segment(config: &StrataStoreConfig, state: &SegmentState) -> Result<()> {
    let segment_id = state.segment_id;
    let path = segment_state_path(config, state);
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
