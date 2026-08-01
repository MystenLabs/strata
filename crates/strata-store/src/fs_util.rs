//! Filesystem helpers shared by the write, GC, and open paths: durable directory
//! syncs, GC segment file unlinking, and retention-directory pruning.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use strata_core::{SegmentId, SegmentState};

use crate::{
    Error, Result, StrataStoreConfig,
    layout::{retention_dir, segment_path, segment_state_path},
};

fn gc_segment_file_path(config: &StrataStoreConfig, state: &SegmentState) -> std::path::PathBuf {
    if state.path.is_empty() {
        segment_path(config, state.segment_id)
    } else {
        segment_state_path(config, state)
    }
}

pub(crate) fn segment_garbage_log_path(mut segment_path: PathBuf) -> PathBuf {
    segment_path.set_extension("glog");
    segment_path
}

pub(crate) fn unlink_gc_segment_file(
    config: &StrataStoreConfig,
    state: &SegmentState,
) -> Result<()> {
    unlink_gc_segment_files(config, std::slice::from_ref(state)).map(|_| ())
}

pub(crate) fn unlink_gc_segment_files(
    config: &StrataStoreConfig,
    states: &[SegmentState],
) -> Result<Vec<(SegmentId, u64)>> {
    let mut parents = BTreeSet::new();
    let mut unlinked_segments = Vec::new();
    for state in states {
        let path = gc_segment_file_path(config, state);
        let file_len = match fs::metadata(&path) {
            Ok(metadata) => Some(metadata.len()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(source) => {
                return Err(Error::Io {
                    path: path.clone(),
                    source,
                });
            }
        };
        match fs::remove_file(&path) {
            Ok(()) => {
                unlinked_segments.push((
                    state.segment_id,
                    file_len.expect("metadata existed before removal"),
                ));
                if let Some(parent) = path.parent() {
                    parents.insert(parent.to_path_buf());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => return Err(Error::Io { path, source }),
        }
        let garbage_path = segment_garbage_log_path(path.clone());
        match fs::remove_file(&garbage_path) {
            Ok(()) => {
                if let Some(parent) = garbage_path.parent() {
                    parents.insert(parent.to_path_buf());
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(Error::Io {
                    path: garbage_path,
                    source,
                });
            }
        }
    }

    for parent in parents {
        sync_dir(&parent)?;
        prune_empty_retention_dirs(config, parent)?;
    }
    Ok(unlinked_segments)
}

pub(crate) fn prune_empty_retention_dirs(
    config: &StrataStoreConfig,
    mut directory: std::path::PathBuf,
) -> Result<()> {
    let root = retention_dir(config);
    while directory != root && directory.starts_with(&root) {
        match fs::remove_dir(&directory) {
            Ok(()) => {
                sync_parent_dir(&directory)?;
                let Some(parent) = directory.parent() else {
                    break;
                };
                directory = parent.to_path_buf();
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let Some(parent) = directory.parent() else {
                    break;
                };
                directory = parent.to_path_buf();
            }
            Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                break;
            }
            Err(source) => {
                return Err(Error::Io {
                    path: directory,
                    source,
                });
            }
        }
    }
    Ok(())
}

pub(crate) fn sync_parent_dir(path: &Path) -> Result<()> {
    let Some(parent) = path.parent() else {
        return Ok(());
    };
    sync_dir(parent)
}

fn sync_dir(path: &Path) -> Result<()> {
    let dir = fs::File::open(path).map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })?;
    dir.sync_all().map_err(|source| Error::Io {
        path: path.to_path_buf(),
        source,
    })
}
