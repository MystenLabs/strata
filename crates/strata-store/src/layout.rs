use std::{ffi::OsStr, path::PathBuf};

use strata_core::SegmentId;

use crate::StrataStoreConfig;

pub(crate) fn segment_path(config: &StrataStoreConfig, segment_id: SegmentId) -> PathBuf {
    config.ingest_dir().join(segment_file_name(segment_id))
}

pub(crate) fn relative_segment_path(config: &StrataStoreConfig, path: PathBuf) -> String {
    path.as_path()
        .strip_prefix(config.namespace_dir())
        .unwrap_or(path.as_path())
        .to_string_lossy()
        .into_owned()
}

pub(crate) fn segment_file_name(segment_id: SegmentId) -> String {
    format!("{segment_id:012}.data")
}

pub(crate) fn parse_segment_file_name(file_name: &OsStr) -> Option<SegmentId> {
    let stem = file_name.to_str()?.strip_suffix(".data")?;
    if stem.len() != 12 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}
