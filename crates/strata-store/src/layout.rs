use std::{ffi::OsStr, path::PathBuf};

use strata_core::{PlacementClass, SegmentId, SegmentState, ShardKey};

use crate::StrataStoreConfig;

pub(crate) fn segment_path(config: &StrataStoreConfig, segment_id: SegmentId) -> PathBuf {
    config.ingest_dir().join(segment_file_name(segment_id))
}

pub(crate) fn retention_segment_path(
    config: &StrataStoreConfig,
    shard: ShardKey,
    placement_class: PlacementClass,
    segment_id: SegmentId,
) -> PathBuf {
    let path = shard_retention_dir(config, shard);
    match placement_class {
        PlacementClass::Ingest => segment_path(config, segment_id),
        PlacementClass::ExactEpoch(epoch) => path
            .join(format!("epoch-{epoch:020}"))
            .join(segment_file_name(segment_id)),
        PlacementClass::Spillover => path.join("spillover").join(segment_file_name(segment_id)),
    }
}

pub(crate) fn retention_dir(config: &StrataStoreConfig) -> PathBuf {
    config.namespace_dir().join("retention")
}

pub(crate) fn shard_retention_dir(config: &StrataStoreConfig, shard: ShardKey) -> PathBuf {
    retention_dir(config).join(shard_dir_name(shard))
}

pub(crate) fn segment_state_path(config: &StrataStoreConfig, state: &SegmentState) -> PathBuf {
    let path = PathBuf::from(&state.path);
    if path.is_absolute() {
        path
    } else {
        config.namespace_dir().join(path)
    }
}

pub(crate) fn relative_segment_path(config: &StrataStoreConfig, path: PathBuf) -> String {
    path.as_path()
        .strip_prefix(config.namespace_dir())
        .unwrap_or(path.as_path())
        .to_string_lossy()
        .into_owned()
}

pub(crate) fn segment_file_name(segment_id: SegmentId) -> String {
    strata_segment::segment_file_name(segment_id)
}

pub(crate) fn shard_dir_name(shard: ShardKey) -> String {
    format!("shard-{:010}-gen-{:020}", shard.id, shard.generation)
}

pub(crate) fn parse_segment_file_name(file_name: &OsStr) -> Option<SegmentId> {
    let stem = file_name.to_str()?.strip_suffix(".data")?;
    if stem.len() != 12 || !stem.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    stem.parse().ok()
}
