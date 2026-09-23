use std::fs;

use core_types::{ShardInfo, ShardKey, ShardState};
use index::StrataIndex;

use crate::{Error, Result, StrataStoreConfig, layout::shard_retention_dir, sync_parent_dir};

/// Returns whether the physical shard generation is fenced by the durable shard registry.
pub(crate) fn shard_generation_is_obsolete(index: &StrataIndex, shard: ShardKey) -> Result<bool> {
    Ok(generation_is_obsolete(
        shard,
        index.get_shard_info(shard.id)?,
    ))
}

/// Removes one generation's shard-owned metadata before unlinking its retention directory.
pub(crate) fn remove_shard_retention_generation(
    config: &StrataStoreConfig,
    index: &StrataIndex,
    shard: ShardKey,
) -> Result<()> {
    let segment_ids = index
        .iter_segment_states_for_shard(shard)?
        .into_iter()
        .map(|(segment_id, _)| segment_id)
        .collect::<Vec<_>>();
    if !segment_ids.is_empty() {
        let mut batch = index.batch();
        index.remove_shard_keyed_metadata_batch(&mut batch, shard)?;
        batch.write_with_sync(true)?;
    }

    let shard_dir = shard_retention_dir(config, shard);
    match fs::remove_dir_all(&shard_dir) {
        Ok(()) => sync_parent_dir(&shard_dir)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(source) => {
            return Err(Error::Io {
                path: shard_dir,
                source,
            });
        }
    }
    Ok(())
}

fn generation_is_obsolete(shard: ShardKey, info: Option<ShardInfo>) -> bool {
    match info {
        Some(info) if shard.generation < info.current_generation => true,
        Some(info)
            if shard.generation == info.current_generation && info.state == ShardState::Dropped =>
        {
            true
        }
        _ => false,
    }
}
