use serde::{Deserialize, Serialize};
use strata_core::{ShardCleanupJob, ShardCleanupState, ShardKey};
use typed_store::{
    Map, TypedStoreError,
    rocks::{DBMap, ReadWriteOptions, RocksDB},
};

use crate::{Result, StrataIndexCfNames};

const RETIRED_CF_BASENAMES: [&str; 6] = [
    "blob_versions",
    "segment_ref_events",
    "segment_gc_overlay",
    "gc_relocations",
    "unaccounted_lsn_ops",
    "accounting_index",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
enum LegacyProjectionKey {
    Manifest,
    DurablePosition,
    ConsumedCursor,
    ShardCleanup(ShardKey),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum LegacyProjectionValue {
    Manifest(Vec<u8>),
    DurablePosition(Vec<u8>),
    ConsumedCursor(Vec<u8>),
    ShardCleanupJob(ShardCleanupJob),
}

pub(crate) fn migrate_and_drop_retired_cfs(
    db: &std::sync::Arc<RocksDB>,
    cf_names: &StrataIndexCfNames,
    shard_cleanup_jobs: &DBMap<ShardKey, ShardCleanupJob>,
) -> Result<()> {
    let prefix = cf_names
        .segment_states
        .strip_suffix("/segment_states")
        .unwrap_or_default();
    let qualify = |basename: &str| {
        if prefix.is_empty() {
            basename.to_owned()
        } else {
            format!("{prefix}/{basename}")
        }
    };
    let legacy_projection_cf = qualify("accounting_index");

    if db.cf_handle(&legacy_projection_cf).is_some() {
        let legacy = DBMap::<LegacyProjectionKey, LegacyProjectionValue>::reopen_with_class(
            db,
            Some(&legacy_projection_cf),
            Some("retired_projection_index"),
            &ReadWriteOptions::default(),
            true,
        )?;
        let mut jobs = Vec::new();
        for row in legacy.safe_iter()? {
            let Ok((_, LegacyProjectionValue::ShardCleanupJob(mut job))) = row else {
                continue;
            };
            if job.state == ShardCleanupState::PendingMaterialization {
                job.state = ShardCleanupState::ReadyForGc;
            }
            jobs.push(job);
        }
        if !jobs.is_empty() {
            let mut batch = shard_cleanup_jobs.batch();
            batch.insert_batch(shard_cleanup_jobs, jobs.iter().map(|job| (&job.shard, job)))?;
            batch.write_with_sync(true)?;
        }
    }

    for basename in RETIRED_CF_BASENAMES {
        let name = qualify(basename);
        if db.cf_handle(&name).is_some() {
            db.drop_cf(&name)
                .map_err(|error| TypedStoreError::RocksDBError(error.into_string()))?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;
    use typed_store::rocks::{DBMap, ReadWriteOptions, open_cf};

    use super::*;
    use crate::{StrataIndex, init_typed_store_metrics, metric_conf_with_suffix};

    #[tokio::test]
    async fn open_migrates_cleanup_jobs_before_dropping_retired_families() {
        init_typed_store_metrics();
        let dir = tempdir().unwrap();
        let prefix = "strata";
        let retired_names = RETIRED_CF_BASENAMES.map(|basename| format!("{prefix}/{basename}"));
        let retired_name_refs = retired_names.iter().map(String::as_str).collect::<Vec<_>>();
        let db = open_cf(
            dir.path(),
            None,
            metric_conf_with_suffix(
                "strata_index_migration_test",
                dir.path().display().to_string(),
            ),
            &retired_name_refs,
        )
        .unwrap();
        let legacy = DBMap::<LegacyProjectionKey, LegacyProjectionValue>::reopen_with_class(
            &db,
            Some(&format!("{prefix}/accounting_index")),
            Some("retired_projection_index"),
            &ReadWriteOptions::default(),
            true,
        )
        .unwrap();
        let shard = ShardKey {
            id: 17,
            generation: 4,
        };
        legacy
            .insert(
                &LegacyProjectionKey::ShardCleanup(shard),
                &LegacyProjectionValue::ShardCleanupJob(ShardCleanupJob {
                    shard,
                    drop_lsn: 42,
                    state: ShardCleanupState::PendingMaterialization,
                }),
            )
            .unwrap();
        drop(legacy);
        drop(db);

        let index =
            StrataIndex::open_path(dir.path(), prefix, dir.path().display().to_string()).unwrap();
        assert_eq!(
            index.get_shard_cleanup_job(shard).unwrap(),
            Some(ShardCleanupJob {
                shard,
                drop_lsn: 42,
                state: ShardCleanupState::ReadyForGc,
            })
        );
        for name in retired_names {
            assert!(
                index.db().cf_handle(&name).is_none(),
                "retired column family survived migration: {name}"
            );
        }
    }
}
