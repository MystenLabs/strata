use core_types::{ShardCleanupJob, ShardCleanupState, ShardKey};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::port::{IndexDb, TypedMap};
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
    db: &Arc<dyn IndexDb>,
    cf_names: &StrataIndexCfNames,
    shard_cleanup_jobs: &TypedMap<ShardKey, ShardCleanupJob>,
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

    if db.cf_exists(&legacy_projection_cf) {
        let legacy = TypedMap::<LegacyProjectionKey, LegacyProjectionValue>::new(
            Arc::clone(db),
            &legacy_projection_cf,
        );
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
        if db.cf_exists(&name) {
            db.drop_cf(&name)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::StrataIndex;
    use crate::port::{RocksBackend, options::default_db_options};

    #[test]
    fn open_migrates_cleanup_jobs_before_dropping_retired_families() {
        let dir = tempdir().unwrap();
        let prefix = "strata";
        let retired_names = RETIRED_CF_BASENAMES.map(|basename| format!("{prefix}/{basename}"));
        let retired_cf_options = retired_names
            .iter()
            .map(|name| (name.clone(), default_db_options()))
            .collect::<Vec<_>>();
        let db: Arc<dyn IndexDb> = Arc::new(
            RocksBackend::open(dir.path(), Some(default_db_options()), &retired_cf_options)
                .unwrap(),
        );
        let legacy = TypedMap::<LegacyProjectionKey, LegacyProjectionValue>::new(
            Arc::clone(&db),
            format!("{prefix}/accounting_index"),
        );
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
                !index.db().cf_exists(&name),
                "retired column family survived migration: {name}"
            );
        }
    }
}
