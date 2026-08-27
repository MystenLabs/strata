use core_types::{ShardCleanupJob, ShardCleanupState, ShardKey};
use typed_store::{Map, rocks::DBBatch};

use crate::{Error, Result, StrataIndex};

impl StrataIndex {
    pub fn get_shard_cleanup_job(&self, shard: ShardKey) -> Result<Option<ShardCleanupJob>> {
        Ok(self.shard_cleanup_jobs.get(&shard)?)
    }

    pub fn put_shard_cleanup_job_batch(
        &self,
        batch: &mut DBBatch,
        mut job: ShardCleanupJob,
    ) -> Result<()> {
        if job.state == ShardCleanupState::PendingMaterialization {
            job.state = ShardCleanupState::ReadyForGc;
        }
        batch
            .insert_batch(self.shard_cleanup_jobs(), [(&job.shard, &job)])
            .map_err(Error::from)?;
        Ok(())
    }

    pub fn delete_shard_cleanup_job_batch(
        &self,
        batch: &mut DBBatch,
        shard: ShardKey,
    ) -> Result<()> {
        batch.delete_batch(self.shard_cleanup_jobs(), [shard])?;
        Ok(())
    }

    pub fn iter_shard_cleanup_jobs(&self) -> Result<Vec<ShardCleanupJob>> {
        self.shard_cleanup_jobs
            .safe_iter()?
            .map(|result| result.map(|(_, job)| job).map_err(Error::from))
            .collect()
    }
}
