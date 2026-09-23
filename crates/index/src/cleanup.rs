use crate::port::map::IndexBatch;
use core_types::{ShardCleanupJob, ShardKey};

use crate::{Result, StrataIndex};

impl StrataIndex {
    pub fn get_shard_cleanup_job(&self, shard: ShardKey) -> Result<Option<ShardCleanupJob>> {
        self.shard_cleanup_jobs.get(&shard)
    }

    pub fn put_shard_cleanup_job_batch(
        &self,
        batch: &mut IndexBatch,
        job: ShardCleanupJob,
    ) -> Result<()> {
        batch.insert_batch(self.shard_cleanup_jobs(), [(&job.shard, &job)])?;
        Ok(())
    }

    pub fn delete_shard_cleanup_job_batch(
        &self,
        batch: &mut IndexBatch,
        shard: ShardKey,
    ) -> Result<()> {
        batch.delete_batch(self.shard_cleanup_jobs(), [shard])?;
        Ok(())
    }

    pub fn iter_shard_cleanup_jobs(&self) -> Result<Vec<ShardCleanupJob>> {
        self.shard_cleanup_jobs
            .safe_iter()?
            .map(|result| result.map(|(_, job)| job))
            .collect()
    }
}
