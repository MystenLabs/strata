use strata_lsm::{GarbageLog, GarbageLogPosition, GarbageRecord, ManifestEdit};

use crate::{Error, Result, StrataIndex};

impl StrataIndex {
    /// Atomically publishes the durable output of one LSM compaction.
    ///
    /// New SSTs must already be synced, and the caller must keep the input tables reserved until
    /// this returns. This method syncs all garbage records as one frame before committing the frame's
    /// end position with the manifest edit. Its mutable log borrow serializes append and publish.
    /// After any error, the caller must reopen the log from its stored position before reusing it.
    ///
    /// Segment summaries are updated later by the sweeper, after the corresponding segment-local
    /// garbage logs are durable.
    pub fn publish_lsm_compaction(
        &self,
        lsm_name: &str,
        edit: &ManifestEdit,
        garbage_log_name: &str,
        garbage_log: &mut GarbageLog,
        records: &[GarbageRecord],
    ) -> Result<GarbageLogPosition> {
        let committed = self
            .get_garbage_log_position(garbage_log_name)?
            .unwrap_or_default();
        if !garbage_log.is_at_committed_position(committed) {
            return Err(Error::Lsm(strata_lsm::Error::InvalidGarbageLog(
                "garbage log contains an unpublished frame; reopen it before publishing".to_owned(),
            )));
        }

        let mut batch = self.batch();
        self.merge_lsm_manifest_batch(&mut batch, lsm_name, edit)?;
        let position = if records.is_empty() {
            committed
        } else {
            garbage_log.append(records)?
        };
        self.put_garbage_log_position_batch(&mut batch, garbage_log_name, position)?;
        batch.write_with_sync(true)?;
        Ok(position)
    }
}
