use lsm::{GarbageLog, GarbageLogPosition, GarbageRecord, ManifestEdit};

use crate::{Error, Result, StrataIndex};

impl StrataIndex {
    /// Atomically publishes the durable output of one LSM compaction.
    ///
    /// New SSTs must already be synced, and the caller must keep the input tables reserved until
    /// this returns. This method syncs all garbage records as one frame before committing the frame's
    /// end position with the manifest edit. The caller must serialize garbage-log append and
    /// publication, including across separately opened handles to the same log.
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
        self.publish_lsm_compaction_streaming(
            lsm_name,
            edit,
            garbage_log_name,
            garbage_log,
            |log| {
                if !records.is_empty() {
                    log.append_unsynced(records)?;
                }
                Ok(())
            },
        )
    }

    /// Publishes a compaction whose garbage is supplied in bounded frames.
    ///
    /// `append` must append only this compaction's records. All appended frames are synced before
    /// their final position and the manifest edit commit together. Until then, none is visible to
    /// the sweeper. The caller holds the same garbage-publication lock as the slice-based method.
    pub fn publish_lsm_compaction_streaming(
        &self,
        lsm_name: &str,
        edit: &ManifestEdit,
        garbage_log_name: &str,
        garbage_log: &mut GarbageLog,
        append: impl FnOnce(&mut GarbageLog) -> lsm::Result<()>,
    ) -> Result<GarbageLogPosition> {
        let committed = self
            .get_garbage_log_position(garbage_log_name)?
            .unwrap_or_default();
        if !garbage_log.is_at_committed_position(committed) {
            return Err(Error::Lsm(lsm::Error::InvalidGarbageLog(
                "garbage log contains an unpublished frame; reopen it before publishing".to_owned(),
            )));
        }

        // A large garbage frame can take time to write and sync. Let flush/frontier publishers
        // keep using the manifest while that happens; this frame remains unpublished until the
        // batch below commits its position together with the edit.
        append(garbage_log)?;
        garbage_log.sync()?;
        let position = if garbage_log.is_at_committed_position(committed) {
            committed
        } else {
            garbage_log.position()
        };

        let guard = self.lock_lsm_manifests();
        let mut batch = self.batch();
        // Merge into the latest manifest, preserving any publications during the log append.
        self.merge_lsm_manifest_batch(&mut batch, lsm_name, edit, &guard)?;
        self.put_garbage_log_position_batch(&mut batch, garbage_log_name, position)?;
        batch.write_with_sync(true)?;
        drop(guard);
        Ok(position)
    }
}
