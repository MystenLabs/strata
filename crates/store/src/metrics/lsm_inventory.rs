//! Scrape cached SST inventory without walking keys, opening files, or retaining the store.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use lsm::Lsm;
use prometheus::{
    IntGaugeVec, Registry,
    core::{Collector, Desc},
    proto::MetricFamily,
};

use super::{opts, to_i64};

#[derive(Clone, Debug)]
pub(super) struct LsmInventory {
    sources: Arc<Mutex<[Weak<Lsm>; 2]>>,
    gauges: [IntGaugeVec; 3],
}

impl LsmInventory {
    pub(super) fn register(
        registry: &Registry,
        labels: &HashMap<String, String>,
    ) -> Result<Self, prometheus::Error> {
        let gauge = |name, help| IntGaugeVec::new(opts(labels, name, help), &["lsm", "kind"]);
        let collector = Self {
            sources: Arc::new(Mutex::new([Weak::new(), Weak::new()])),
            gauges: [
                gauge(
                    "lsm_sst_files",
                    "SST files in the current read view, including pending flushed patches; excludes obsolete and staged files.",
                )?,
                gauge(
                    "lsm_sst_bytes",
                    "SST file bytes in the current read view; excludes memtables, payload segments, obsolete and staged files.",
                )?,
                gauge(
                    "lsm_sst_records",
                    "Physical SST records in the current read view, including duplicate keys and obsolete operands; not unique live keys.",
                )?,
            ],
        };
        registry.register(Box::new(collector.clone()))?;
        Ok(collector)
    }

    pub(super) fn bind(&self, main: &Arc<Lsm>, relocation: &Arc<Lsm>) {
        *self.sources.lock().unwrap_or_else(|e| e.into_inner()) =
            [Arc::downgrade(main), Arc::downgrade(relocation)];
    }
}

impl Collector for LsmInventory {
    fn desc(&self) -> Vec<&Desc> {
        self.gauges.iter().flat_map(Collector::desc).collect()
    }

    fn collect(&self) -> Vec<MetricFamily> {
        // Serialize concurrent scrapes so one cannot change the gauges while another collects.
        // Writers never take this lock. Each LSM copies six cached integers under its view lock.
        let sources = self.sources.lock().unwrap_or_else(|e| e.into_inner());
        for (name, source) in ["main", "relocation"].into_iter().zip(sources.iter()) {
            let Some(lsm) = source.upgrade() else {
                // Do not report stale inventory or zero bytes for a closed/unbound store.
                for kind in ["base", "patch"] {
                    for gauge in &self.gauges {
                        let _ = gauge.remove_label_values(&[name, kind]);
                    }
                }
                continue;
            };
            let stats = lsm.table_stats();
            for (kind, stats) in [("base", stats.base), ("patch", stats.patch)] {
                for (gauge, value) in
                    self.gauges
                        .iter()
                        .zip([stats.files, stats.bytes, stats.records])
                {
                    gauge.with_label_values(&[name, kind]).set(to_i64(value));
                }
            }
        }
        self.gauges.iter().flat_map(Collector::collect).collect()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    use lsm::{LsmOptions, Manifest, TableWriter};
    use tempfile::tempdir;

    use super::*;

    fn samples(registry: &Registry, name: &str) -> HashMap<(String, String), f64> {
        registry
            .gather()
            .into_iter()
            .find(|family| family.name() == name)
            .into_iter()
            .flat_map(|family| family.get_metric().to_vec())
            .map(|metric| {
                let label = |name| {
                    metric
                        .get_label()
                        .iter()
                        .find(|l| l.name() == name)
                        .unwrap()
                        .value()
                        .to_owned()
                };
                assert_eq!(label("store"), "inventory-test");
                ((label("lsm"), label("kind")), metric.get_gauge().value())
            })
            .collect()
    }

    #[test]
    fn inventory_tracks_installed_views_recovery_and_store_lifetime() {
        let directory = tempdir().unwrap();
        let empty = Manifest::empty("base-v1", "patch-v1", NonZeroU32::new(1).unwrap());
        let main_root = directory.path().join("main");
        let main = Arc::new(
            Lsm::from_parts(
                &main_root,
                Arc::new(empty.clone()),
                vec![],
                None,
                LsmOptions::default(),
            )
            .unwrap(),
        );
        let relocation = Arc::new(
            Lsm::from_parts(
                directory.path().join("relocation"),
                Arc::new(empty.clone()),
                vec![],
                None,
                LsmOptions::default(),
            )
            .unwrap(),
        );
        let registry = Registry::new();
        let labels = HashMap::from([("store".to_owned(), "inventory-test".to_owned())]);
        let collector = LsmInventory::register(&registry, &labels).unwrap();
        assert!(samples(&registry, "strata_store_lsm_sst_files").is_empty());
        collector.bind(&main, &relocation);
        assert_eq!(samples(&registry, "strata_store_lsm_sst_files").len(), 4);

        let mut base = TableWriter::create_base(&main_root, "base-1.sst", 1, 0, "base-v1").unwrap();
        base.add(b"a", b"one").unwrap();
        base.add(b"b", b"two").unwrap();
        let base = base.finish().unwrap();
        let mut patch =
            TableWriter::create_patch(&main_root, "patch-2.sst", 2, 0, "patch-v1").unwrap();
        patch.add_patch(b"a", 10, b"update").unwrap();
        let patch = patch.finish().unwrap();
        let mut manifest = empty.clone();
        manifest.next_table_id = 3;
        manifest
            .partitions
            .get_mut(&0)
            .unwrap()
            .base
            .push(base.clone());
        manifest
            .partitions
            .get_mut(&0)
            .unwrap()
            .patches
            .push(patch.clone());
        main.install_manifest(manifest.clone()).unwrap();
        let key = |kind: &str| ("main".to_owned(), kind.to_owned());
        assert_eq!(
            samples(&registry, "strata_store_lsm_sst_files")[&key("patch")],
            1.0
        );
        assert_eq!(
            samples(&registry, "strata_store_lsm_sst_bytes")[&key("base")],
            base.file_len as f64
        );
        assert_eq!(
            samples(&registry, "strata_store_lsm_sst_bytes")[&key("patch")],
            patch.file_len as f64
        );
        assert_eq!(
            samples(&registry, "strata_store_lsm_sst_records")[&key("base")],
            2.0
        );
        assert_eq!(
            samples(&registry, "strata_store_lsm_sst_records")[&key("patch")],
            1.0
        );
        assert_eq!(
            samples(&registry, "strata_store_lsm_sst_bytes")
                [&("relocation".to_owned(), "base".to_owned())],
            0.0
        );

        let cache_before = main.block_cache_stats();
        let readers_before = main.table_store().cached_reader_count();
        for _ in 0..100 {
            registry.gather();
        }
        assert_eq!(main.block_cache_stats(), cache_before);
        assert_eq!(main.table_store().cached_reader_count(), readers_before);

        main.install_manifest(empty.clone()).unwrap();
        assert!(main_root.join(&patch.relative_path).exists());
        assert!(
            samples(&registry, "strata_store_lsm_sst_files")
                .values()
                .all(|&v| v == 0.0)
        );
        drop(main);
        // The registry must not keep the LSM alive or report stale inventory after shutdown.
        assert_eq!(samples(&registry, "strata_store_lsm_sst_files").len(), 2);

        let reopened = Arc::new(
            Lsm::from_parts(
                &main_root,
                Arc::new(manifest),
                vec![],
                None,
                LsmOptions::default(),
            )
            .unwrap(),
        );
        collector.bind(&reopened, &relocation);
        assert_eq!(
            samples(&registry, "strata_store_lsm_sst_records")[&key("base")],
            2.0
        );
        assert_eq!(
            samples(&registry, "strata_store_lsm_sst_files")[&key("patch")],
            1.0
        );
        drop(reopened);
        drop(relocation);
        assert!(samples(&registry, "strata_store_lsm_sst_files").is_empty());
    }
}
