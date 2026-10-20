//! `cargo bench -p lsm --bench inventory`
//!
//! Measures cached inventory reads as the installed SST count grows. File creation and snapshot
//! construction are outside the timed loop. This is a metadata-access microbenchmark, not a
//! concurrent workload or end-to-end ingestion benchmark.

use std::{hint::black_box, num::NonZeroU32, sync::Arc, time::Instant};

use lsm::{Lsm, LsmOptions, Manifest, TableWriter};

fn main() {
    const ITERATIONS: u32 = 1_000_000;
    for files in [0, 100, 1_000] {
        let dir = tempfile::tempdir().unwrap();
        let mut manifest = Manifest::empty("base-v1", "patch-v1", NonZeroU32::new(1).unwrap());
        for id in 1..=files {
            let mut writer = TableWriter::create_patch(
                dir.path(),
                format!("patch-{id:020}.sst"),
                id,
                0,
                "patch-v1",
            )
            .unwrap();
            writer.add_patch(b"key", id, b"value").unwrap();
            manifest
                .partitions
                .get_mut(&0)
                .unwrap()
                .patches
                .push(writer.finish().unwrap());
        }
        manifest.next_table_id = files + 1;
        let lsm = Lsm::from_parts(
            dir.path(),
            Arc::new(manifest),
            vec![],
            None,
            LsmOptions::default(),
        )
        .unwrap();
        assert_eq!(lsm.table_stats().patch.files, files);
        for _ in 0..1_000 {
            black_box(lsm.table_stats());
        }
        let start = Instant::now();
        for _ in 0..ITERATIONS {
            black_box(black_box(&lsm).table_stats());
        }
        println!(
            "{files:4} SSTs: {:.1} ns/cached inventory read ({ITERATIONS} iterations)",
            start.elapsed().as_nanos() as f64 / f64::from(ITERATIONS)
        );
    }
}
