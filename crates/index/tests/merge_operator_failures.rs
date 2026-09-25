//! A merge operator that cannot decode its input must crash the process, not fail quietly.
//!
//! Returning `None` from a full merge tells RocksDB the merge failed, which leaves the key
//! unreadable and surfaces later as corruption. For GC accounting that is the dangerous outcome, so
//! the operators panic instead. The panic is non-unwinding -- the callback is `extern "C"` -- so the
//! process aborts.
//!
//! An abort cannot be observed from inside the process that aborts, so this test re-executes its own
//! binary and inspects how the child died.

use std::process::Command;

use core_types::SegmentGcSummary;
use index::StrataIndex;
use index::port::codec::encode_key;

/// Set in the child, so it corrupts an operand and dies instead of re-spawning.
const CHILD: &str = "STRATA_MERGE_ABORT_CHILD";
const TEST: &str = "a_corrupt_operand_crashes_the_process";
const SEGMENT: u64 = 7;

#[test]
fn a_corrupt_operand_crashes_the_process() {
    if std::env::var(CHILD).is_ok() {
        corrupt_an_operand_and_read_it();
        return;
    }

    let output = Command::new(std::env::current_exe().expect("test binary path"))
        .args([TEST, "--exact", "--nocapture"])
        .env(CHILD, "1")
        .output()
        .expect("run the child");

    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(
        !output.status.success(),
        "the child exited successfully; a corrupt operand must not be tolerated.\nstderr:\n{stderr}"
    );
    assert!(
        !stderr.contains("MERGE RETURNED NORMALLY"),
        "the merge returned instead of crashing.\nstderr:\n{stderr}"
    );
    assert!(
        stderr.contains("strata-segment-gc-summary-merge") && stderr.contains("cannot decode"),
        "the crash did not name the operator and the failure.\nstderr:\n{stderr}"
    );
    // Non-unwinding panics abort, which is what makes this a hard stop.
    assert!(
        stderr.contains("non-unwinding") || stderr.contains("abort"),
        "expected an abort rather than a plain panic.\nstderr:\n{stderr}"
    );
}

fn corrupt_an_operand_and_read_it() {
    let dir = tempfile::tempdir().unwrap();
    let index = StrataIndex::open_path(dir.path(), "merge", "merge").unwrap();

    // A valid summary for the merge to fold onto.
    let mut batch = index.batch();
    index
        .put_segment_gc_summary_batch(&mut batch, SEGMENT, &SegmentGcSummary::default())
        .unwrap();
    batch.write_with_sync(true).unwrap();

    // Stage an operand that cannot possibly decode, going through the port's byte-level batch so
    // the typed layer cannot reject it first.
    let cf = index.cf_names().segment_gc_summaries.clone();
    let key = encode_key(&SEGMENT).unwrap();
    let mut raw = index.db().write_batch();
    raw.merge(&cf, &key, &[0xff]).unwrap();
    raw.write(true).unwrap();

    // Reading drives the full merge, which is where the operand is decoded.
    let _ = index.get_segment_gc_summary(SEGMENT);

    eprintln!("MERGE RETURNED NORMALLY");
}
