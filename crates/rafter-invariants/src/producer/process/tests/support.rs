//! Collision-resistant paths shared by producer process scenarios.

use std::path::PathBuf;

static TEST_SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

pub(super) fn unique_test_path(label: &str) -> PathBuf {
    let sequence = next_sequence();
    PathBuf::from("target/rafter-invariants/process-tests").join(format!(
        "rafter-invariants-{label}-{}-{sequence}",
        std::process::id()
    ))
}

pub(super) fn next_sequence() -> u64 {
    TEST_SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
