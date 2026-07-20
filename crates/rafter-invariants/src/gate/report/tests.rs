//! Scenarios for official report publication and readback.

use std::sync::atomic::{AtomicU64, Ordering};

static TEST_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn report_readback_rejects_any_post_write_difference() {
    let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "rafter-invariants-report-readback-{}-{id}.json",
        std::process::id()
    ));
    std::fs::write(&path, b"corrupt").expect("write corrupt report fixture");

    let error = super::verify_written(&path, b"expected")
        .expect_err("a changed official report must fail readback");
    assert!(error
        .to_string()
        .contains("does not match the rendered output"));
    std::fs::remove_file(path).expect("remove report fixture");
}
