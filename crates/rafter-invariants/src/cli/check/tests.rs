//! Scenarios for fail-closed implicit result discovery.

use std::sync::atomic::{AtomicU64, Ordering};

use super::profile_result_files;

static DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn implicit_discovery_includes_missing_required_layers() {
    let id = DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "rafter-invariants-discovery-{}-{id}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).expect("test directory exists");
    for name in [
        "pr-tests.json",
        "pr-simulator.json",
        "nightly-tests.json",
        "pr-unexpected.json",
    ] {
        std::fs::write(root.join(name), b"not parsed during discovery").expect("fixture writes");
    }

    let paths = profile_result_files(
        &root,
        "pr",
        &[
            rafter_invariants::EvidenceLayer::Tests,
            rafter_invariants::EvidenceLayer::Simulator,
            rafter_invariants::EvidenceLayer::Tla,
        ],
    );
    assert_eq!(
        paths,
        vec![
            root.join("pr-simulator.json"),
            root.join("pr-tests.json"),
            root.join("pr-tla.json"),
        ]
    );
    let _ = std::fs::remove_dir_all(root);
}
