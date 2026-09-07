//! The runtime's manifest stays free of edge and framework dependencies.
//!
//! A durable core that pulled in an async runtime, a logging facade, or a
//! serialization framework would force those choices on every embedding. This
//! reads the manifest text, so it catches an edge declared but not yet used.

use std::{fs, path::Path};

#[test]
fn rafter_runtime_dependency_boundary_stays_free_of_server_and_runtime_edge_dependencies() {
    let manifest_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
    let manifest =
        fs::read_to_string(&manifest_path).expect("raft runtime manifest should be readable");

    for forbidden in ["tokio", "tracing", "log", "serde", "serde_json"] {
        assert!(
            !manifest.contains(forbidden),
            "rafter-runtime must not depend on {forbidden}"
        );
    }
}
