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
