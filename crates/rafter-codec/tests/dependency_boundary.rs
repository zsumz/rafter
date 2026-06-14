use std::{fs, path::Path};

#[test]
fn rafter_codec_dependency_boundary_stays_free_of_server_and_runtime_dependencies() {
    let manifest = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
        .expect("read rafter-codec Cargo.toml");

    for forbidden in [
        "rafter-storage",
        "tokio",
        "tracing",
        "log",
        "serde",
        "serde_json",
    ] {
        assert!(
            !manifest.contains(forbidden),
            "rafter-codec must not depend on {forbidden}"
        );
    }
}
