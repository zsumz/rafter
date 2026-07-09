use std::{fs, path::Path};

#[test]
fn rafter_app_dependency_boundary_uses_runtime_api_not_concrete_runtime() {
    let manifest = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
        .expect("read rafter-app Cargo.toml");
    let dependencies = manifest_section(&manifest, "dependencies");
    let dev_dependencies = manifest_section(&manifest, "dev-dependencies");

    assert_normal_dependency(&dependencies, "rafter");
    assert_normal_dependency(&dependencies, "rafter-runtime-api");
    for forbidden in [
        "rafter-runtime",
        "rafter-storage",
        "rafter-service",
        "rafter-multiraft",
        "rafter-transport-tcp-insecure",
    ] {
        assert_no_dependency(&dependencies, forbidden, "rafter-app normal dependencies");
    }

    assert_dev_dependency(
        &dev_dependencies,
        "rafter-runtime",
        "app examples may instantiate DurableRaftNode without making app depend on it",
    );
    assert_dev_dependency(
        &dev_dependencies,
        "rafter-storage",
        "app examples may use in-memory stores without making app depend on storage",
    );
}

fn manifest_section(manifest: &str, section: &str) -> String {
    let header = format!("[{section}]");
    let Some(start) = manifest.lines().position(|line| line.trim() == header) else {
        return String::new();
    };
    manifest
        .lines()
        .skip(start + 1)
        .take_while(|line| !line.trim_start().starts_with('['))
        .collect::<Vec<_>>()
        .join("\n")
}

fn assert_normal_dependency(dependencies: &str, crate_name: &str) {
    assert!(
        dependencies
            .lines()
            .any(|line| line.trim_start().starts_with(&format!("{crate_name} ="))),
        "expected normal dependency on {crate_name}; dependencies:\n{dependencies}"
    );
}

fn assert_dev_dependency(dev_dependencies: &str, crate_name: &str, reason: &str) {
    assert!(
        dev_dependencies
            .lines()
            .any(|line| line.trim_start().starts_with(&format!("{crate_name} ="))),
        "expected documented dev-dependency on {crate_name} ({reason}); dev-dependencies:\n{dev_dependencies}"
    );
}

fn assert_no_dependency(dependencies: &str, crate_name: &str, label: &str) {
    assert!(
        !dependencies
            .lines()
            .any(|line| line.trim_start().starts_with(&format!("{crate_name} ="))),
        "{label} must not include {crate_name}; dependencies:\n{dependencies}"
    );
}
