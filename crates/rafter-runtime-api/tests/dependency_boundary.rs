use std::{fs, path::Path};

#[test]
fn rafter_runtime_api_dependency_boundary_depends_only_on_core() {
    let manifest = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
        .expect("read rafter-runtime-api Cargo.toml");
    let dependencies = manifest_section(&manifest, "dependencies");

    assert_normal_dependency(&dependencies, "rafter");
    for forbidden in [
        "rafter-storage",
        "rafter-runtime",
        "rafter-app",
        "rafter-service",
        "rafter-multiraft",
        "rafter-codec",
        "rafter-transport-tcp-insecure",
    ] {
        assert_no_dependency(
            &dependencies,
            forbidden,
            "rafter-runtime-api normal dependencies",
        );
    }
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

fn assert_no_dependency(dependencies: &str, crate_name: &str, label: &str) {
    assert!(
        !dependencies
            .lines()
            .any(|line| line.trim_start().starts_with(&format!("{crate_name} ="))),
        "{label} must not include {crate_name}; dependencies:\n{dependencies}"
    );
}
