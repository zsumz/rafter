//! Scenarios for locked registry build-script source identity.

use std::{fs, path::Path, process::Command};

use super::cargo_graph::validate_registry_build_script_source_identity;

#[test]
fn registry_build_scripts_require_a_locked_archive_checksum() {
    let source = "registry+https://github.com/rust-lang/crates.io-index";
    let metadata = serde_json::json!({
        "packages": [{
            "name": "dependency",
            "version": "1.2.3",
            "source": source,
            "targets": [{"kind": ["custom-build"]}]
        }]
    })
    .to_string();
    let lock = format!(
        "version = 4\n\n[[package]]\nname = \"dependency\"\nversion = \"1.2.3\"\nsource = \"{source}\"\nchecksum = \"{}\"\n",
        "a".repeat(64)
    );

    validate_registry_build_script_source_identity(&metadata, &lock)
        .expect("a locked registry archive and producer digest bind build-script effects");

    let missing_checksum = lock
        .lines()
        .filter(|line| !line.starts_with("checksum ="))
        .collect::<Vec<_>>()
        .join("\n");
    let error = validate_registry_build_script_source_identity(&metadata, &missing_checksum)
        .expect_err("registry build scripts without locked source checksums fail closed")
        .to_string();
    assert!(error.contains("has no locked checksum"), "{error}");
}

#[test]
fn non_registry_build_script_sources_fail_closed() {
    let source = "git+https://example.invalid/dependency#0123456789abcdef";
    let metadata = serde_json::json!({
        "packages": [{
            "name": "dependency",
            "version": "1.2.3",
            "source": source,
            "targets": [{"kind": ["custom-build"]}]
        }]
    })
    .to_string();
    let lock = format!(
        "version = 4\n\n[[package]]\nname = \"dependency\"\nversion = \"1.2.3\"\nsource = \"{source}\"\n"
    );

    let error = validate_registry_build_script_source_identity(&metadata, &lock)
        .expect_err("Git build scripts do not have a locked registry archive checksum")
        .to_string();
    assert!(error.contains("unbound non-registry source"), "{error}");
}

#[test]
fn workspace_registry_build_scripts_are_present_in_the_full_locked_graph() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("invariant crate is nested below the workspace root");
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--locked", "--offline"])
        .current_dir(workspace)
        .output()
        .expect("capture full locked workspace metadata");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata = String::from_utf8(output.stdout).expect("Cargo metadata is UTF-8");
    let parsed: serde_json::Value = serde_json::from_str(&metadata).expect("parse Cargo metadata");
    let has_registry_build_script = parsed["packages"]
        .as_array()
        .expect("metadata packages")
        .iter()
        .any(|package| {
            package["source"]
                .as_str()
                .is_some_and(|source| source.starts_with("registry+"))
                && package["targets"].as_array().is_some_and(|targets| {
                    targets.iter().any(|target| {
                        target["kind"].as_array().is_some_and(|kinds| {
                            kinds
                                .iter()
                                .any(|kind| kind.as_str() == Some("custom-build"))
                        })
                    })
                })
        });
    assert!(
        has_registry_build_script,
        "the regression fixture requires at least one registry build script"
    );
    validate_registry_build_script_source_identity(
        &metadata,
        &fs::read_to_string(workspace.join("Cargo.lock")).expect("read workspace lockfile"),
    )
    .expect("every resolved registry build script has locked source provenance");
}
