//! Adversarial private Cargo metadata authority scenarios.

use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use serde_json::json;

use super::CompilationGraph;

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

#[test]
fn ambient_workspace_root_is_rejected() {
    with_fixture(|fixture| {
        let mut metadata = fixture.metadata();
        metadata["workspace_root"] = json!(fixture.outside_root);
        let error = fixture
            .parse(&metadata, 0)
            .expect_err("ambient root must fail");
        assert!(
            error.contains("outside the authenticated workspace"),
            "{error}"
        );
    });
}

#[test]
fn path_package_manifest_escape_is_rejected() {
    with_fixture(|fixture| {
        let mut metadata = fixture.metadata();
        metadata["packages"][0]["manifest_path"] = json!(fixture.outside_manifest);
        let error = fixture
            .parse(&metadata, 0)
            .expect_err("manifest escape must fail");
        assert!(
            error.contains("escapes the authenticated workspace"),
            "{error}"
        );
    });
}

#[test]
fn unsupported_registry_source_is_rejected() {
    with_fixture(|fixture| {
        let mut metadata = fixture.metadata();
        metadata["packages"][0]["source"] = json!("git+https://example.invalid/repo");
        let error = fixture
            .parse(&metadata, 0)
            .expect_err("foreign source must fail");
        assert!(error.contains("unsupported package source"), "{error}");
    });
}

#[test]
fn registry_package_inventory_mismatch_is_rejected() {
    with_fixture(|fixture| {
        let error = fixture
            .parse(&fixture.metadata(), 1)
            .expect_err("missing registry package must fail");
        assert!(error.contains("resolved 0 registry packages"), "{error}");
    });
}

fn with_fixture(test: impl FnOnce(&Fixture)) {
    let id = NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed);
    let root = PathBuf::from("target/rafter-invariants/metadata-tests")
        .join(format!("{}-{id}", std::process::id()));
    let workspace = root.join("workspace");
    let package = workspace.join("fixture");
    let vendor = root.join("vendor");
    let outside_root = root.join("outside");
    for directory in [&package.join("src"), &vendor, &outside_root] {
        fs::create_dir_all(directory).expect("create metadata fixture directory");
    }
    fs::write(
        package.join("Cargo.toml"),
        "[package]\nname='fixture'\nversion='0.0.1'\n",
    )
    .expect("write package manifest");
    fs::write(package.join("src/lib.rs"), "pub fn fixture() {}\n").expect("write package source");
    fs::write(
        outside_root.join("Cargo.toml"),
        "[package]\nname='outside'\nversion='0.0.1'\n",
    )
    .expect("write outside manifest");
    let fixture = Fixture {
        workspace: fs::canonicalize(workspace).expect("canonical workspace"),
        package: fs::canonicalize(package).expect("canonical package"),
        vendor: fs::canonicalize(vendor).expect("canonical vendor"),
        outside_root: fs::canonicalize(&outside_root).expect("canonical outside root"),
        outside_manifest: fs::canonicalize(outside_root.join("Cargo.toml"))
            .expect("canonical outside manifest"),
    };
    test(&fixture);
}

struct Fixture {
    workspace: PathBuf,
    package: PathBuf,
    vendor: PathBuf,
    outside_root: PathBuf,
    outside_manifest: PathBuf,
}

impl Fixture {
    fn metadata(&self) -> serde_json::Value {
        json!({
            "workspace_root": self.workspace,
            "packages": [{
                "name": "fixture",
                "version": "0.0.1",
                "id": "fixture 0.0.1 (path+file:///fixture)",
                "source": null,
                "manifest_path": self.package.join("Cargo.toml"),
                "targets": [{
                    "name": "fixture",
                    "kind": ["lib"],
                    "src_path": self.package.join("src/lib.rs")
                }],
            }],
        })
    }

    fn parse(
        &self,
        metadata: &serde_json::Value,
        required_registry_packages: usize,
    ) -> Result<CompilationGraph, String> {
        CompilationGraph::parse_fixture(
            &serde_json::to_vec(metadata).expect("serialize metadata"),
            &self.workspace,
            &self.vendor,
            required_registry_packages,
        )
    }
}
