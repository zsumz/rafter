use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use super::{
    cargo_config_sha256_with_home as digest_for_release, parse_tracked_source_paths,
    validate_manifest_path_overrides, validate_resolved_path_package_metadata,
    validate_trusted_cargo_package_metadata, CargoRelease,
};

const PRE_INCLUDE_CARGO: CargoRelease = CargoRelease::new(1, 93);
const INCLUDE_CAPABLE_CARGO: CargoRelease = CargoRelease::new(1, 94);

fn cargo_config_sha256_with_home(
    root: &Path,
    cargo_home: Option<&Path>,
) -> Result<String, Box<dyn std::error::Error>> {
    digest_for_release(root, cargo_home, INCLUDE_CAPABLE_CARGO)
}

static SCRATCH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    fn new(label: &str) -> Self {
        let sequence = SCRATCH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rafter-source-{label}-{}-{sequence}",
            std::process::id()
        ));
        Self::at(path)
    }

    fn at(path: PathBuf) -> Self {
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create source provenance scratch directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn write(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().expect("fixture file parent"))
        .expect("create fixture directory");
    fs::write(path, contents).expect("write fixture file");
}

#[test]
fn cargo_config_digest_tracks_parent_configs_from_workspace() {
    let scratch = ScratchDir::new("cargo-parent");
    let parent = scratch.path().join("parent");
    let workspace = parent.join("workspace");
    let cargo_home = scratch.path().join("cargo-home");
    fs::create_dir_all(&workspace).expect("create workspace");
    fs::create_dir_all(&cargo_home).expect("create Cargo home");

    let baseline = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash config hierarchy without parent config");
    let parent_config = parent.join(".cargo/config.toml");
    write(&parent_config, "[build]\njobs = 1\n");
    let added = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash config hierarchy with parent config");
    write(&parent_config, "[build]\njobs = 2\n");
    let changed = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash changed parent config");

    assert_ne!(baseline, added);
    assert_ne!(added, changed);
}

#[test]
fn cargo_config_digest_tracks_cargo_home() {
    let scratch = ScratchDir::new("cargo-home");
    let workspace = scratch.path().join("workspace");
    let cargo_home = scratch.path().join("cargo-home");
    fs::create_dir_all(&workspace).expect("create workspace");
    fs::create_dir_all(&cargo_home).expect("create Cargo home");

    let baseline = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash config hierarchy without Cargo home config");
    let home_config = cargo_home.join("config.toml");
    write(&home_config, "[net]\noffline = true\n");
    let added = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash Cargo home config");
    write(&home_config, "[net]\noffline = false\n");
    let changed = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash changed Cargo home config");

    assert_ne!(baseline, added);
    assert_ne!(added, changed);
}

#[test]
fn cargo_config_prefers_extensionless_file_and_hashes_its_identity() {
    let scratch = ScratchDir::new("cargo-file-precedence");
    let workspace = scratch.path().join("workspace");
    let cargo_home = scratch.path().join("cargo-home");
    let cargo_dir = workspace.join(".cargo");
    fs::create_dir_all(&workspace).expect("create workspace");
    fs::create_dir_all(&cargo_home).expect("create Cargo home");

    let config_toml = cargo_dir.join("config.toml");
    let config = cargo_dir.join("config");
    write(&config_toml, "[build]\njobs = 1\n");
    let toml_identity = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash config.toml identity");

    write(&config, "[build]\njobs = 1\n");
    let extensionless_identity = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash extensionless config identity");
    assert_ne!(toml_identity, extensionless_identity);

    write(&config_toml, "[build]\njobs = 99\n");
    let ignored_toml_change = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("ignore lower-priority config.toml");
    assert_eq!(extensionless_identity, ignored_toml_change);

    write(&config, "[build]\njobs = 2\n");
    let extensionless_change = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash changed extensionless config");
    assert_ne!(extensionless_identity, extensionless_change);
}

#[test]
fn cargo_config_digest_is_portable_across_checkout_roots() {
    let scratch = ScratchDir::new("cargo-root-portability");
    let mut digests = Vec::new();
    for name in ["producer", "aggregate"] {
        let parent = scratch.path().join(name);
        let workspace = parent.join("workspace");
        let cargo_home = scratch.path().join(format!("{name}-cargo-home"));
        fs::create_dir_all(&workspace).expect("create relocated workspace");
        fs::create_dir_all(&cargo_home).expect("create relocated Cargo home");
        write(&workspace.join(".cargo/config.toml"), "[build]\njobs = 2\n");
        write(&parent.join(".cargo/config"), "[net]\noffline = true\n");
        write(
            &cargo_home.join("config.toml"),
            "[term]\ncolor = \"never\"\n",
        );
        digests.push(
            cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
                .expect("hash relocated Cargo configuration hierarchy"),
        );
    }

    assert_eq!(digests[0], digests[1]);
}

#[test]
fn cargo_config_precedence_is_portable_across_unequal_empty_directory_depths() {
    let scratch = ScratchDir::new("cargo-precedence-portability");
    let layouts = [
        ("shallow", PathBuf::from("workspace")),
        (
            "deep",
            PathBuf::from("empty/levels/do/not/change/precedence/workspace"),
        ),
    ];
    let mut digests = Vec::new();
    for (name, workspace_suffix) in layouts {
        let hierarchy = scratch.path().join(name).join("hierarchy");
        let workspace = hierarchy.join(workspace_suffix);
        let cargo_home = scratch.path().join(format!("{name}-cargo-home"));
        fs::create_dir_all(&workspace).expect("create relocated workspace");
        fs::create_dir_all(&cargo_home).expect("create relocated Cargo home");
        write(&workspace.join(".cargo/config.toml"), "[build]\njobs = 2\n");
        write(
            &hierarchy.join(".cargo/config.toml"),
            "[net]\noffline = true\n",
        );
        write(
            &cargo_home.join("config.toml"),
            "[term]\ncolor = \"never\"\n",
        );
        digests.push(
            cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
                .expect("hash effective Cargo configuration precedence"),
        );
    }

    assert_eq!(digests[0], digests[1]);
}

#[test]
fn cargo_config_include_traversal_respects_cargo_release() {
    let scratch = ScratchDir::new("cargo-include-version");
    let workspace = scratch.path().join("workspace");
    let cargo_home = scratch.path().join("cargo-home");
    fs::create_dir_all(&workspace).expect("create workspace");
    fs::create_dir_all(&cargo_home).expect("create Cargo home");
    let config = workspace.join(".cargo/config.toml");
    let included = workspace.join(".cargo/shared.toml");
    write(&config, "include = [\"shared.toml\"]\n[build]\njobs = 1\n");
    write(&included, "[net]\noffline = true\n");

    let old_before = digest_for_release(&workspace, Some(&cargo_home), PRE_INCLUDE_CARGO)
        .expect("old Cargo hashes the active config");
    let new_before = digest_for_release(&workspace, Some(&cargo_home), INCLUDE_CAPABLE_CARGO)
        .expect("new Cargo hashes the included config");
    write(&included, "[net]\noffline = false\n");
    let old_after = digest_for_release(&workspace, Some(&cargo_home), PRE_INCLUDE_CARGO)
        .expect("old Cargo ignores the include target");
    let new_after = digest_for_release(&workspace, Some(&cargo_home), INCLUDE_CAPABLE_CARGO)
        .expect("new Cargo follows the include target");

    assert_eq!(old_before, old_after);
    assert_ne!(new_before, new_after);

    write(
        &config,
        "include = [\"shared.toml\"]\n# active config bytes changed\n[build]\njobs = 1\n",
    );
    let old_config_changed = digest_for_release(&workspace, Some(&cargo_home), PRE_INCLUDE_CARGO)
        .expect("old Cargo still hashes the active config bytes");
    assert_ne!(old_before, old_config_changed);
}

#[test]
fn cargo_config_digest_tracks_recursive_includes() {
    let scratch = ScratchDir::new("cargo-includes");
    let workspace = scratch.path().join("workspace");
    let cargo_home = scratch.path().join("cargo-home");
    fs::create_dir_all(&workspace).expect("create workspace");
    fs::create_dir_all(&cargo_home).expect("create Cargo home");
    write(
        &workspace.join(".cargo/config.toml"),
        "include = [\"shared.toml\"]\n[build]\njobs = 1\n",
    );
    write(
        &workspace.join(".cargo/shared.toml"),
        "include = [\"nested.toml\"]\n[net]\noffline = true\n",
    );
    let nested = workspace.join(".cargo/nested.toml");
    write(&nested, "[term]\ncolor = \"never\"\n");

    let baseline = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash recursive Cargo includes");
    write(&nested, "[term]\ncolor = \"always\"\n");
    let changed = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash changed recursive Cargo include");

    assert_ne!(baseline, changed);
}

#[test]
fn cargo_config_digest_tracks_optional_include_presence() {
    let scratch = ScratchDir::new("cargo-optional-include");
    let workspace = scratch.path().join("workspace");
    let cargo_home = scratch.path().join("cargo-home");
    fs::create_dir_all(&workspace).expect("create workspace");
    fs::create_dir_all(&cargo_home).expect("create Cargo home");
    write(
        &workspace.join(".cargo/config.toml"),
        "include = [{ path = \"local.toml\", optional = true }]\n",
    );

    let missing = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash missing optional Cargo include");
    write(&workspace.join(".cargo/local.toml"), "[build]\njobs = 2\n");
    let present = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect("hash present optional Cargo include");

    assert_ne!(missing, present);
}

#[test]
fn cargo_config_include_cycles_fail_closed() {
    let scratch = ScratchDir::new("cargo-include-cycle");
    let workspace = scratch.path().join("workspace");
    let cargo_home = scratch.path().join("cargo-home");
    fs::create_dir_all(&workspace).expect("create workspace");
    fs::create_dir_all(&cargo_home).expect("create Cargo home");
    write(
        &workspace.join(".cargo/config.toml"),
        "include = [\"cycle.toml\"]\n",
    );
    write(
        &workspace.join(".cargo/cycle.toml"),
        "include = [\"config.toml\"]\n",
    );

    let error = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect_err("recursive Cargo include cycle must fail closed");
    assert!(error.to_string().contains("include cycle"));
}

#[test]
fn cargo_config_rejects_unbound_build_input_settings() {
    let cases = [
        (
            "build-rustc",
            "[build]\nrustc = \"../rustc\"\n",
            "build.rustc",
        ),
        (
            "build-rustc-wrapper",
            "[build]\nrustc-wrapper = \"../wrapper\"\n",
            "build.rustc-wrapper",
        ),
        (
            "build-workspace-wrapper",
            "[build]\nrustc-workspace-wrapper = \"../wrapper\"\n",
            "build.rustc-workspace-wrapper",
        ),
        (
            "build-rustdoc",
            "[build]\nrustdoc = \"../rustdoc\"\n",
            "build.rustdoc",
        ),
        (
            "build-target",
            "[build]\ntarget = \"../target.json\"\n",
            "build.target",
        ),
        (
            "build-target-dir",
            "[build]\ntarget-dir = \"../external-target\"\n",
            "build.target-dir",
        ),
        (
            "build-rustflags",
            "[build]\nrustflags = [\"-L\", \"../native\"]\n",
            "build.rustflags",
        ),
        (
            "build-rustdocflags",
            "[build]\nrustdocflags = [\"--extern-html-root-url\"]\n",
            "build.rustdocflags",
        ),
        (
            "target-linker",
            "[target.x86_64-unknown-linux-gnu]\nlinker = \"../linker\"\n",
            "target.x86_64-unknown-linux-gnu.linker",
        ),
        (
            "target-runner",
            "[target.x86_64-unknown-linux-gnu]\nrunner = \"../runner\"\n",
            "target.x86_64-unknown-linux-gnu.runner",
        ),
        (
            "target-rustflags",
            "[target.x86_64-unknown-linux-gnu]\nrustflags = [\"-L../native\"]\n",
            "target.x86_64-unknown-linux-gnu.rustflags",
        ),
        (
            "target-rustdocflags",
            "[target.x86_64-unknown-linux-gnu]\nrustdocflags = [\"--cfg\", \"docsrs\"]\n",
            "target.x86_64-unknown-linux-gnu.rustdocflags",
        ),
        ("paths", "paths = [\"../dependency\"]\n", "paths"),
        (
            "patch",
            "[patch.crates-io]\nserde = { path = \"../patched-serde\" }\n",
            "patch",
        ),
        (
            "patch-registry-url",
            "[patch.\"https://example.invalid/source\"]\nserde = { path = \"../patched-serde\" }\n",
            "patch",
        ),
        (
            "patch-dotted",
            "patch.crates-io.serde.path = \"../patched-serde\"\n",
            "patch",
        ),
        (
            "source",
            "[source.crates-io]\nreplace-with = \"vendored\"\n",
            "source",
        ),
        ("env", "[env]\nRUSTFLAGS = \"-L../native\"\n", "env"),
        ("unstable", "[unstable]\nbuild-std = true\n", "unstable"),
    ];

    for (label, config, expected_setting) in cases {
        let scratch = ScratchDir::new(label);
        let workspace = scratch.path().join("workspace");
        let cargo_home = scratch.path().join("cargo-home");
        fs::create_dir_all(&workspace).expect("create workspace");
        fs::create_dir_all(&cargo_home).expect("create Cargo home");
        write(&workspace.join(".cargo/config.toml"), config);

        let error = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
            .expect_err("unbound Cargo build input must fail closed")
            .to_string();
        assert!(
            error.contains(expected_setting),
            "{label} reported the wrong setting: {error}"
        );
    }
}

#[test]
fn cargo_config_rejects_path_bases_and_alternate_lockfiles() {
    for (label, config) in [
        ("path-bases", "[path-bases]\nworkspace = \"../workspace\"\n"),
        (
            "resolver",
            "[resolver]\nlockfile-path = \"../Cargo.lock\"\n",
        ),
    ] {
        let scratch = ScratchDir::new(label);
        let workspace = scratch.path().join("workspace");
        let cargo_home = scratch.path().join("cargo-home");
        fs::create_dir_all(&workspace).expect("create workspace");
        fs::create_dir_all(&cargo_home).expect("create Cargo home");
        write(&workspace.join(".cargo/config.toml"), config);
        let error = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
            .expect_err("Cargo path bases and alternate lockfiles must fail closed")
            .to_string();
        assert!(error.contains(label), "{label}: {error}");
    }
}

#[test]
fn cargo_config_rejects_adversarial_external_rustc_wrapper() {
    let scratch = ScratchDir::new("cargo-external-wrapper");
    let workspace = scratch.path().join("workspace");
    let cargo_home = scratch.path().join("cargo-home");
    fs::create_dir_all(&workspace).expect("create workspace");
    fs::create_dir_all(&cargo_home).expect("create Cargo home");
    write(
        &scratch.path().join("external-rustc-wrapper"),
        "first wrapper\n",
    );
    write(
        &workspace.join(".cargo/config.toml"),
        "[build]\nrustc-wrapper = \"../external-rustc-wrapper\"\n",
    );

    let error = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect_err("external rustc wrapper must not remain an unbound build input")
        .to_string();
    assert!(error.contains("build.rustc-wrapper"));
}

#[test]
fn cargo_config_rejects_adversarial_external_path_patch() {
    let scratch = ScratchDir::new("cargo-external-patch");
    let workspace = scratch.path().join("workspace");
    let cargo_home = scratch.path().join("cargo-home");
    let patched_dependency = scratch.path().join("patched-dependency");
    fs::create_dir_all(&workspace).expect("create workspace");
    fs::create_dir_all(&cargo_home).expect("create Cargo home");
    fs::create_dir_all(&patched_dependency).expect("create patched dependency");
    write(
        &patched_dependency.join("Cargo.toml"),
        "[package]\nname = \"patched-dependency\"\nversion = \"1.0.0\"\n",
    );
    write(
        &patched_dependency.join("src/lib.rs"),
        "pub fn injected() {}\n",
    );
    write(
        &workspace.join(".cargo/config.toml"),
        "[patch.crates-io]\npatched-dependency = { path = \"../patched-dependency\" }\n",
    );

    let error = cargo_config_sha256_with_home(&workspace, Some(&cargo_home))
        .expect_err("external path patch must not remain an unbound source input")
        .to_string();
    assert!(error.contains("patch"), "{error}");
}

#[test]
fn cargo_metadata_rejects_manifest_external_path_inputs() {
    for (label, manifest) in [
        (
            "dependency",
            "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nexternal-dependency = { path = \"../external-dependency\" }\n",
        ),
        (
            "patch",
            "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\nexternal-dependency = \"0.1\"\n\n[patch.crates-io]\nexternal-dependency = { path = \"../external-dependency\" }\n",
        ),
    ] {
        let scratch = ScratchDir::new(&format!("cargo-manifest-external-{label}"));
        let workspace = scratch.path().join("workspace");
        let external = scratch.path().join("external-dependency");
        write(&workspace.join("Cargo.toml"), manifest);
        write(&workspace.join("src/lib.rs"), "pub fn fixture() {}\n");
        write(
            &external.join("Cargo.toml"),
            "[package]\nname = \"external-dependency\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
        );
        write(&external.join("src/lib.rs"), "pub fn external() {}\n");

        for arguments in [
            &["init", "-q"][..],
            &["add", "Cargo.toml", "src/lib.rs"][..],
        ] {
            assert!(Command::new("git")
                .args(arguments)
                .current_dir(&workspace)
                .status()
                .expect("run git fixture command")
                .success());
        }
        assert!(Command::new("cargo")
            .args(["generate-lockfile", "--offline"])
            .current_dir(&workspace)
            .status()
            .expect("generate fixture lockfile")
            .success());
        assert!(Command::new("git")
            .args(["add", "Cargo.lock"])
            .current_dir(&workspace)
            .status()
            .expect("track fixture lockfile")
            .success());

        let metadata = Command::new("cargo")
            .args([
                "metadata",
                "--format-version",
                "1",
                "--locked",
                "--offline",
                "--no-deps",
            ])
            .current_dir(&workspace)
            .output()
            .expect("capture fixture metadata");
        assert!(metadata.status.success(), "{label}");
        let metadata = String::from_utf8(metadata.stdout).expect("fixture metadata is UTF-8");
        let tracked = parse_tracked_source_paths("Cargo.toml\0Cargo.lock\0src/lib.rs\0")
            .expect("parse tracked fixture paths");
        let error = if label == "patch" {
            validate_manifest_path_overrides(&workspace, &tracked)
        } else {
            validate_resolved_path_package_metadata(&workspace, &metadata, &tracked)
        }
        .expect_err("an external manifest path input must fail closed")
        .to_string();
        let expected = if label == "patch" {
            "overrides are outside the source binding contract"
        } else {
            "outside the bound source tree"
        };
        assert!(error.contains(expected), "{label}: {error}");
    }
}

#[test]
fn cargo_metadata_accepts_tracked_in_tree_path_package() {
    let scratch = ScratchDir::new("cargo-tracked-path");
    let workspace = scratch.path();
    write(
        &workspace.join("Cargo.toml"),
        "[package]\nname = \"tracked-fixture\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[workspace]\n",
    );
    write(&workspace.join("src/lib.rs"), "pub fn fixture() {}\n");
    for arguments in [
        &["init", "-q"][..],
        &["add", "Cargo.toml", "src/lib.rs"][..],
    ] {
        assert!(Command::new("git")
            .args(arguments)
            .current_dir(workspace)
            .status()
            .expect("run git fixture command")
            .success());
    }
    assert!(Command::new("cargo")
        .args(["generate-lockfile", "--offline"])
        .current_dir(workspace)
        .status()
        .expect("generate fixture lockfile")
        .success());
    assert!(Command::new("git")
        .args(["add", "Cargo.lock"])
        .current_dir(workspace)
        .status()
        .expect("track fixture lockfile")
        .success());
    let metadata = Command::new("cargo")
        .args([
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--offline",
            "--no-deps",
        ])
        .current_dir(workspace)
        .output()
        .expect("capture tracked fixture metadata");
    assert!(metadata.status.success());
    let metadata = String::from_utf8(metadata.stdout).expect("fixture metadata is UTF-8");
    let tracked = parse_tracked_source_paths("Cargo.toml\0Cargo.lock\0src/lib.rs\0")
        .expect("parse tracked fixture paths");
    validate_manifest_path_overrides(workspace, &tracked)
        .expect("the workspace manifest override contract is satisfied");
    validate_resolved_path_package_metadata(workspace, &metadata, &tracked)
        .expect("the resolved path package is tracked inside the bound source tree");
}

#[test]
fn cargo_metadata_rejects_custom_build_targets() {
    let scratch = ScratchDir::new("cargo-custom-build");
    let root = scratch.path();
    write(
        &root.join("Cargo.toml"),
        "[package]\nname = \"fixture\"\nversion = \"0.1.0\"\n",
    );
    write(&root.join("build.rs"), "fn main() {}\n");
    let canonical_root = fs::canonicalize(root).expect("canonical scratch root");
    let metadata = serde_json::json!({
        "workspace_root": canonical_root,
        "packages": [{
            "source": null,
            "manifest_path": canonical_root.join("Cargo.toml"),
            "dependencies": [],
            "targets": [{
                "kind": ["custom-build"],
                "src_path": canonical_root.join("build.rs")
            }]
        }]
    })
    .to_string();
    let tracked = parse_tracked_source_paths("Cargo.toml\0build.rs\0")
        .expect("parse tracked custom-build paths");

    let error = validate_resolved_path_package_metadata(root, &metadata, &tracked)
        .expect_err("custom build scripts can inject unbound compiler inputs")
        .to_string();
    assert!(error.contains("custom build targets"), "{error}");
}

#[test]
fn cargo_manifest_rejects_patch_and_replace_even_when_they_are_in_tree() {
    for (section, override_table) in [
        (
            "patch",
            "[patch.crates-io]\ndependency = { path = \"dependency\" }\n",
        ),
        (
            "replace",
            "[replace]\n\"dependency:0.1.0\" = { path = \"dependency\" }\n",
        ),
    ] {
        let scratch = ScratchDir::new(&format!("cargo-manifest-{section}"));
        write(
            &scratch.path().join("Cargo.toml"),
            &format!("[workspace]\n{override_table}"),
        );
        let tracked = parse_tracked_source_paths("Cargo.toml\0")
            .expect("parse tracked manifest fixture path");
        let error = validate_manifest_path_overrides(scratch.path(), &tracked)
            .expect_err("manifest dependency overrides must fail closed")
            .to_string();
        assert!(error.contains(section), "{section}: {error}");
    }
}

#[test]
fn cargo_metadata_accepts_canonical_oracle_dependencies() {
    let (scratch, metadata) = oracle_dependency_metadata(
        "canonical",
        "rafter-invariant-test = { path = \"../crates/rafter-invariant-test\" }",
        "rafter-invariant-test-macros = { path = \"../rafter-invariant-test-macros\" }",
    );
    validate_trusted_cargo_package_metadata(scratch.path(), &metadata)
        .expect("canonical oracle package edges must remain trusted");
}

#[test]
fn cargo_metadata_rejects_tracked_aliased_oracle_package() {
    let (scratch, metadata) = oracle_dependency_metadata(
        "forged-oracle",
        "rafter-invariant-test = { package = \"forged-oracle\", path = \"../forged-oracle\" }",
        "rafter-invariant-test-macros = { path = \"../rafter-invariant-test-macros\" }",
    );
    let error = validate_trusted_cargo_package_metadata(scratch.path(), &metadata)
        .expect_err("a tracked package alias must not own the trusted oracle crate name")
        .to_string();
    assert!(
        error.contains("does not resolve to canonical package"),
        "{error}"
    );
}

#[test]
fn cargo_metadata_rejects_tracked_macro_package_substitution() {
    let (scratch, metadata) = oracle_dependency_metadata(
        "forged-macros",
        "rafter-invariant-test = { path = \"../crates/rafter-invariant-test\" }",
        "rafter-invariant-test-macros = { package = \"forged-macros\", path = \"../../forged-macros\" }",
    );
    let error = validate_trusted_cargo_package_metadata(scratch.path(), &metadata)
        .expect_err("a tracked package alias must not own the trusted attribute-macro crate name")
        .to_string();
    assert!(
        error.contains("does not resolve to canonical package"),
        "{error}"
    );
}

#[test]
fn cargo_metadata_rejects_noncanonical_protected_library_target() {
    let (scratch, metadata) = oracle_dependency_metadata_with_forged_target(
        "target-name-collision",
        "forged-oracle = { path = \"../forged-oracle\" }",
        "rafter-invariant-test-macros = { path = \"../rafter-invariant-test-macros\" }",
        Some("rafter_invariant_test"),
    );
    let error = validate_trusted_cargo_package_metadata(scratch.path(), &metadata)
        .expect_err("a noncanonical package must not expose the protected library target name")
        .to_string();
    assert!(error.contains("protected target name"), "{error}");
}

#[test]
fn cargo_metadata_rejects_path_dependency_hidden_outside_workspace_inventory() {
    let (scratch, metadata) = oracle_dependency_metadata_fixture(
        "excluded-path-package",
        "forged-oracle = { path = \"../forged-oracle\" }",
        "rafter-invariant-test-macros = { path = \"../rafter-invariant-test-macros\" }",
        None,
        false,
    );
    let error = validate_trusted_cargo_package_metadata(scratch.path(), &metadata)
        .expect_err("a path dependency hidden from no-deps metadata must fail closed")
        .to_string();
    assert!(
        error.contains("resolves to 0 workspace packages"),
        "{error}"
    );
}

fn oracle_dependency_metadata(
    label: &str,
    oracle_dependency: &str,
    macro_dependency: &str,
) -> (ScratchDir, String) {
    oracle_dependency_metadata_with_forged_target(label, oracle_dependency, macro_dependency, None)
}

fn oracle_dependency_metadata_with_forged_target(
    label: &str,
    oracle_dependency: &str,
    macro_dependency: &str,
    forged_oracle_target: Option<&str>,
) -> (ScratchDir, String) {
    oracle_dependency_metadata_fixture(
        label,
        oracle_dependency,
        macro_dependency,
        forged_oracle_target,
        true,
    )
}

fn oracle_dependency_metadata_fixture(
    label: &str,
    oracle_dependency: &str,
    macro_dependency: &str,
    forged_oracle_target: Option<&str>,
    include_forged_members: bool,
) -> (ScratchDir, String) {
    let scratch = ScratchDir::new(&format!("cargo-oracle-{label}"));
    let root = scratch.path();
    let workspace = if include_forged_members {
        "[workspace]\nresolver = \"2\"\nmembers = [\"app\", \"crates/rafter-invariant-test\", \"crates/rafter-invariant-test-macros\", \"forged-oracle\", \"forged-macros\"]\n"
    } else {
        "[workspace]\nresolver = \"2\"\nmembers = [\"app\", \"crates/rafter-invariant-test\", \"crates/rafter-invariant-test-macros\"]\nexclude = [\"forged-oracle\", \"forged-macros\"]\n"
    };
    write(&root.join("Cargo.toml"), workspace);
    write(
        &root.join("app/Cargo.toml"),
        &format!(
            "[package]\nname = \"app\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dev-dependencies]\n{oracle_dependency}\n"
        ),
    );
    write(&root.join("app/src/lib.rs"), "pub fn app() {}\n");
    write(
        &root.join("crates/rafter-invariant-test/Cargo.toml"),
        &format!(
            "[package]\nname = \"rafter-invariant-test\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[dependencies]\n{macro_dependency}\n"
        ),
    );
    write(
        &root.join("crates/rafter-invariant-test/src/lib.rs"),
        "pub fn oracle() {}\n",
    );
    write(
        &root.join("crates/rafter-invariant-test-macros/Cargo.toml"),
        "[package]\nname = \"rafter-invariant-test-macros\"\nversion = \"0.1.0\"\nedition = \"2021\"\n\n[lib]\nproc-macro = true\n",
    );
    write(
        &root.join("crates/rafter-invariant-test-macros/src/lib.rs"),
        "",
    );
    for (directory, package, target) in [
        ("forged-oracle", "forged-oracle", forged_oracle_target),
        ("forged-macros", "forged-macros", None),
    ] {
        let target =
            target.map_or_else(String::new, |name| format!("\n[lib]\nname = \"{name}\"\n"));
        write(
            &root.join(directory).join("Cargo.toml"),
            &format!(
                "[package]\nname = \"{package}\"\nversion = \"0.1.0\"\nedition = \"2021\"\n{target}"
            ),
        );
        write(&root.join(directory).join("src/lib.rs"), "");
    }
    assert!(Command::new("cargo")
        .args(["generate-lockfile", "--offline"])
        .current_dir(root)
        .status()
        .expect("generate oracle fixture lockfile")
        .success());
    let metadata = Command::new("cargo")
        .args([
            "metadata",
            "--format-version",
            "1",
            "--locked",
            "--offline",
            "--no-deps",
        ])
        .current_dir(root)
        .output()
        .expect("capture oracle fixture metadata");
    assert!(
        metadata.status.success(),
        "{}",
        String::from_utf8_lossy(&metadata.stderr)
    );
    (
        scratch,
        String::from_utf8(metadata.stdout).expect("oracle fixture metadata is UTF-8"),
    )
}
