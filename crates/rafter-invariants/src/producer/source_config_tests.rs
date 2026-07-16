use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use super::{cargo_config_sha256_with_home as digest_for_release, CargoRelease};

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
