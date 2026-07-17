use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use crate::{SourceMaterializationReceipt, SourceReceipt, ToolReceipt};

use super::{
    bind_adjacent_tool_inputs, find_tool, layer_contract, maelstrom_jar_path,
    tool_identity_arguments, tool_version_output, verify_layer_contract, CargoRelease,
};

static SCRATCH_SEQUENCE: AtomicU64 = AtomicU64::new(0);

struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    fn new(label: &str) -> Self {
        let sequence = SCRATCH_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rafter-source-identity-{label}-{}-{sequence}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).expect("create source identity scratch directory");
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

#[test]
fn registered_tools_have_exact_reviewed_identity_probes() {
    let cases: &[(&str, &[&str])] = &[
        ("java", &["-version"]),
        ("maelstrom", &["serve", "--help"]),
        ("dot", &["-V"]),
        ("gnuplot", &["--version"]),
    ];
    let registered = ["tests", "simulator", "tla", "maelstrom"]
        .into_iter()
        .flat_map(|layer| {
            layer_contract(layer)
                .expect("registered layer contract")
                .tools
                .iter()
                .copied()
        })
        .collect::<BTreeSet<_>>();
    let probed = cases.iter().map(|(name, _)| *name).collect::<BTreeSet<_>>();

    assert_eq!(registered, probed);
    for &(name, arguments) in cases {
        assert_eq!(
            tool_identity_arguments(name).expect("reviewed identity probe"),
            arguments
        );
    }
    assert!(tool_identity_arguments("unregistered-tool").is_err());
}

#[test]
fn tool_version_preserves_combined_stdout_and_stderr() {
    assert_eq!(
        tool_version_output("fixture-tool", "stdout\n", "stderr\n")
            .expect("combined identity output"),
        "stdout\nstderr"
    );
}

#[test]
fn maelstrom_tool_identity_binds_adjacent_jar_replacement() {
    let scratch = ScratchDir::new("maelstrom-jar");
    let launcher = scratch.path().join("maelstrom");
    let jar = scratch.path().join("lib/maelstrom.jar");
    fs::create_dir_all(jar.parent().expect("Maelstrom JAR parent"))
        .expect("create Maelstrom lib directory");
    fs::write(&launcher, b"fixed launcher bytes").expect("write Maelstrom launcher");
    fs::write(&jar, b"first jar bytes").expect("write first Maelstrom JAR");

    let first = bind_adjacent_tool_inputs("maelstrom", "fixed probe output".to_owned(), &launcher)
        .expect("bind first Maelstrom JAR");
    fs::write(&jar, b"replacement jar bytes").expect("replace Maelstrom JAR");
    let replacement =
        bind_adjacent_tool_inputs("maelstrom", "fixed probe output".to_owned(), &launcher)
            .expect("bind replacement Maelstrom JAR");

    assert_ne!(first, replacement);
    assert!(first.starts_with("fixed probe output\n"));
    assert!(replacement.starts_with("fixed probe output\n"));
}

#[test]
#[cfg(unix)]
fn maelstrom_jar_resolution_is_identical_through_a_symlinked_launcher() {
    use std::os::unix::fs::symlink;

    let scratch = ScratchDir::new("maelstrom-symlink");
    let installation = scratch.path().join("installation");
    let launcher = installation.join("maelstrom");
    let jar = installation.join("lib/maelstrom.jar");
    let path_launcher = scratch.path().join("bin/maelstrom");
    fs::create_dir_all(jar.parent().expect("Maelstrom JAR parent"))
        .expect("create Maelstrom installation");
    fs::create_dir_all(path_launcher.parent().expect("PATH launcher parent"))
        .expect("create PATH directory");
    fs::write(&launcher, b"launcher").expect("write canonical launcher");
    fs::write(&jar, b"jar").expect("write adjacent JAR");
    symlink(&launcher, &path_launcher).expect("symlink launcher into PATH directory");

    assert_eq!(
        fs::canonicalize(
            maelstrom_jar_path(&path_launcher).expect("resolve JAR through launcher symlink")
        )
        .expect("canonicalize resolved JAR"),
        fs::canonicalize(&jar).expect("canonicalize expected JAR")
    );
}

#[test]
fn every_maelstrom_scenario_builds_from_the_source_bound_workspace() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let common = fs::read_to_string(root.join("scripts/maelstrom-lin-kv-common"))
        .expect("read shared Maelstrom launcher");
    assert!(common.contains("cd \"$REPO_ROOT\"\n    cargo build --locked \"$@\""));

    for script in [
        "maelstrom-lin-kv",
        "maelstrom-lin-kv-membership-change",
        "maelstrom-lin-kv-leader-restart",
        "maelstrom-lin-kv-app-persist-crash",
        "maelstrom-lin-kv-repeated-restart",
        "maelstrom-lin-kv-lease-isolation",
        "maelstrom-lin-kv-forced-snapshot",
    ] {
        let source = fs::read_to_string(root.join("scripts").join(script))
            .unwrap_or_else(|error| panic!("read {script}: {error}"));
        assert!(
            source.contains("rafter_maelstrom_cargo_build"),
            "{script} bypasses the source-bound Cargo helper"
        );
        assert!(
            source.contains("RAFTER_MAELSTROM_SCRIPT_DIR"),
            "{script} cannot resolve adjacent helpers when executed by descriptor"
        );
        assert!(
            !source.contains("cargo build"),
            "{script} launches Cargo from the trial working directory"
        );
    }
}

#[test]
fn tla_fetch_can_resolve_pins_when_the_runner_is_descriptor_bound() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source = fs::read_to_string(root.join("scripts/tla-model-check")).expect("read TLA runner");

    assert!(source.contains("RAFTER_TLA_REPO_ROOT"));
}

#[test]
fn cargo_release_parsing_has_the_config_include_boundary() {
    let before = CargoRelease::from_verbose_identity("cargo 1.93.1\nrelease: 1.93.1\n")
        .expect("parse Cargo release before config includes");
    let boundary = CargoRelease::from_verbose_identity("cargo 1.94.0\nrelease: 1.94.0\n")
        .expect("parse Cargo release with config includes");

    assert!(!before.follows_config_includes());
    assert!(boundary.follows_config_includes());
}

#[test]
fn dot_identity_probe_runs_when_dot_is_installed() {
    if find_tool("dot").is_none() {
        return;
    }

    let arguments = tool_identity_arguments("dot").expect("reviewed dot identity probe");
    let output = std::process::Command::new("dot")
        .args(arguments)
        .output()
        .expect("run dot identity probe");
    assert!(
        output.status.success(),
        "dot identity probe failed with {:?}",
        output.status.code()
    );
    let stdout = String::from_utf8(output.stdout).expect("dot stdout is UTF-8");
    let stderr = String::from_utf8(output.stderr).expect("dot stderr is UTF-8");
    assert!(
        !stderr.trim().is_empty(),
        "dot -V must expose its stderr-only version output"
    );
    let version =
        tool_version_output("dot", &stdout, &stderr).expect("capture combined dot identity output");
    assert!(
        version.to_ascii_lowercase().contains("graphviz"),
        "unexpected dot -V identity output: {version:?}"
    );
}

#[test]
#[cfg(unix)]
fn tool_version_rejects_empty_combined_output() {
    let error = tool_version_output("fixture-tool", "", "")
        .expect_err("empty version command must fail")
        .to_string();
    assert!(error.contains("empty identity output"));
}

fn source(build_profile: &str, features: &[&str], tools: &[&str]) -> SourceReceipt {
    SourceReceipt {
        commit: "commit".to_owned(),
        tree: "tree".to_owned(),
        materialization: SourceMaterializationReceipt {
            contract: "git-head-worktree-raw-v1".to_owned(),
            sha256: "0".repeat(64),
            tracked_entries: 1,
            submodules: 0,
        },
        cargo_lock_sha256: "0".repeat(64),
        cargo: "cargo".to_owned(),
        cargo_sha256: "0".repeat(64),
        cargo_config_sha256: "0".repeat(64),
        rustc: "rustc".to_owned(),
        rustc_sha256: "0".repeat(64),
        target: "target".to_owned(),
        build_profile: build_profile.to_owned(),
        features: features.iter().map(|value| (*value).to_owned()).collect(),
        tools: tools
            .iter()
            .map(|name| {
                (
                    (*name).to_owned(),
                    ToolReceipt {
                        version: "version".to_owned(),
                        sha256: "0".repeat(64),
                    },
                )
            })
            .collect(),
        environment_sha256: "0".repeat(64),
        clean: true,
    }
}

#[test]
fn layer_contract_rejects_altered_build_profile_and_features() {
    let exact = source("test", &["no-default-features"], &[]);
    verify_layer_contract("tests", &exact).expect("exact tests contract");

    let mut altered_profile = exact.clone();
    altered_profile.build_profile = "release".to_owned();
    assert!(verify_layer_contract("tests", &altered_profile).is_err());

    let mut altered_features = exact;
    altered_features.features = vec!["internal-test-hooks".to_owned()];
    assert!(verify_layer_contract("tests", &altered_features).is_err());
}

#[test]
fn layer_contract_rejects_cross_layer_receipts_and_tool_drift() {
    let simulator = source("release-and-test", &["internal-test-hooks"], &[]);
    assert!(verify_layer_contract("tests", &simulator).is_err());

    let mut tla = source("tla", &[], &["java"]);
    verify_layer_contract("tla", &tla).expect("exact TLA contract");
    tla.tools.insert(
        "curl".to_owned(),
        ToolReceipt {
            version: "version".to_owned(),
            sha256: "0".repeat(64),
        },
    );
    assert!(verify_layer_contract("tla", &tla).is_err());
}
