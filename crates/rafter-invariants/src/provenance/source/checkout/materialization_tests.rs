//! Scenarios for races, aliases, modes, and ignored checkout materialization.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use super::{
    capture_materialization, validate_ignored_inventory, validate_ignored_path_types,
    CheckoutCommandRunner, CommandOutput, GeneratedOutputPolicy,
};

struct TestCommandRunner;

impl CheckoutCommandRunner for TestCommandRunner {
    fn run(
        &self,
        program: &str,
        arguments: &[&str],
        current_dir: &Path,
    ) -> Result<CommandOutput, Box<dyn std::error::Error>> {
        let output = Command::new(program)
            .args(arguments)
            .current_dir(current_dir)
            .output()?;
        let stdout = String::from_utf8(output.stdout)?;
        let stderr = String::from_utf8(output.stderr)?;
        if !output.status.success() {
            return Err(format!("fixture command {program} failed: {}", stderr.trim()).into());
        }
        Ok(CommandOutput { stdout, stderr })
    }
}

struct TestGeneratedOutputs;

impl GeneratedOutputPolicy for TestGeneratedOutputs {
    fn permits(&self, path: &Path) -> bool {
        reviewed_generated_output(path)
    }
}

struct TestRepository {
    root: PathBuf,
}

impl TestRepository {
    fn new() -> Self {
        Self::new_with_object_format(None)
    }

    fn new_with_object_format(object_format: Option<&str>) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/source-materialization-tests")
            .join(format!(
                "checkout-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(&root).expect("create materialization test repository");
        if let Some(object_format) = object_format {
            let argument = format!("--object-format={object_format}");
            git(&root, &["init", "-q", &argument]);
        } else {
            git(&root, &["init", "-q"]);
        }
        git(
            &root,
            &["config", "user.email", "invariants@example.invalid"],
        );
        git(&root, &["config", "user.name", "Invariant Tests"]);
        git(&root, &["config", "commit.gpgsign", "false"]);
        fs::write(root.join("tracked.txt"), b"recorded\n").expect("write baseline tracked source");
        git(&root, &["add", "--", "tracked.txt"]);
        git(&root, &["commit", "-qm", "baseline"]);
        Self { root }
    }

    fn capture(&self) -> Result<super::MaterializedSource, Box<dyn std::error::Error>> {
        capture_materialization(&self.root, &TestCommandRunner, &TestGeneratedOutputs)
    }

    fn status(&self) -> String {
        git(
            &self.root,
            &["status", "--porcelain=v1", "--untracked-files=all"],
        )
    }
}

impl Drop for TestRepository {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.root).expect("remove materialization test repository");
    }
}

fn git(root: &Path, arguments: &[&str]) -> String {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .output()
        .expect("run Git in materialization test repository");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout)
        .expect("Git test output is UTF-8")
        .trim()
        .to_owned()
}

fn reviewed_generated_output(path: &Path) -> bool {
    let components = path
        .components()
        .filter_map(|component| match component {
            std::path::Component::Normal(value) => Some(value.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>();
    matches!(components.as_slice(), [first, ..] if first == "target" || first == "store")
        || matches!(components.as_slice(), [first, second, ..]
            if (first == "artifacts"
                && (second == "invariants" || reviewed_tla_evidence_artifact(second)))
                || (first == "bench-compare" && second == "target")
                || (first == "fuzz" && second == "target")
                || (first == "tools" && second == "cache"))
        || matches!(components.as_slice(), [first, second, third, ..]
            if first == "crates" && second == "rafter-invariants" && third == "target")
        || matches!(components.as_slice(), [first, second, rest @ ..]
            if first == "specs" && second == "tla" && rest.iter().any(|value| value == "states"))
        || components.iter().any(|value| value == "__pycache__")
        || path.extension().is_some_and(|extension| extension == "pyc")
}

fn reviewed_tla_evidence_artifact(name: &str) -> bool {
    const FIXTURE_SUFFIXES: &[&str] = &[
        "ElectionSafety",
        "LogMatching-LogMatchingRecorderOnly",
        "LogMatching-SnapshotPrefixRecorderOnly",
        "LeaderCompleteness-LeaderCompletenessRecorderOnly",
        "CommittedPrefixStability-CommittedPrefixRecorderOnly",
        "StateMachineSafety",
        "StateMachineSafety-ApplicationEpochRecorderOnly",
        "StaleLeaderFencing-HigherTermRecorderOnly",
        "StaleLeaderFencing-StaleAuthorityRecorderOnly",
        "CommittedEntriesHaveQuorum-CommitQuorumRecorderOnly",
        "ReadBarrierLinearizability-ReadBarrierRecorderOnly",
    ];
    matches!(
        name,
        "tla-log"
            | "tla.log"
            | "tla-trace-log"
            | "tla-tool"
            | "tla-spec"
            | "tla-trace-spec"
            | "tla-detector-spec"
            | "tla-runner"
            | "tla-tool-asset-id"
            | "tla-tool-checksums"
            | "tla-config"
            | "tla-trace-config"
            | "tla-detector-config"
            | "tla-mutation-log"
            | "tla-producer"
            | "tla-checkpoint-contract"
            | "tla-checkpoint-inventory"
            | "tla-checkpoint-recovered-contract"
            | "tla-checkpoint-recovered-inventory"
            | "tla-checkpoint-recovery-report"
    ) || ["tla-detector-log-", "tla-detector-config-"]
        .into_iter()
        .any(|prefix| {
            name.strip_prefix(prefix)
                .is_some_and(|suffix| FIXTURE_SUFFIXES.contains(&suffix))
        })
}

#[test]
fn clean_checkout_has_stable_raw_materialization_receipt() {
    let repository = TestRepository::new();
    let first = repository.capture().expect("capture clean materialization");
    let second = repository
        .capture()
        .expect("recapture clean materialization");

    assert_eq!(first.commit, second.commit);
    assert_eq!(first.tree, second.tree);
    assert_eq!(first.receipt, second.receipt);
    assert_eq!(first.receipt.contract, "git-head-worktree-raw-v1");
    assert_eq!(first.receipt.tracked_entries, 1);
    assert_eq!(first.receipt.submodules, 0);
}

#[test]
fn sha256_git_repository_uses_its_declared_object_format() {
    let repository = TestRepository::new_with_object_format(Some("sha256"));
    let materialized = repository
        .capture()
        .expect("capture SHA-256 Git materialization");

    assert_eq!(materialized.commit.len(), 64);
    assert_eq!(materialized.tree.len(), 64);
    assert_eq!(materialized.receipt.sha256.len(), 64);
}

#[test]
fn replacement_objects_cannot_redirect_the_recorded_head_tree() {
    let repository = TestRepository::new();
    let original_commit = git(&repository.root, &["rev-parse", "HEAD"]);
    fs::write(repository.root.join("tracked.txt"), b"replacement\n")
        .expect("write replacement commit source");
    git(&repository.root, &["add", "--", "tracked.txt"]);
    git(&repository.root, &["commit", "-qm", "replacement"]);
    let replacement_commit = git(&repository.root, &["rev-parse", "HEAD"]);
    git(&repository.root, &["reset", "--hard", &original_commit]);
    git(
        &repository.root,
        &["replace", &original_commit, &replacement_commit],
    );
    git(&repository.root, &["reset", "--hard", "HEAD"]);
    assert!(
        repository.status().is_empty(),
        "replacement ref is status-clean"
    );
    let replaced_tree = git(&repository.root, &["rev-parse", "HEAD^{tree}"]);
    let original_tree = git(
        &repository.root,
        &["--no-replace-objects", "rev-parse", "HEAD^{tree}"],
    );
    assert_ne!(
        replaced_tree, original_tree,
        "fixture replacement redirects Git"
    );

    let error = repository
        .capture()
        .expect_err("replacement refs cannot redirect source capture")
        .to_string();
    assert!(error.contains("bytes differ"), "{error}");
}

#[test]
fn assume_unchanged_cannot_hide_modified_worktree_bytes() {
    let repository = TestRepository::new();
    git(
        &repository.root,
        &["update-index", "--assume-unchanged", "tracked.txt"],
    );
    fs::write(repository.root.join("tracked.txt"), b"substituted\n")
        .expect("replace assume-unchanged source bytes");
    assert!(repository.status().is_empty(), "porcelain status is hidden");

    let error = repository
        .capture()
        .expect_err("raw materialization must reject hidden byte changes")
        .to_string();
    assert!(error.contains("bytes differ"), "{error}");
}

#[test]
fn skip_worktree_cannot_hide_modified_worktree_bytes() {
    let repository = TestRepository::new();
    git(
        &repository.root,
        &["update-index", "--skip-worktree", "tracked.txt"],
    );
    fs::write(repository.root.join("tracked.txt"), b"substituted\n")
        .expect("replace skip-worktree source bytes");
    assert!(repository.status().is_empty(), "porcelain status is hidden");

    let error = repository
        .capture()
        .expect_err("raw materialization must reject hidden byte changes")
        .to_string();
    assert!(error.contains("bytes differ"), "{error}");
}

#[cfg(unix)]
#[test]
fn executable_mode_drift_is_rejected() {
    let repository = TestRepository::new();
    let path = repository.root.join("tracked.txt");
    let mut permissions = fs::metadata(&path)
        .expect("read tracked permissions")
        .permissions();
    permissions.set_mode(permissions.mode() | 0o111);
    fs::set_permissions(&path, permissions).expect("change tracked executable mode");

    let error = repository
        .capture()
        .expect_err("raw materialization must reject executable mode drift")
        .to_string();
    assert!(error.contains("executable mode differs"), "{error}");
}

#[cfg(unix)]
#[test]
fn owner_executable_mode_drift_hidden_by_assume_unchanged_is_rejected() {
    let repository = TestRepository::new();
    let path = repository.root.join("tracked.txt");
    let mut permissions = fs::metadata(&path)
        .expect("read tracked permissions")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&path, permissions).expect("make tracked file executable");
    git(&repository.root, &["add", "--", "tracked.txt"]);
    git(&repository.root, &["commit", "-qm", "track executable"]);
    git(
        &repository.root,
        &["update-index", "--assume-unchanged", "tracked.txt"],
    );
    let mut permissions = fs::metadata(&path)
        .expect("reread tracked permissions")
        .permissions();
    permissions.set_mode(0o655);
    fs::set_permissions(&path, permissions).expect("clear only the owner executable bit");
    assert!(
        repository.status().is_empty(),
        "porcelain mode drift is hidden"
    );

    let error = repository
        .capture()
        .expect_err("Git's owner executable bit must be checked exactly")
        .to_string();
    assert!(error.contains("executable mode differs"), "{error}");
}

#[cfg(unix)]
#[test]
fn tracked_symlinks_fail_closed_even_when_the_target_text_is_stable() {
    let repository = TestRepository::new();
    std::os::unix::fs::symlink("recorded-target", repository.root.join("linked"))
        .expect("create tracked symlink");
    git(&repository.root, &["add", "--", "linked"]);
    git(&repository.root, &["commit", "-qm", "track symlink"]);

    let error = repository
        .capture()
        .expect_err("tracked symlinks can dereference unbound compiler inputs")
        .to_string();
    assert!(error.contains("Git symlinks are outside"), "{error}");
}

#[test]
fn ignored_paths_outside_reviewed_generated_roots_fail_closed() {
    let repository = TestRepository::new();
    fs::write(repository.root.join(".gitignore"), "ignored.rs\n")
        .expect("write ignored source rule");
    git(&repository.root, &["add", "--", ".gitignore"]);
    git(&repository.root, &["commit", "-qm", "ignore source input"]);
    fs::write(repository.root.join("ignored.rs"), "fn substituted() {}\n")
        .expect("write ignored source input");
    assert!(
        repository.status().is_empty(),
        "ignored source is status-clean"
    );

    let error = repository
        .capture()
        .expect_err("unreviewed ignored inputs must fail closed")
        .to_string();
    assert!(
        error.contains("outside reviewed generated-output roots"),
        "{error}"
    );
}

#[test]
fn nul_inventory_preserves_leading_spaces_in_ignored_paths() {
    let error = validate_ignored_inventory(" target/generated.rs\0", &TestGeneratedOutputs)
        .expect_err("leading spaces in NUL inventories must not be trimmed")
        .to_string();
    assert!(
        error.contains("outside reviewed generated-output roots"),
        "{error}"
    );
}

#[test]
fn reviewed_generated_output_roots_do_not_change_source_materialization() {
    let repository = TestRepository::new();
    fs::write(repository.root.join(".gitignore"), "/target/\n")
        .expect("write generated-output rule");
    git(&repository.root, &["add", "--", ".gitignore"]);
    git(
        &repository.root,
        &["commit", "-qm", "ignore generated output"],
    );
    fs::create_dir_all(repository.root.join("target")).expect("create target output root");
    fs::write(repository.root.join("target/generated.bin"), b"generated\n")
        .expect("write generated output");

    repository
        .capture()
        .expect("reviewed generated outputs are not compiler source inputs");
}

#[test]
fn vanished_files_under_generated_roots_do_not_change_source_materialization() {
    let repository = TestRepository::new();
    fs::create_dir_all(repository.root.join("target/rafter-invariants/telemetry"))
        .expect("create generated telemetry root");

    validate_ignored_path_types(
        &repository.root,
        "target/rafter-invariants/telemetry/1234-7.pgid\0",
    )
    .expect("vanished generated telemetry is ignored churn");
}

#[cfg(unix)]
#[test]
fn ignored_symlinks_under_generated_roots_fail_closed() {
    let repository = TestRepository::new();
    fs::write(repository.root.join(".gitignore"), "/target/\n")
        .expect("write generated-output rule");
    fs::write(repository.root.join("first.bin"), b"first\n").expect("write first source");
    fs::write(repository.root.join("second.bin"), b"second\n").expect("write second source");
    git(
        &repository.root,
        &["add", "--", ".gitignore", "first.bin", "second.bin"],
    );
    git(
        &repository.root,
        &["commit", "-qm", "track selectable source inputs"],
    );
    fs::create_dir_all(repository.root.join("target")).expect("create target output root");
    std::os::unix::fs::symlink("../first.bin", repository.root.join("target/selected.bin"))
        .expect("create ignored source-selection symlink");

    let error = repository
        .capture()
        .expect_err("ignored symlinks must not select compiler inputs")
        .to_string();
    assert!(error.contains("ignored filesystem symlink"), "{error}");
}

#[test]
fn invariant_crate_target_root_is_a_reviewed_generated_output() {
    let repository = TestRepository::new();
    fs::write(
        repository.root.join(".gitignore"),
        "/crates/rafter-invariants/target/\n",
    )
    .expect("write invariant target ignore rule");
    git(&repository.root, &["add", "--", ".gitignore"]);
    git(
        &repository.root,
        &["commit", "-qm", "ignore invariant telemetry target"],
    );
    fs::create_dir_all(repository.root.join("crates/rafter-invariants/target"))
        .expect("create invariant target root");
    fs::write(
        repository
            .root
            .join("crates/rafter-invariants/target/telemetry.json"),
        b"{}\n",
    )
    .expect("write invariant telemetry");

    repository
        .capture()
        .expect("the repository-owned invariant target root is reviewed");
}

#[test]
fn nested_target_allowlist_is_exact() {
    assert!(reviewed_generated_output(Path::new(
        "crates/rafter-invariants/target/telemetry.json"
    )));
    assert!(!reviewed_generated_output(Path::new(
        "crates/other/target/generated.rs"
    )));
}

#[test]
fn tla_evidence_artifacts_are_reviewed_generated_output() {
    assert!(reviewed_generated_output(Path::new("artifacts/tla-config")));
    assert!(reviewed_generated_output(Path::new("artifacts/tla.log")));
    assert!(reviewed_generated_output(Path::new(
        "artifacts/tla-producer"
    )));
    assert!(reviewed_generated_output(Path::new(
        "artifacts/tla-detector-config-ElectionSafety"
    )));
    assert!(reviewed_generated_output(Path::new(
        "artifacts/tla-detector-log-ElectionSafety"
    )));
    assert!(!reviewed_generated_output(Path::new(
        "artifacts/tla-detector-config-UnknownPredicate"
    )));
    assert!(!reviewed_generated_output(Path::new(
        "artifacts/unchecked-source.rs"
    )));
}

#[test]
fn arbitrary_nested_cargo_target_roots_fail_closed() {
    let repository = TestRepository::new();
    fs::write(
        repository.root.join(".gitignore"),
        "/crates/other/target/\n",
    )
    .expect("write arbitrary target ignore rule");
    git(&repository.root, &["add", "--", ".gitignore"]);
    git(
        &repository.root,
        &["commit", "-qm", "ignore arbitrary nested target"],
    );
    fs::create_dir_all(repository.root.join("crates/other/target"))
        .expect("create arbitrary target root");
    fs::write(
        repository.root.join("crates/other/target/generated.rs"),
        "fn substituted() {}\n",
    )
    .expect("write arbitrary generated source");

    let error = repository
        .capture()
        .expect_err("only the repository-owned nested target root is reviewed")
        .to_string();
    assert!(
        error.contains("outside reviewed generated-output roots"),
        "{error}"
    );
}

#[test]
fn fixture_repositories_disable_inherited_commit_signing() {
    let repository = TestRepository::new();
    assert_eq!(
        git(
            &repository.root,
            &["config", "--local", "--get", "commit.gpgsign"]
        ),
        "false"
    );
}

#[test]
fn gitlinks_fail_closed_as_unsupported_source_materialization() {
    let repository = TestRepository::new();
    let commit = git(&repository.root, &["rev-parse", "HEAD"]);
    let cache_info = format!("160000,{commit},nested-submodule");
    git(
        &repository.root,
        &["update-index", "--add", "--cacheinfo", &cache_info],
    );
    git(&repository.root, &["commit", "-qm", "record gitlink"]);

    let error = repository
        .capture()
        .expect_err("submodules are outside the materialization contract")
        .to_string();
    assert!(error.contains("submodules are outside"), "{error}");
}
