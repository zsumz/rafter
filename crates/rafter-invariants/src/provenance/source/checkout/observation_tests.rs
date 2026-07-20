//! Scenarios for checkout mutations during a source observation.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
};

use super::{observe_checkout_with, CheckoutCommandRunner, CommandOutput, GeneratedOutputPolicy};

struct NoGeneratedOutputs;

impl GeneratedOutputPolicy for NoGeneratedOutputs {
    fn permits(&self, _path: &Path) -> bool {
        false
    }
}

struct MutateBeforeFinalStatus {
    status_calls: AtomicUsize,
}

impl CheckoutCommandRunner for MutateBeforeFinalStatus {
    fn run(
        &self,
        program: &str,
        arguments: &[&str],
        current_dir: &Path,
    ) -> Result<CommandOutput, Box<dyn std::error::Error>> {
        if program == "git"
            && arguments == ["status", "--porcelain=v1", "--untracked-files=all"]
            && self.status_calls.fetch_add(1, Ordering::Relaxed) == 1
        {
            fs::write(current_dir.join("late-untracked-source"), b"changed\n")?;
        }
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

struct TestRepository {
    root: PathBuf,
}

impl TestRepository {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/source-observation-tests")
            .join(format!(
                "checkout-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("src")).expect("create source observation fixture");
        fs::write(
            root.join("Cargo.toml"),
            "[package]\nname = \"source-observation-fixture\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[workspace]\nmembers = [\"crates/rafter-invariant-test\", \"crates/rafter-invariant-test-macros\"]\n\n[dependencies]\nrafter-invariant-test-macros = { path = \"crates/rafter-invariant-test-macros\" }\n\n[dev-dependencies]\nrafter-invariant-test = { path = \"crates/rafter-invariant-test\" }\n",
        )
        .expect("write fixture manifest");
        fs::write(root.join("src/lib.rs"), "pub fn value() -> u8 { 1 }\n")
            .expect("write fixture source");
        write_package(
            &root,
            "rafter-invariant-test",
            "[package]\nname = \"rafter-invariant-test\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
            "pub fn marker() {}\n",
        );
        write_package(
            &root,
            "rafter-invariant-test-macros",
            "[package]\nname = \"rafter-invariant-test-macros\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[lib]\nproc-macro = true\n",
            "extern crate proc_macro;\n",
        );
        command(&root, "cargo", &["generate-lockfile", "--offline"]);
        command(&root, "git", &["init", "-q"]);
        command(
            &root,
            "git",
            &["config", "user.email", "invariants@example.invalid"],
        );
        command(&root, "git", &["config", "user.name", "Invariant Tests"]);
        command(&root, "git", &["config", "commit.gpgsign", "false"]);
        command(&root, "git", &["add", "--", "."]);
        command(&root, "git", &["commit", "-qm", "baseline"]);
        Self { root }
    }
}

fn write_package(root: &Path, package: &str, manifest: &str, source: &str) {
    let package_root = root.join("crates").join(package);
    fs::create_dir_all(package_root.join("src")).expect("create trusted fixture package");
    fs::write(package_root.join("Cargo.toml"), manifest).expect("write trusted fixture manifest");
    fs::write(package_root.join("src/lib.rs"), source).expect("write trusted fixture source");
}

impl Drop for TestRepository {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

#[test]
fn source_observation_rejects_a_mutation_before_its_final_cleanliness_check() {
    let repository = TestRepository::new();
    let error = observe_checkout_with(
        &repository.root,
        &MutateBeforeFinalStatus {
            status_calls: AtomicUsize::new(0),
        },
        &NoGeneratedOutputs,
    )
    .expect_err("late source mutation must fail closed");

    assert!(
        error
            .to_string()
            .contains("clean tracked and untracked worktree"),
        "unexpected source-observation error: {error}"
    );
}

fn command(root: &Path, program: &str, arguments: &[&str]) {
    let output = Command::new(program)
        .args(arguments)
        .current_dir(root)
        .output()
        .unwrap_or_else(|error| panic!("run fixture command {program}: {error}"));
    assert!(
        output.status.success(),
        "fixture command {program} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
