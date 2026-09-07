//! Unix shell fixtures for exact CI test inventory checks.

use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

#[cfg(unix)]
pub(super) fn run_fixture(
    root: &Path,
    bin: &Path,
    log: &Path,
    expected: &str,
) -> std::process::Output {
    let system_path = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![bin.to_path_buf()];
    paths.extend(std::env::split_paths(&system_path));
    Command::new("bash")
        .arg(root.join("scripts/cargo-test-exact"))
        .args([expected, "family", "-p", "demo", "--", "--ignored"])
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("CARGO_TEST_EXACT_LOG", log)
        .output()
        .unwrap()
}

#[cfg(unix)]
pub(super) fn run_fixture_without_libtest_args(
    root: &Path,
    bin: &Path,
    log: &Path,
    expected: &str,
) -> std::process::Output {
    let system_path = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![bin.to_path_buf()];
    paths.extend(std::env::split_paths(&system_path));
    Command::new("bash")
        .arg(root.join("scripts/cargo-test-exact"))
        .args([expected, "family", "-p", "demo"])
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("CARGO_TEST_EXACT_LOG", log)
        .output()
        .unwrap()
}

#[cfg(unix)]
pub(super) fn run_fixture_with_inventory(
    root: &Path,
    bin: &Path,
    log: &Path,
    inventory: &Path,
) -> std::process::Output {
    let system_path = std::env::var_os("PATH").unwrap_or_default();
    let mut paths = vec![bin.to_path_buf()];
    paths.extend(std::env::split_paths(&system_path));
    Command::new("bash")
        .arg(root.join("scripts/cargo-test-exact"))
        .args(["2", "family", "--inventory"])
        .arg(inventory)
        .args(["-p", "demo"])
        .env("PATH", std::env::join_paths(paths).unwrap())
        .env("CARGO_TEST_EXACT_LOG", log)
        .output()
        .unwrap()
}

#[cfg(unix)]
pub(super) fn stderr(output: &std::process::Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[cfg(unix)]
static NEXT_FIXTURE_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(unix)]
pub(super) struct Fixture {
    pub(super) path: PathBuf,
}

#[cfg(unix)]
impl Fixture {
    pub(super) fn new() -> Self {
        let id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rafter-cargo-test-exact-{}-{id}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        Self { path }
    }
}

#[cfg(unix)]
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}
