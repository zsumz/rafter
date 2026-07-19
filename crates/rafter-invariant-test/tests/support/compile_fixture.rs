//! Isolated Cargo fixture for compiler-facing integration contracts.

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_SCRATCH: AtomicU64 = AtomicU64::new(0);

pub(crate) struct CargoFixture(PathBuf);

impl CargoFixture {
    pub(crate) fn new(label: &str, dependencies: &str) -> Self {
        let sequence = NEXT_SCRATCH.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rafter-invariant-test-{label}-{}-{sequence}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(path.join("src")).unwrap();
        fs::write(
            path.join("Cargo.toml"),
            format!(
                "[package]\nname = \"{label}\"\nversion = \"0.0.0\"\nedition = \"2021\"\n\n[dependencies]\n{dependencies}\n"
            ),
        )
        .unwrap();
        Self(path)
    }

    pub(crate) fn write_source(&self, source: &str) {
        fs::write(self.0.join("src/main.rs"), source).unwrap();
    }

    pub(crate) fn compile(&self) -> Output {
        Command::new("cargo")
            .args(["test", "--no-run", "--offline", "--quiet"])
            .env("CARGO_TARGET_DIR", self.0.join("target"))
            .current_dir(&self.0)
            .output()
            .expect("compile detector contract fixture")
    }
}

impl Drop for CargoFixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[allow(clippy::unnecessary_debug_formatting)]
pub(crate) fn runtime_dependency(alias: &str) -> String {
    let runtime = Path::new(env!("CARGO_MANIFEST_DIR"));
    format!("{alias} = {{ package = \"rafter-invariant-test\", path = {runtime:?} }}")
}
