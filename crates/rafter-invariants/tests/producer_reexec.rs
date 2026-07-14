#![cfg(unix)]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

const PRODUCER_DIGEST_ENV: &str = "RAFTER_INVARIANT_PRODUCER_SHA256";

#[test]
fn producer_bootstrap_reexecs_and_rejects_forged_bindings() {
    let root = workspace_root();
    let scratch = scratch();
    let bootstrap = scratch.join("rafter-invariants-bootstrap");
    copy_executable(
        Path::new(env!("CARGO_BIN_EXE_rafter-invariants")),
        &bootstrap,
    );

    let first = probe(&bootstrap, &root, None);
    assert!(
        first.status.success(),
        "bootstrap failed: {}",
        String::from_utf8_lossy(&first.stderr)
    );
    let managed = PathBuf::from(
        String::from_utf8(first.stdout)
            .expect("probe output is UTF-8")
            .trim(),
    );
    let image_root = root.join("target/rafter-invariants/producer-images");
    assert!(managed.starts_with(&image_root));
    let digest = managed
        .parent()
        .and_then(Path::file_name)
        .and_then(|value| value.to_str())
        .expect("managed image path contains digest")
        .to_owned();
    assert_eq!(digest.len(), 64);

    fs::remove_file(&bootstrap).expect("remove mutable bootstrap");
    let retained = probe(&managed, &root, Some(&digest));
    assert!(
        retained.status.success(),
        "retained image failed: {}",
        String::from_utf8_lossy(&retained.stderr)
    );

    copy_executable(&managed, &bootstrap);
    let forged_digest = probe(&bootstrap, &root, Some(&"f".repeat(64)));
    assert!(!forged_digest.status.success());
    let wrong_path = probe(&bootstrap, &root, Some(&digest));
    assert!(!wrong_path.status.success());

    fs::remove_dir_all(scratch).expect("remove producer re-exec scratch directory");
}

fn probe(program: &Path, root: &Path, digest: Option<&str>) -> std::process::Output {
    let mut command = Command::new(program);
    command
        .arg("producer-probe")
        .current_dir(root)
        .env_remove(PRODUCER_DIGEST_ENV);
    if let Some(digest) = digest {
        command.env(PRODUCER_DIGEST_ENV, digest);
    }
    command.output().expect("run producer probe")
}

fn copy_executable(source: &Path, destination: &Path) {
    fs::copy(source, destination).expect("copy producer bootstrap");
    fs::set_permissions(destination, fs::Permissions::from_mode(0o700))
        .expect("make producer bootstrap executable");
}

fn workspace_root() -> PathBuf {
    fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .expect("canonical workspace root")
}

fn scratch() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "rafter-producer-reexec-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    fs::create_dir_all(&path).expect("create producer re-exec scratch directory");
    path
}
