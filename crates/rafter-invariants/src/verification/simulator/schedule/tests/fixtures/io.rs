//! Filesystem, process-log, and executable helpers for fixture construction.

use std::{collections::BTreeMap, env, fs, path::Path, process::Command};

use sha2::{Digest, Sha256};

pub(super) fn copy_plan_input(workspace: &Path, root: &Path, path: &str) -> crate::PlanInput {
    let bytes = fs::read(workspace.join(path)).expect("read plan input");
    let destination = root.join(path);
    fs::create_dir_all(destination.parent().expect("plan input parent"))
        .expect("create plan input parent");
    fs::write(destination, &bytes).expect("write plan input");
    crate::PlanInput {
        path: path.to_owned(),
        sha256: format!("{:x}", Sha256::digest(&bytes)),
        size_bytes: bytes.len() as u64,
    }
}

pub(super) fn executable_sha256(name: &str) -> String {
    let path = env::split_paths(&env::var_os("PATH").expect("PATH is configured"))
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| panic!("{name} is present on PATH"));
    format!(
        "{:x}",
        Sha256::digest(fs::read(path).expect("read executable"))
    )
}

pub(super) fn source_bound_launchers(
    process_runtime: &BTreeMap<String, crate::ExecutableReceipt>,
) -> Vec<crate::LauncherReceipt> {
    crate::receipt::fixture_launchers(false)
        .into_iter()
        .map(|mut launcher| {
            launcher.executable = process_runtime
                .get(&launcher.runtime)
                .unwrap_or_else(|| panic!("missing fixture runtime {}", launcher.runtime))
                .clone();
            launcher
        })
        .collect()
}

pub(super) fn git(root: &Path, arguments: &[&str]) {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .output()
        .expect("run git fixture command");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(super) fn framed_process_log(
    label: &str,
    invocation: &crate::InvocationReceipt,
    timed_out: bool,
    stdout: &str,
    stderr: &str,
) -> String {
    format!(
        concat!(
            "schema_version: 4\n",
            "label: {label}\n",
            "invocation: {invocation}\n",
            "exit_code: Some(0)\n",
            "timed_out: {timed_out}\n",
            "duration_ms: 1\n",
            "peak_rss_kib: 1\n",
            "stdout_bytes: {stdout_bytes}\n",
            "stderr_bytes: {stderr_bytes}\n\n",
            "{stdout}{stderr}",
        ),
        label = label,
        invocation = serde_json::to_string(invocation).expect("serialize invocation"),
        timed_out = timed_out,
        stdout_bytes = stdout.len(),
        stderr_bytes = stderr.len(),
        stdout = stdout,
        stderr = stderr,
    )
}

pub(super) fn write_fixture_artifact(
    root: &Path,
    path: &str,
    kind: &str,
    bytes: &[u8],
) -> crate::ArtifactRef {
    let destination = root.join(path);
    fs::create_dir_all(destination.parent().expect("artifact parent"))
        .expect("create artifact parent");
    fs::write(destination, bytes).expect("write simulator fixture artifact");
    crate::ArtifactRef {
        kind: kind.to_owned(),
        path: path.to_owned(),
        sha256: format!("{:x}", Sha256::digest(bytes)),
        size_bytes: bytes.len() as u64,
    }
}
