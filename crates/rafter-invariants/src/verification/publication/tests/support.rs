//! Canonical report, directory, process-log, and tar fixtures for publication tests.

use std::{fmt::Write as _, fs, path::Path};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use sha2::{Digest, Sha256};

use super::VerifierArchiveExpectation;

mod completed;

pub(super) use completed::{completed_report, CompletedReport};

pub(super) struct Fixture {
    pub(super) temp: tempfile::TempDir,
    pub(super) root: std::path::PathBuf,
    pub(super) manifest: std::path::PathBuf,
    pub(super) manifest_sha256: String,
    pub(super) payload: std::path::PathBuf,
    pub(super) expectation: VerifierArchiveExpectation,
}

#[cfg(unix)]
pub(super) fn fixture() -> Fixture {
    let (report, expectation) = synthetic_report();
    fixture_from_report("run-1", &report, expectation, Vec::new())
}

#[cfg(unix)]
pub(super) fn fixture_from_report(
    label: &str,
    report: &[u8],
    expectation: VerifierArchiveExpectation,
    extra_artifacts: Vec<(String, Vec<u8>)>,
) -> Fixture {
    let temp = tempfile::tempdir().expect("temporary publication root");
    let root = temp.path().join(label);
    fs::create_dir(&root).expect("create verifier root");
    let report_sha256 = sha256(report);
    let report_name = format!("verifier-replay-report-{report_sha256}");
    let payload = root.join(&report_name);
    fs::write(&payload, report).expect("write verifier report");
    let mut entries = vec![(report_name, report_sha256)];
    for (name, bytes) in extra_artifacts {
        fs::write(root.join(&name), &bytes).expect("write extra verifier artifact");
        make_read_only(&root.join(&name));
        entries.push((name, sha256(&bytes)));
    }
    entries.sort();
    let mut manifest_bytes = String::new();
    for (name, digest) in &entries {
        writeln!(&mut manifest_bytes, "{digest}  {name}").expect("write manifest fixture");
    }
    let manifest = root.join(format!(
        "verifier-artifact-manifest-{}",
        sha256(manifest_bytes.as_bytes())
    ));
    fs::write(&manifest, manifest_bytes.as_bytes()).expect("write verifier manifest");
    make_read_only(&payload);
    make_read_only(&manifest);
    make_read_only(&root);
    Fixture {
        temp,
        root,
        manifest,
        manifest_sha256: sha256(manifest_bytes.as_bytes()),
        payload,
        expectation,
    }
}

#[cfg(unix)]
pub(super) fn semantic_failure_fixture() -> Fixture {
    let temp = tempfile::tempdir().expect("temporary publication root");
    let root = temp.path().join("run-invalid");
    fs::create_dir(&root).expect("create verifier root");
    let payload = root.join(format!("verifier-replay-report-{}", sha256(b"{}\n")));
    fs::write(&payload, b"{}\n").expect("write invalid replay report");
    let manifest_bytes = format!(
        "{}  {}\n",
        sha256(b"{}\n"),
        payload.file_name().unwrap().to_str().unwrap()
    );
    let manifest = root.join(format!(
        "verifier-artifact-manifest-{}",
        sha256(manifest_bytes.as_bytes())
    ));
    fs::write(&manifest, manifest_bytes.as_bytes()).expect("write verifier manifest");
    make_read_only(&payload);
    make_read_only(&manifest);
    make_read_only(&root);
    Fixture {
        temp,
        root,
        manifest,
        manifest_sha256: sha256(manifest_bytes.as_bytes()),
        payload,
        expectation: synthetic_report().1,
    }
}

pub(super) fn synthetic_report() -> (Vec<u8>, VerifierArchiveExpectation) {
    use crate::verification::{
        detector_replay::ReplayReportExpectation,
        source::{
            AuthenticatedSourceReceipt, ReplaySourceReceipts, ReplayToolchainProgramReceipt,
            ReplayToolchainReceipt, SourceMaterializationReceipt,
        },
    };

    let manifest = crate::ProfileManifest::load(
        &std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../verification/raft-invariant-profiles.json"),
    )
    .expect("load profile manifest");
    let contract = manifest.verifiers["pr"].detector_replay.clone();
    let program = |name: &str| ReplayToolchainProgramReceipt {
        identity: format!("{name} 1.88.0"),
        launcher_sha256: "1".repeat(64),
        executable_path: format!("/toolchain/bin/{name}"),
        executable_sha256: "2".repeat(64),
    };
    let source = AuthenticatedSourceReceipt {
        commit: "a".repeat(40),
        tree: "3".repeat(40),
        materialization: SourceMaterializationReceipt {
            contract: "git-tree-materialization-v1".to_owned(),
            sha256: "4".repeat(64),
            tracked_entries: 1,
            submodules: 0,
        },
        cargo_lock_sha256: "5".repeat(64),
        cargo_config_sha256: "6".repeat(64),
        environment_sha256: "7".repeat(64),
        target: "x86_64-unknown-linux-gnu".to_owned(),
    };
    let receipts = ReplaySourceReceipts::from_parts(
        source,
        ReplayToolchainReceipt {
            cargo: program("cargo"),
            rustc: program("rustc"),
        },
    )
    .expect("create synthetic receipts");
    let registry = crate::verification::source::RegistryReceipt {
        lock_sha256: receipts.source.cargo_lock_sha256.clone(),
        package_count: contract.required_registry_packages,
        archive_bytes: 1,
        expanded_bytes: 1,
        entries: 1,
        materialization_sha256: "8".repeat(64),
    };
    let report = serde_json::json!({
        "schema_version": 4,
        "profile": "pr",
        "source_ref": receipts.source.commit,
        "source": receipts.source,
        "source_sha256": receipts.source_sha256,
        "toolchain": receipts.toolchain,
        "toolchain_sha256": receipts.toolchain_sha256,
        "contract": contract,
        "registry": registry,
        "inventory": {"fixtures": 0, "targets": 0, "evidence_bindings": 1},
        "compilation": {
            "status": "harness_error",
            "message": "synthetic preparation failure",
            "targets": [],
            "processes": []
        },
        "fixtures": []
    });
    let bytes = crate::verification::detector_replay::canonical_report_value(report.clone())
        .expect("render synthetic report");
    let receipts = ReplaySourceReceipts::from_parts(
        serde_json::from_value(report["source"].clone()).expect("decode source receipt"),
        serde_json::from_value(report["toolchain"].clone()).expect("decode toolchain receipt"),
    )
    .expect("rebuild synthetic receipts");
    let contract = serde_json::from_value(report["contract"].clone()).expect("decode contract");
    let expectation = VerifierArchiveExpectation::from_replay(ReplayReportExpectation::new(
        "pr".to_owned(),
        receipts,
        contract,
        Some(serde_json::from_value(report["registry"].clone()).expect("decode registry receipt")),
    ));
    (bytes, expectation)
}

pub(super) fn process_log(role: &str, execution_id: &str, stream: &str, payload: &[u8]) -> Vec<u8> {
    let mut bytes = format!(
        "rafter-verifier-process-log-v2\nrole:{role}\nexecution-id:{execution_id}\nstream:{stream}\npayload-bytes:{}\n\n",
        payload.len()
    )
    .into_bytes();
    bytes.extend_from_slice(payload);
    bytes
}

pub(super) fn artifact_ref(bytes: &[u8]) -> serde_json::Value {
    let digest = sha256(bytes);
    serde_json::json!({
        "kind": "verifier-replay-process-log",
        "path": format!("target/verifier/verifier-replay-process-log-{digest}"),
        "sha256": digest,
        "size_bytes": bytes.len()
    })
}

pub(super) fn append_entry(builder: &mut tar::Builder<&mut fs::File>, name: &str, bytes: &[u8]) {
    append_with_mode(builder, name, bytes, 0o644);
}

pub(super) fn append_canonical_entry(
    builder: &mut tar::Builder<&mut fs::File>,
    name: &str,
    bytes: &[u8],
) {
    append_with_mode(builder, name, bytes, 0o444);
}

fn append_with_mode(
    builder: &mut tar::Builder<&mut fs::File>,
    name: &str,
    bytes: &[u8],
    mode: u32,
) {
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Regular);
    header.set_mode(mode);
    header.set_uid(0);
    header.set_gid(0);
    header.set_mtime(0);
    header.set_size(u64::try_from(bytes.len()).expect("fixture size"));
    header.set_cksum();
    builder
        .append_data(&mut header, name, bytes)
        .expect("append archive entry");
}

pub(super) fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(unix)]
pub(super) fn make_read_only(path: &Path) {
    let mode = if fs::metadata(path).expect("path metadata").is_dir() {
        0o555
    } else {
        0o444
    };
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).expect("make path read-only");
}

#[cfg(unix)]
pub(super) fn make_writable(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o700)).expect("make path writable");
}
