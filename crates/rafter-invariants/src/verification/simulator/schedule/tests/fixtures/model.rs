//! Shared types and lifecycle for serialized simulator fixtures.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::MutexGuard,
};

#[derive(Clone, Copy)]
pub(in super::super) enum RuntimeDefect {
    ProvenanceOnly,
    Timeout,
    MalformedEvent,
    LaunchFailure,
    PassExitOne,
    CounterexampleExitOne,
}

#[derive(Clone, Copy, Debug)]
pub(in super::super) enum ProvenanceSubstitution {
    Package,
    Source,
    TargetName,
    TargetKind,
    Executable,
    CompileRoot,
}

pub(in super::super) struct SimulatorFixture {
    pub(in super::super) root: PathBuf,
    pub(in super::super) producer_root: PathBuf,
    pub(in super::super) bundle_path: PathBuf,
    pub(super) timeout_output_dir: PathBuf,
    pub(in super::super) catalog: crate::Catalog,
    pub(in super::super) manifest: crate::ProfileManifest,
    pub(super) _serial: MutexGuard<'static, ()>,
}

pub(super) struct PendingSimulatorFixture {
    pub(super) root: PathBuf,
    pub(super) producer_root: PathBuf,
    pub(super) bundle_path: PathBuf,
    pub(super) timeout_output_dir: PathBuf,
    pub(super) armed: bool,
}

impl PendingSimulatorFixture {
    pub(super) fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PendingSimulatorFixture {
    fn drop(&mut self) {
        if self.armed {
            cleanup_fixture_artifacts(
                &self.root,
                &self.producer_root,
                &self.bundle_path,
                &self.timeout_output_dir,
            );
        }
    }
}

impl Drop for SimulatorFixture {
    fn drop(&mut self) {
        cleanup_fixture_artifacts(
            &self.root,
            &self.producer_root,
            &self.bundle_path,
            &self.timeout_output_dir,
        );
    }
}

impl SimulatorFixture {
    pub(in super::super) fn serialized_bundle(&self) -> crate::ResultBundle {
        serde_json::from_slice(
            &fs::read(&self.bundle_path).expect("read serialized simulator bundle"),
        )
        .expect("decode serialized simulator bundle")
    }
}

pub(super) fn cleanup_fixture_artifacts(
    root: &Path,
    producer_root: &Path,
    bundle_path: &Path,
    timeout_output_dir: &Path,
) {
    let _ = fs::remove_dir_all(timeout_output_dir);
    let _ = fs::remove_file(bundle_path);
    let _ = fs::remove_dir_all(root);
    let _ = fs::remove_dir_all(producer_root);
}

pub(super) struct CompileFixture {
    pub(super) binary_path: PathBuf,
    pub(super) binary_artifact: crate::ArtifactRef,
    pub(super) compile_artifact: crate::ArtifactRef,
}

pub(super) struct RuntimeFixture {
    pub(super) fast_artifact: crate::ArtifactRef,
    pub(super) producer_artifact: crate::ArtifactRef,
    pub(super) duration_ms: u64,
    pub(super) peak_rss_kib: u64,
    pub(super) checks: Vec<crate::CheckReceipt>,
    pub(super) results: Vec<crate::EvidenceResult>,
}

pub(super) struct RuntimeFixtureInput<'a> {
    pub(super) root: &'a Path,
    pub(super) output_dir: &'a Path,
    pub(super) source_ref: &'a str,
    pub(super) current_dir: &'a Path,
    pub(super) environment: &'a BTreeMap<String, String>,
    pub(super) process_runtime: &'a BTreeMap<String, crate::ExecutableReceipt>,
    pub(super) compile: &'a CompileFixture,
}
