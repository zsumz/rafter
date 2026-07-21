//! Adversarial serialized-intake scenarios over a real clean fixture checkout.

use std::{fs, path::PathBuf, process::Command};

use super::fixtures::{materialize_cross_root_fixture, RuntimeDefect, SimulatorFixture};
use crate::verification::{EvidenceIntake, IntakeDefectKind, VerificationRequest};

#[test]
fn repeated_result_path_is_rejected_as_ambiguous_evidence() {
    let fixture = materialize_cross_root_fixture(RuntimeDefect::ProvenanceOnly);
    let intake = verify_paths(
        &fixture,
        &[fixture.bundle_path.clone(), fixture.bundle_path.clone()],
    );

    assert!(intake.accepted().is_empty());
    assert!(
        intake.defects().iter().any(|defect| {
            defect.kind() == IntakeDefectKind::Unverifiable
                && defect.message().contains("duplicate evidence result path")
        }),
        "{:?}",
        intake.defects()
    );
    assert_all_red(&fixture, &intake);
}

#[test]
fn deleted_and_mutated_referenced_artifacts_are_unverifiable() {
    for mutation in [ArtifactMutation::Delete, ArtifactMutation::Replace] {
        let fixture = materialize_cross_root_fixture(RuntimeDefect::ProvenanceOnly);
        let artifact = referenced_artifact(&fixture);
        match mutation {
            ArtifactMutation::Delete => {
                fs::remove_file(&artifact).expect("delete fixture artifact");
            }
            ArtifactMutation::Replace => {
                fs::write(&artifact, b"adversarial replacement\n")
                    .expect("replace fixture artifact");
            }
        }

        let intake = verify_paths(&fixture, std::slice::from_ref(&fixture.bundle_path));
        assert!(intake.accepted().is_empty());
        assert!(
            intake.defects().iter().any(|defect| {
                defect.kind() == IntakeDefectKind::Unverifiable
                    && defect.message().contains("artifact")
            }),
            "{:?}",
            intake.defects()
        );
        assert_all_red(&fixture, &intake);
    }
}

#[test]
fn dirty_checkout_is_unverifiable_and_new_clean_commit_is_stale() {
    let dirty = materialize_cross_root_fixture(RuntimeDefect::ProvenanceOnly);
    fs::write(dirty.root.join("uncommitted-source-change"), b"dirty\n")
        .expect("write dirty checkout marker");
    let dirty_intake = verify_paths(&dirty, std::slice::from_ref(&dirty.bundle_path));
    assert!(
        dirty_intake.defects().iter().any(|defect| {
            defect.kind() == IntakeDefectKind::Unverifiable
                && defect
                    .message()
                    .contains("clean tracked and untracked worktree")
        }),
        "{:?}",
        dirty_intake.defects()
    );
    assert_all_red(&dirty, &dirty_intake);
    drop(dirty);

    let stale = materialize_cross_root_fixture(RuntimeDefect::ProvenanceOnly);
    fs::write(stale.root.join("committed-source-change"), b"stale\n")
        .expect("write committed checkout marker");
    git(&stale.root, &["add", "committed-source-change"]);
    git(
        &stale.root,
        &[
            "-c",
            "user.name=Rafter Fixture",
            "-c",
            "user.email=fixture@example.invalid",
            "-c",
            "commit.gpgsign=false",
            "commit",
            "-q",
            "-m",
            "test: advance verifier checkout",
        ],
    );
    let stale_intake = verify_paths(&stale, std::slice::from_ref(&stale.bundle_path));
    assert!(stale_intake.defects().iter().any(|defect| {
        defect.kind() == IntakeDefectKind::Stale
            && defect
                .message()
                .contains("evidence source identity does not match")
    }));
    assert_all_red(&stale, &stale_intake);
}

#[derive(Clone, Copy)]
enum ArtifactMutation {
    Delete,
    Replace,
}

fn verify_paths(fixture: &SimulatorFixture, paths: &[PathBuf]) -> EvidenceIntake {
    let bundle = fixture.serialized_bundle();
    let request = VerificationRequest::new(
        &fixture.catalog,
        &fixture.manifest,
        &bundle.execution.plan,
        &bundle.source_ref,
        &fixture.root,
    );
    crate::verification::verify_paths(request, paths, Vec::new())
        .expect("adversarial serialized evidence has a typed intake")
}

fn referenced_artifact(fixture: &SimulatorFixture) -> PathBuf {
    let bundle = fixture.serialized_bundle();
    bundle
        .execution
        .checks
        .iter()
        .flat_map(|check| &check.artifacts)
        .map(|artifact| fixture.root.join(&artifact.path))
        .find(|path| path.is_file())
        .expect("serialized fixture references a materialized artifact")
}

fn assert_all_red(fixture: &SimulatorFixture, intake: &EvidenceIntake) {
    let report = crate::verdict::reduce(&fixture.catalog, &fixture.manifest, intake)
        .expect("defective intake reduces deterministically");
    assert_eq!(report.summary.green, 0);
    assert_eq!(report.summary.red, 44);
}

fn git(root: &std::path::Path, arguments: &[&str]) {
    let output = Command::new("git")
        .args(arguments)
        .current_dir(root)
        .output()
        .expect("run fixture Git command");
    assert!(
        output.status.success(),
        "git {arguments:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}
