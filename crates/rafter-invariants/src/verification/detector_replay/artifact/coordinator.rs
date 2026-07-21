//! Replay assessment and report publication coordination.

use std::{error::Error, time::Instant};

use crate::{
    contract::profile::DetectorReplayContract,
    verification::source::{AuthenticatedCompilationSource, ReplaySourceReceipts},
};

use super::super::{
    assessment::DetectorReplayAssessment,
    result::{DetectorReplayAttempt, DetectorReplayRun},
    DetectorReplayPlan, PreparationFailureRequest,
};
use super::{
    fixtures::{self, PublishedFixtures},
    model::{
        CompilationReport, CompilationStatus, ReplayInventory, ReplayReport, TargetReport,
        REPORT_SCHEMA_VERSION,
    },
    process,
    publisher::ReplayArtifactPublisher,
    report,
};

pub(in crate::verification::detector_replay) fn publish_attempt(
    replay: &DetectorReplayPlan,
    attempt: DetectorReplayAttempt,
    source: &AuthenticatedCompilationSource<'_>,
    contract: &DetectorReplayContract,
    profile: &str,
    source_ref: &str,
    publication_deadline: Instant,
) -> Result<DetectorReplayAssessment, Box<dyn Error>> {
    let publisher = ReplayArtifactPublisher::create(profile, source_ref, publication_deadline)?;
    let receipts = source.replay_receipts()?;
    let registry = Some(source.registry_receipt());
    match attempt {
        DetectorReplayAttempt::Completed(run) => publish_completed(
            ReplayReportContext {
                publisher,
                replay,
                registry,
                receipts,
                contract,
                profile,
                source_ref,
            },
            *run,
        ),
        DetectorReplayAttempt::CompilationFailed(failure) => {
            let mut artifacts = Vec::new();
            let processes = process::reports(
                &publisher,
                [
                    ("cargo-metadata", failure.metadata_output.as_ref()),
                    ("cargo-test-no-run", failure.compiler_output.as_ref()),
                ],
                &mut artifacts,
            )?;
            let mut processes = processes;
            if let Some(diagnostics) = &failure.retained_diagnostics {
                let failure_report = process::lifecycle_error(
                    &publisher,
                    "cargo-process-lifecycle",
                    "cargo-process-lifecycle",
                    &failure.message,
                    diagnostics,
                )?;
                artifacts.extend(failure_report.logs().iter().cloned());
                processes.push(failure_report);
            }
            let report = ReplayReport {
                schema_version: REPORT_SCHEMA_VERSION,
                profile: profile.to_owned(),
                source_ref: source_ref.to_owned(),
                source: receipts.source,
                source_sha256: receipts.source_sha256,
                toolchain: receipts.toolchain,
                toolchain_sha256: receipts.toolchain_sha256,
                contract: contract.clone(),
                registry,
                inventory: report::inventory(replay)?,
                compilation: CompilationReport {
                    status: CompilationStatus::HarnessError,
                    message: Some(failure.message.clone()),
                    metadata_sha256: None,
                    targets: Vec::new(),
                    processes,
                },
                fixtures: Vec::new(),
            };
            let report_artifact = report::publish(&publisher, &report)?;
            artifacts.push(report_artifact);
            artifacts.push(publisher.publish_manifest()?);
            let assessment = DetectorReplayAssessment::harness_error(
                replay
                    .targets()
                    .values()
                    .flatten()
                    .flat_map(|fixture| fixture.evidence.iter().cloned()),
                &failure.message,
                artifacts,
            )
            .map_err(Box::<dyn Error>::from)?;
            assessment
                .with_artifact_guard(publisher.seal()?)
                .map_err(Into::into)
        }
    }
}

pub(in crate::verification::detector_replay) fn publish_preparation_failure(
    request: PreparationFailureRequest<'_>,
) -> Result<DetectorReplayAssessment, Box<dyn Error>> {
    let PreparationFailureRequest {
        inventory,
        replay,
        receipts,
        contract,
        profile,
        source_ref,
        registry,
        message,
        deadlines,
    } = request;
    let publication_deadline = deadlines.publication();
    let publisher = ReplayArtifactPublisher::create(profile, source_ref, publication_deadline)?;
    let replay_inventory = match replay {
        Some(replay) => report::inventory(replay)?,
        None => ReplayInventory {
            fixtures: 0,
            targets: 0,
            evidence_bindings: inventory.len(),
            sha256: None,
        },
    };
    let report = ReplayReport {
        schema_version: REPORT_SCHEMA_VERSION,
        profile: profile.to_owned(),
        source_ref: source_ref.to_owned(),
        source: receipts.source,
        source_sha256: receipts.source_sha256,
        toolchain: receipts.toolchain,
        toolchain_sha256: receipts.toolchain_sha256,
        contract: contract.clone(),
        registry,
        inventory: replay_inventory,
        compilation: CompilationReport {
            status: CompilationStatus::HarnessError,
            message: Some(message.to_owned()),
            metadata_sha256: None,
            targets: Vec::new(),
            processes: Vec::new(),
        },
        fixtures: Vec::new(),
    };
    let report_artifact = report::publish(&publisher, &report)?;
    let manifest_artifact = publisher.publish_manifest()?;
    let assessment = DetectorReplayAssessment::harness_error(
        inventory,
        message,
        vec![report_artifact, manifest_artifact],
    )
    .map_err(Box::<dyn Error>::from)?;
    assessment
        .with_artifact_guard(publisher.seal()?)
        .map_err(Into::into)
}

struct ReplayReportContext<'a> {
    publisher: ReplayArtifactPublisher,
    replay: &'a DetectorReplayPlan,
    registry: Option<crate::verification::source::RegistryReceipt>,
    receipts: ReplaySourceReceipts,
    contract: &'a DetectorReplayContract,
    profile: &'a str,
    source_ref: &'a str,
}

fn publish_completed(
    context: ReplayReportContext<'_>,
    run: DetectorReplayRun,
) -> Result<DetectorReplayAssessment, Box<dyn Error>> {
    let ReplayReportContext {
        publisher,
        replay,
        registry,
        receipts,
        contract,
        profile,
        source_ref,
    } = context;
    let mut artifacts = Vec::new();
    let processes = process::reports(
        &publisher,
        [
            ("cargo-metadata", Some(&run.compilation.metadata_output)),
            ("cargo-test-no-run", Some(&run.compilation.compiler_output)),
        ],
        &mut artifacts,
    )?;
    let PublishedFixtures {
        mut qualifications,
        reports: fixtures,
    } = fixtures::publish(&publisher, replay, run.fixtures, &mut artifacts)?;
    let report = ReplayReport {
        schema_version: REPORT_SCHEMA_VERSION,
        profile: profile.to_owned(),
        source_ref: source_ref.to_owned(),
        source: receipts.source,
        source_sha256: receipts.source_sha256,
        toolchain: receipts.toolchain,
        toolchain_sha256: receipts.toolchain_sha256,
        contract: contract.clone(),
        registry,
        inventory: report::inventory(replay)?,
        compilation: CompilationReport {
            status: CompilationStatus::Passed,
            message: None,
            metadata_sha256: Some(run.compilation.metadata_sha256),
            targets: run
                .compilation
                .targets
                .keys()
                .map(TargetReport::from)
                .collect(),
            processes,
        },
        fixtures,
    };
    let report_artifact = report::publish(&publisher, &report)?;
    artifacts.push(report_artifact.clone());
    for qualification in qualifications.values_mut() {
        qualification.attach_artifact(report_artifact.clone());
    }
    let manifest_artifact = publisher.publish_manifest()?;
    artifacts.push(manifest_artifact.clone());
    for qualification in qualifications.values_mut() {
        qualification.attach_artifact(manifest_artifact.clone());
    }
    let assessment =
        DetectorReplayAssessment::new(qualifications, artifacts).map_err(Box::<dyn Error>::from)?;
    assessment
        .with_artifact_guard(publisher.seal()?)
        .map_err(Into::into)
}
