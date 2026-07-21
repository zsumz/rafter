//! Authenticated fixture result publication and evidence qualification.

use std::{collections::BTreeMap, error::Error};

use crate::evidence::ArtifactRef;

use super::{
    model::{FixtureReport, FixtureSourceReport, ReplayEvidenceReport, TargetReport},
    process,
    publisher::ReplayArtifactPublisher,
};
use crate::verification::detector_replay::{
    assessment::EvidenceReplayQualification,
    model::ReplayEvidence,
    result::{FixtureReplayResult, FixtureReplayStatus},
    DetectorReplayPlan,
};

pub(super) struct PublishedFixtures {
    pub(super) qualifications: BTreeMap<String, EvidenceReplayQualification>,
    pub(super) reports: Vec<FixtureReport>,
}

pub(super) fn publish(
    publisher: &ReplayArtifactPublisher,
    replay: &DetectorReplayPlan,
    replayed: Vec<FixtureReplayResult>,
    artifacts: &mut Vec<ArtifactRef>,
) -> Result<PublishedFixtures, Box<dyn Error>> {
    if replayed.len() != replay.fixture_count() {
        return Err(format!(
            "detector replay produced {} fixture results for {} authenticated fixtures",
            replayed.len(),
            replay.fixture_count()
        )
        .into());
    }
    let mut qualifications = BTreeMap::new();
    let mut reports = Vec::with_capacity(replayed.len());
    let expected = replay
        .targets()
        .iter()
        .flat_map(|(target, fixtures)| fixtures.iter().map(move |fixture| (target, fixture)));
    for ((expected_target, expected_fixture), fixture) in expected.zip(replayed) {
        if fixture.target != *expected_target
            || fixture.test_name != expected_fixture.identity.test_name
            || fixture.evidence != expected_fixture.evidence
        {
            return Err(
                "detector replay result does not match its authenticated fixture binding".into(),
            );
        }
        let target = TargetReport::from(&fixture.target);
        let execution_id = process::fixture_execution_id(&target, &fixture.test_name);
        let process = fixture
            .output
            .as_ref()
            .map(|output| process::report(publisher, "detector-fixture", &execution_id, output))
            .transpose()?;
        let mut process = process;
        if let Some(diagnostics) = &fixture.retained_diagnostics {
            process = Some(process::lifecycle_error(
                publisher,
                "detector-fixture",
                &execution_id,
                fixture
                    .message
                    .as_deref()
                    .unwrap_or("detector fixture process lifecycle failed"),
                diagnostics,
            )?);
        }
        let fixture_artifacts = process
            .as_ref()
            .map(|process| process.logs().to_vec())
            .unwrap_or_default();
        artifacts.extend(fixture_artifacts.iter().cloned());
        for evidence in &fixture.evidence {
            let previous = qualifications.insert(
                evidence.evidence_id.clone(),
                qualification(&fixture, evidence, &fixture_artifacts),
            );
            if previous.is_some() {
                return Err(format!(
                    "detector replay produced duplicate qualification for {}",
                    evidence.evidence_id
                )
                .into());
            }
        }
        reports.push(FixtureReport {
            target,
            test_name: fixture.test_name,
            source: FixtureSourceReport::try_from(expected_fixture)?,
            evidence: fixture
                .evidence
                .iter()
                .map(ReplayEvidenceReport::from)
                .collect(),
            status: fixture.status,
            token: fixture.token,
            challenge: fixture.challenge,
            message: fixture.message,
            process,
        });
    }
    Ok(PublishedFixtures {
        qualifications,
        reports,
    })
}

fn qualification(
    fixture: &FixtureReplayResult,
    evidence: &ReplayEvidence,
    artifacts: &[ArtifactRef],
) -> EvidenceReplayQualification {
    if fixture.status == FixtureReplayStatus::Passed {
        EvidenceReplayQualification::passed(evidence.invariant_id.clone(), artifacts.to_vec())
    } else {
        EvidenceReplayQualification::failed(
            evidence.invariant_id.clone(),
            fixture
                .message
                .as_deref()
                .unwrap_or("detector fixture replay did not pass"),
            artifacts.to_vec(),
        )
    }
}
