//! Reconstruction of the logical replay inventory from published fixture rows.

use std::{collections::BTreeMap, path::PathBuf};

use crate::contract::TestIdentity;

use super::super::model::{ReplayEvidenceReport, ReplayReport, TargetReport};
use crate::verification::detector_replay::{
    DetectorReplayPlan, ReplayEvidence, ReplayFixture, ReplayTarget,
};

pub(super) fn validate(report: &ReplayReport) -> Result<(), String> {
    let mut targets = BTreeMap::<ReplayTarget, Vec<ReplayFixture>>::new();
    for fixture in &report.fixtures {
        let target = ReplayTarget {
            package: fixture.target.package.clone(),
            kind: fixture.target.kind.clone(),
            name: fixture.target.name.clone(),
        };
        targets.entry(target).or_default().push(ReplayFixture {
            identity: TestIdentity {
                package: fixture.target.package.clone(),
                target_kind: fixture.target.kind.clone(),
                target: fixture.target.name.clone(),
                test_name: fixture.test_name.clone(),
            },
            fixture: fixture.source.fixture_symbol.clone(),
            fixture_path: PathBuf::from(&fixture.source.fixture_path),
            fixture_sha256: fixture.source.fixture_sha256.clone(),
            detector: fixture.source.detector_symbol.clone(),
            detector_path: PathBuf::from(&fixture.source.detector_path),
            detector_sha256: fixture.source.detector_sha256.clone(),
            registered_identity: fixture.source.registered_identity.clone(),
            source_graph_sha256: fixture.source.source_graph_sha256.clone(),
            expected_witnesses: fixture.source.expected_witnesses.clone(),
            evidence: canonical_evidence(&fixture.evidence)?,
        });
    }
    let replay = DetectorReplayPlan::new(targets);
    let canonical_targets = replay
        .targets()
        .keys()
        .map(TargetReport::from)
        .collect::<Vec<_>>();
    if canonical_targets != report.compilation.targets {
        return Err("replay report target rows are not in canonical inventory order".to_owned());
    }
    let canonical_fixtures = replay
        .targets()
        .iter()
        .flat_map(|(target, fixtures)| {
            fixtures
                .iter()
                .map(move |fixture| (target, fixture.identity.test_name.as_str()))
        })
        .collect::<Vec<_>>();
    let report_fixtures = report
        .fixtures
        .iter()
        .map(|fixture| {
            (
                ReplayTarget {
                    package: fixture.target.package.clone(),
                    kind: fixture.target.kind.clone(),
                    name: fixture.target.name.clone(),
                },
                fixture.test_name.as_str(),
            )
        })
        .collect::<Vec<_>>();
    if canonical_fixtures
        .iter()
        .map(|(target, test)| ((*target).clone(), *test))
        .collect::<Vec<_>>()
        != report_fixtures
    {
        return Err("replay report fixture rows are not in canonical inventory order".to_owned());
    }
    let observed_sha256 = replay.inventory_sha256()?;
    if report.inventory.sha256.as_deref() != Some(observed_sha256.as_str())
        || replay.fixture_count() != report.inventory.fixtures
        || replay.target_count() != report.inventory.targets
        || replay.evidence_binding_count() != report.inventory.evidence_bindings
    {
        return Err("replay report inventory digest does not match its fixture rows".to_owned());
    }
    Ok(())
}

fn canonical_evidence(rows: &[ReplayEvidenceReport]) -> Result<Vec<ReplayEvidence>, String> {
    let evidence = rows
        .iter()
        .map(|row| ReplayEvidence {
            invariant_id: row.invariant_id.clone(),
            evidence_id: row.evidence_id.clone(),
        })
        .collect::<Vec<_>>();
    let mut canonical = evidence.clone();
    canonical.sort();
    canonical.dedup();
    if evidence != canonical {
        return Err("replay fixture evidence rows are not unique canonical order".to_owned());
    }
    Ok(evidence)
}
