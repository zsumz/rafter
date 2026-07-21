//! Fail-closed registry selection and source-contract analysis for replay.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path, PathBuf},
};

use crate::{
    contract::{
        catalog::{Catalog, EvidenceDescriptor},
        profile::{DetectorReplayContract, ProfileContract},
        TestIdentity,
    },
    evidence::limits::MAX_ARTIFACT_BYTES,
    verification::{
        DetectorFixtureAnalysis, DetectorFixtureContract, DetectorFixtureSourceBinding,
    },
};
use sha2::{Digest, Sha256};

use super::{model::ReplayEvidence, DetectorReplayPlan, ReplayFixture, ReplayTarget};

#[derive(Clone, Debug, Eq, PartialEq)]
struct FixtureCandidate {
    identity: TestIdentity,
    fixture: String,
    fixture_path: PathBuf,
    detector: String,
    detector_path: PathBuf,
    evidence: BTreeSet<ReplayEvidence>,
}

#[cfg(test)]
pub(crate) fn prepare(
    catalog: &Catalog,
    profile: &ProfileContract,
    contract: &DetectorReplayContract,
    source_root: &Path,
) -> Result<DetectorReplayPlan, String> {
    prepare_inner(catalog, profile, contract, source_root, None)
}

pub(crate) fn prepare_bounded(
    catalog: &Catalog,
    profile: &ProfileContract,
    contract: &DetectorReplayContract,
    source_root: &Path,
    deadline: std::time::Instant,
) -> Result<DetectorReplayPlan, String> {
    prepare_inner(catalog, profile, contract, source_root, Some(deadline))
}

fn prepare_inner(
    catalog: &Catalog,
    profile: &ProfileContract,
    contract: &DetectorReplayContract,
    source_root: &Path,
    deadline: Option<std::time::Instant>,
) -> Result<DetectorReplayPlan, String> {
    let candidates = candidates(catalog, profile)?;
    let canonical_root = fs::canonicalize(source_root)
        .map_err(|error| format!("canonicalize replay source root: {error}"))?;
    let mut analysis = DetectorFixtureAnalysis::default();
    let mut targets = BTreeMap::<ReplayTarget, Vec<ReplayFixture>>::new();
    for candidate in candidates.into_values() {
        require_time(deadline)?;
        let (fixture_path, fixture_source) = source(&canonical_root, &candidate.fixture_path)?;
        let (detector_path, detector_source) = source(&canonical_root, &candidate.detector_path)?;
        let DetectorFixtureContract {
            registered_identity,
            witnesses,
            source_graph_sha256,
        } = analysis.analyze(&DetectorFixtureSourceBinding {
            fixture_source: &fixture_source,
            detector_source: &detector_source,
            source_root: &canonical_root,
            fixture_path: &fixture_path,
            detector_path: &detector_path,
            test_identity: &candidate.identity,
            fixture: &candidate.fixture,
            detector: &candidate.detector,
        })?;
        targets
            .entry(ReplayTarget::from(&candidate.identity))
            .or_default()
            .push(ReplayFixture {
                identity: candidate.identity,
                fixture: candidate.fixture,
                fixture_path: candidate.fixture_path,
                fixture_sha256: sha256(fixture_source.as_bytes()),
                detector: candidate.detector,
                detector_path: candidate.detector_path,
                detector_sha256: sha256(detector_source.as_bytes()),
                registered_identity,
                source_graph_sha256,
                expected_witnesses: witnesses,
                evidence: candidate.evidence.into_iter().collect(),
            });
    }
    let replay = DetectorReplayPlan::new(targets);
    require_time(deadline)?;
    require_reviewed_inventory(&replay, contract)?;
    Ok(replay)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn require_time(deadline: Option<std::time::Instant>) -> Result<(), String> {
    if deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
        return Err("detector replay inventory analysis deadline expired".to_owned());
    }
    Ok(())
}

pub(crate) fn required_evidence(
    catalog: &Catalog,
    profile: &ProfileContract,
) -> Vec<ReplayEvidence> {
    catalog
        .required_evidence(profile)
        .into_values()
        .flatten()
        .filter(|descriptor| descriptor.layer == "simulator" && descriptor.strength == "direct")
        .map(|descriptor| {
            let evidence_id = descriptor.evidence_id();
            ReplayEvidence {
                invariant_id: descriptor.invariant_id,
                evidence_id,
            }
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn candidates(
    catalog: &Catalog,
    profile: &ProfileContract,
) -> Result<BTreeMap<TestIdentity, FixtureCandidate>, String> {
    let mut candidates = BTreeMap::new();
    for descriptor in catalog.required_evidence(profile).into_values().flatten() {
        if descriptor.layer != "simulator" || descriptor.strength != "direct" {
            continue;
        }
        let candidate = candidate(&descriptor)?;
        match candidates.entry(candidate.identity.clone()) {
            std::collections::btree_map::Entry::Vacant(entry) => {
                entry.insert(candidate);
            }
            std::collections::btree_map::Entry::Occupied(mut entry) => {
                let existing = entry.get_mut();
                if existing.fixture != candidate.fixture
                    || existing.fixture_path != candidate.fixture_path
                    || existing.detector != candidate.detector
                    || existing.detector_path != candidate.detector_path
                {
                    return Err(format!(
                        "detector replay identity {} has conflicting registry bindings",
                        existing.identity.test_name
                    ));
                }
                existing.evidence.extend(candidate.evidence);
            }
        }
    }
    Ok(candidates)
}

fn candidate(descriptor: &EvidenceDescriptor) -> Result<FixtureCandidate, String> {
    let field = |value: &Option<String>, name: &str| {
        value.clone().ok_or_else(|| {
            format!(
                "direct simulator evidence {} has no {name}",
                descriptor.evidence_id()
            )
        })
    };
    let identity = descriptor
        .simulator
        .as_ref()
        .and_then(|simulator| simulator.negative_test.clone())
        .ok_or_else(|| {
            format!(
                "direct simulator evidence {} has no negative test identity",
                descriptor.evidence_id()
            )
        })?;
    Ok(FixtureCandidate {
        identity,
        fixture: field(&descriptor.negative_fixture, "negative fixture")?,
        fixture_path: field(&descriptor.negative_fixture_path, "negative fixture path")?.into(),
        detector: field(
            &descriptor.negative_fixture_detector,
            "negative fixture detector",
        )?,
        detector_path: descriptor.negative_detector_path().into(),
        evidence: BTreeSet::from([ReplayEvidence {
            invariant_id: descriptor.invariant_id.clone(),
            evidence_id: descriptor.evidence_id(),
        }]),
    })
}

fn source(root: &Path, relative: &Path) -> Result<(PathBuf, String), String> {
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "detector replay source is not canonical repository-relative: {}",
            relative.display()
        ));
    }
    let path = fs::canonicalize(root.join(relative)).map_err(|error| {
        format!(
            "canonicalize detector replay source {}: {error}",
            relative.display()
        )
    })?;
    if !path.starts_with(root) {
        return Err(format!(
            "detector replay source escapes authenticated root: {}",
            relative.display()
        ));
    }
    let length = fs::metadata(&path)
        .map_err(|error| format!("inspect detector replay source {}: {error}", path.display()))?
        .len();
    if length > MAX_ARTIFACT_BYTES {
        return Err(format!(
            "detector replay source {} exceeds the {MAX_ARTIFACT_BYTES}-byte limit",
            relative.display()
        ));
    }
    let source = fs::read_to_string(&path)
        .map_err(|error| format!("read detector replay source {}: {error}", path.display()))?;
    Ok((path, source))
}

fn require_reviewed_inventory(
    replay: &DetectorReplayPlan,
    contract: &DetectorReplayContract,
) -> Result<(), String> {
    let inventory_sha256 = replay.inventory_sha256()?;
    if replay.fixture_count() != contract.required_unique_fixtures
        || replay.evidence_binding_count() != contract.required_evidence_bindings
        || replay.target_count() != contract.required_targets
        || inventory_sha256 != contract.required_inventory_sha256
    {
        return Err(format!(
            "detector replay requires inventory {} with exactly {} fixtures, {} evidence bindings, and {} targets; found inventory {} with {} fixtures, {} evidence bindings, and {} targets",
            contract.required_inventory_sha256,
            contract.required_unique_fixtures,
            contract.required_evidence_bindings,
            contract.required_targets,
            inventory_sha256,
            replay.fixture_count(),
            replay.evidence_binding_count(),
            replay.target_count()
        ));
    }
    Ok(())
}
