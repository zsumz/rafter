//! Immutable replay inventory derived from reviewed registry and source contracts.

use std::{collections::BTreeMap, path::PathBuf};

use crate::contract::TestIdentity;
use sha2::{Digest, Sha256};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ReplayTarget {
    pub(crate) package: String,
    pub(crate) kind: String,
    pub(crate) name: String,
}

impl From<&TestIdentity> for ReplayTarget {
    fn from(identity: &TestIdentity) -> Self {
        Self {
            package: identity.package.clone(),
            kind: identity.target_kind.clone(),
            name: identity.target.clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct ReplayEvidence {
    pub(crate) invariant_id: String,
    pub(crate) evidence_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ReplayFixture {
    pub(crate) identity: TestIdentity,
    pub(crate) fixture: String,
    pub(crate) fixture_path: PathBuf,
    pub(crate) fixture_sha256: String,
    pub(crate) detector: String,
    pub(crate) detector_path: PathBuf,
    pub(crate) detector_sha256: String,
    pub(crate) registered_identity: String,
    pub(crate) source_graph_sha256: String,
    pub(crate) expected_witnesses: BTreeMap<String, usize>,
    pub(crate) evidence: Vec<ReplayEvidence>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DetectorReplayPlan {
    targets: BTreeMap<ReplayTarget, Vec<ReplayFixture>>,
}

impl DetectorReplayPlan {
    pub(super) fn new(targets: BTreeMap<ReplayTarget, Vec<ReplayFixture>>) -> Self {
        Self { targets }
    }

    #[cfg(test)]
    pub(in crate::verification) fn from_test_targets(
        targets: BTreeMap<ReplayTarget, Vec<ReplayFixture>>,
    ) -> Self {
        Self::new(targets)
    }

    pub(crate) fn targets(&self) -> &BTreeMap<ReplayTarget, Vec<ReplayFixture>> {
        &self.targets
    }

    pub(crate) fn target_count(&self) -> usize {
        self.targets.len()
    }

    pub(crate) fn fixture_count(&self) -> usize {
        self.targets.values().map(Vec::len).sum()
    }

    pub(crate) fn evidence_binding_count(&self) -> usize {
        self.targets
            .values()
            .flatten()
            .map(|fixture| fixture.evidence.len())
            .sum()
    }

    pub(crate) fn inventory_sha256(&self) -> Result<String, String> {
        let mut digest = Sha256::new();
        for (target, fixtures) in &self.targets {
            frame(&mut digest, b"target-package", target.package.as_bytes())?;
            frame(&mut digest, b"target-kind", target.kind.as_bytes())?;
            frame(&mut digest, b"target-name", target.name.as_bytes())?;
            for fixture in fixtures {
                frame(
                    &mut digest,
                    b"fixture-package",
                    fixture.identity.package.as_bytes(),
                )?;
                frame(
                    &mut digest,
                    b"fixture-kind",
                    fixture.identity.target_kind.as_bytes(),
                )?;
                frame(
                    &mut digest,
                    b"fixture-target",
                    fixture.identity.target.as_bytes(),
                )?;
                frame(
                    &mut digest,
                    b"fixture-test",
                    fixture.identity.test_name.as_bytes(),
                )?;
                frame(&mut digest, b"fixture-symbol", fixture.fixture.as_bytes())?;
                frame(
                    &mut digest,
                    b"fixture-path",
                    path_bytes(&fixture.fixture_path)?,
                )?;
                frame(
                    &mut digest,
                    b"fixture-sha256",
                    fixture.fixture_sha256.as_bytes(),
                )?;
                frame(&mut digest, b"detector-symbol", fixture.detector.as_bytes())?;
                frame(
                    &mut digest,
                    b"detector-path",
                    path_bytes(&fixture.detector_path)?,
                )?;
                frame(
                    &mut digest,
                    b"detector-sha256",
                    fixture.detector_sha256.as_bytes(),
                )?;
                frame(
                    &mut digest,
                    b"registered-identity",
                    fixture.registered_identity.as_bytes(),
                )?;
                frame(
                    &mut digest,
                    b"source-graph-sha256",
                    fixture.source_graph_sha256.as_bytes(),
                )?;
                for (witness, count) in &fixture.expected_witnesses {
                    frame(&mut digest, b"witness", witness.as_bytes())?;
                    let count = u64::try_from(*count)
                        .map_err(|_| "detector replay witness count exceeds u64".to_owned())?;
                    frame(&mut digest, b"witness-count", &count.to_be_bytes())?;
                }
                for evidence in &fixture.evidence {
                    frame(
                        &mut digest,
                        b"evidence-invariant",
                        evidence.invariant_id.as_bytes(),
                    )?;
                    frame(&mut digest, b"evidence-id", evidence.evidence_id.as_bytes())?;
                }
            }
        }
        Ok(format!("{:x}", digest.finalize()))
    }
}

fn path_bytes(path: &std::path::Path) -> Result<&[u8], String> {
    path.to_str().map(str::as_bytes).ok_or_else(|| {
        format!(
            "detector replay inventory path is not UTF-8: {}",
            path.display()
        )
    })
}

fn frame(digest: &mut Sha256, label: &[u8], value: &[u8]) -> Result<(), String> {
    let label_length = u64::try_from(label.len())
        .map_err(|_| "detector replay inventory label exceeds u64".to_owned())?;
    let value_length = u64::try_from(value.len())
        .map_err(|_| "detector replay inventory value exceeds u64".to_owned())?;
    digest.update(label_length.to_be_bytes());
    digest.update(label);
    digest.update(value_length.to_be_bytes());
    digest.update(value);
    Ok(())
}

#[cfg(test)]
#[path = "model_tests.rs"]
mod tests;
