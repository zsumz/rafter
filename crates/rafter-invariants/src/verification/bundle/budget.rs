//! Trusted resource policy for unverified receipts and artifact bundles.

use crate::{
    evidence::limits::{MAX_ARTIFACT_REFS_PER_BUNDLE, MAX_VERDICT_ARTIFACT_REFS},
    verification::AggregateError,
};

pub(crate) const MAX_RECEIPT_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAX_RETAINED_ARTIFACT_BYTES: u64 = 512 * 1024 * 1024;
pub(crate) const MAX_PLAN_INPUT_BYTES: u64 = 8 * 1024 * 1024;
pub(crate) const MAX_PLAN_INPUT_TOTAL_BYTES: u64 = 32 * 1024 * 1024;

const MAX_MAELSTROM_NIGHTLY_ARTIFACT_REFS: usize = 1_024;
const MAX_MAELSTROM_WEEKLY_ARTIFACT_REFS: usize = MAX_VERDICT_ARTIFACT_REFS;
const MAX_MAELSTROM_NIGHTLY_ARTIFACT_DECLARATIONS: usize = 2_048;
const MAX_MAELSTROM_WEEKLY_ARTIFACT_DECLARATIONS: usize = 8_192;

const GIB: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct BundleBudget {
    pub(super) declarations: usize,
    pub(super) references: usize,
    pub(super) bytes: u64,
}

impl BundleBudget {
    pub(crate) fn for_trusted(profile: &str, runner: &str) -> Result<Self, AggregateError> {
        let (artifact_declarations, artifact_refs, artifact_bytes) = match runner {
            "tests" | "simulator" | "tla" => (
                MAX_ARTIFACT_REFS_PER_BUNDLE,
                MAX_ARTIFACT_REFS_PER_BUNDLE,
                GIB,
            ),
            "maelstrom" if profile == "weekly" => (
                MAX_MAELSTROM_WEEKLY_ARTIFACT_DECLARATIONS,
                MAX_MAELSTROM_WEEKLY_ARTIFACT_REFS,
                4 * GIB,
            ),
            "maelstrom" if profile == "nightly" => (
                MAX_MAELSTROM_NIGHTLY_ARTIFACT_DECLARATIONS,
                MAX_MAELSTROM_NIGHTLY_ARTIFACT_REFS,
                2 * GIB,
            ),
            "maelstrom" => {
                return Err(AggregateError::new(
                    "Maelstrom evidence is not permitted in the PR profile".to_owned(),
                ));
            }
            runner => {
                return Err(AggregateError::new(format!(
                    "no artifact resource policy exists for runner {runner}"
                )));
            }
        };
        Ok(Self {
            declarations: artifact_declarations,
            references: artifact_refs,
            bytes: artifact_bytes,
        })
    }

    #[cfg(test)]
    pub(crate) const fn artifact_declarations(self) -> usize {
        self.declarations
    }

    #[cfg(test)]
    pub(crate) const fn artifact_refs(self) -> usize {
        self.references
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ProfileBudget {
    receipt_bytes: u64,
    artifact_refs: usize,
    artifact_bytes: u64,
}

impl ProfileBudget {
    pub(crate) fn for_trusted(profile: &str, receipt_count: usize) -> Result<Self, AggregateError> {
        let receipt_count = u64::try_from(receipt_count).map_err(|error| {
            AggregateError::new(format!("represent trusted receipt count: {error}"))
        })?;
        let receipt_bytes = receipt_count
            .checked_mul(MAX_RECEIPT_BYTES)
            .ok_or_else(|| {
                AggregateError::new("profile receipt byte budget overflowed u64".to_owned())
            })?;
        let artifact_bytes = match profile {
            "pr" => 3 * GIB / 2,
            "nightly" => 4 * GIB,
            "weekly" => 6 * GIB,
            _ => {
                return Err(AggregateError::new(format!(
                    "no artifact resource policy exists for profile {profile}"
                )));
            }
        };
        Ok(Self {
            receipt_bytes,
            artifact_refs: MAX_VERDICT_ARTIFACT_REFS,
            artifact_bytes,
        })
    }

    pub(crate) const fn receipt_bytes(self) -> u64 {
        self.receipt_bytes
    }

    pub(crate) const fn artifact_refs(self) -> usize {
        self.artifact_refs
    }

    pub(crate) const fn artifact_bytes(self) -> u64 {
        self.artifact_bytes
    }
}
