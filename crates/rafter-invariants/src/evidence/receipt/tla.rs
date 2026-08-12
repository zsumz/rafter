//! Primary-continuation policy and outcome vocabulary for TLA+ receipts.

use serde::{Deserialize, Serialize};

/// Configuration key naming a profile's primary-continuation policy.
pub(crate) const PRIMARY_COMPLETION_KEY: &str = "primary_completion";

const GATING_FRONTIER_EXHAUSTED: &str = "gating-frontier-exhausted";
const REPORTING_CONTINUATION: &str = "reporting-continuation";

/// What a profile's primary configuration is allowed to mean.
///
/// The bounded PR model genuinely drains its queue, so its continuation gates:
/// nothing short of exhaustion at the calibrated floors passes. The scheduled
/// monolith has never drained once and, at a measured frontier fanout near 2.85
/// with no inflection, will not; gating on an event that cannot occur produces
/// a permanently red lane that reports nothing about the protocol.
///
/// Under `ReportingContinuation` the continuation still runs its full budget,
/// still checkpoints and recovers, still emits every artifact, and is still
/// verified end to end as source-bound evidence. Only verdict derivation
/// changes, and only for the single outcome "budget elapsed with a healthy open
/// frontier". A counterexample is still a counterexample, and malformed or
/// missing evidence is still a harness error.
///
/// This enum is deliberately exhaustive so an unreviewed policy fails during
/// decoding instead of being treated as one of the reviewed two.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum PrimaryCompletionPolicy {
    #[serde(rename = "gating-frontier-exhausted")]
    GatingFrontierExhausted,
    #[serde(rename = "reporting-continuation")]
    ReportingContinuation,
}

impl PrimaryCompletionPolicy {
    /// Decodes the pinned configuration value, rejecting anything unreviewed.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            GATING_FRONTIER_EXHAUSTED => Some(Self::GatingFrontierExhausted),
            REPORTING_CONTINUATION => Some(Self::ReportingContinuation),
            _ => None,
        }
    }

    /// Whether the primary continuation's own completion decides the layer.
    #[must_use]
    pub const fn gates(self) -> bool {
        matches!(self, Self::GatingFrontierExhausted)
    }
}

/// How the primary continuation actually ended, independent of whether that
/// ending gated.
///
/// Receipts carry this so a green scheduled lane still states plainly that its
/// monolith did not finish. The floor comparison is deliberately absent: in
/// reporting mode the pinned minimums are accumulation context published as
/// observations, not a terminal condition.
///
/// This enum is deliberately exhaustive so an unreviewed outcome fails during
/// decoding rather than widening what a receipt may claim.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ContinuationOutcome {
    #[serde(rename = "frontier-exhausted")]
    FrontierExhausted,
    #[serde(rename = "counterexample")]
    Counterexample,
    #[serde(rename = "budget-elapsed-frontier-open")]
    BudgetElapsedFrontierOpen,
}

/// The primary continuation's declared policy and observed ending.
///
/// Present on every TLA+ check receipt and required by the TLA+ receipt
/// validator. The policy is contract-pinned rather than producer-chosen, so the
/// verifier rejects a receipt whose declared policy disagrees with the profile
/// it claims to come from.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TlaContinuationBinding {
    pub policy: PrimaryCompletionPolicy,
    pub outcome: ContinuationOutcome,
}
