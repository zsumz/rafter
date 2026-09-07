//! Coverage marks recording which situations an exploration actually reached.
//!
//! An observation set names evidence, never protocol state: it is aggregated
//! out of band and hashes to a constant, so marking coverage can never split a
//! model state in two or inflate the explored state count. A situation never
//! reached is reported as a coverage gap, not a pass.

use std::hash::{Hash, Hasher};

mod label;

use label::ALL;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub(super) enum Observation {
    WellFormedStatesChecked,
    TermAdvances,
    SameTermVoteReobservations,
    SameTermVotedRestarts,
    NonvoterVoteDecisions,
    StaleLogVoteDecisions,
    ElectionCertificates,
    EligibleLeaderCertificates,
    StableElectionCertificates,
    JointElectionCertificates,
    HigherTermAuthorityDeliveries,
    StaleAuthorityResponses,
    StaleAuthorityStateComparisons,
    PreVoteRequestDeliveries,
    LeaderPreVoteRequestDeliveries,
    StalePreVoteResponses,
    SameTermLeaderLogGrowth,
    SuccessfulNonemptyAppendObservations,
    SuccessfulAppendPrevLogMatches,
    SuccessfulAppendStoredSuffixMatches,
    CrossNodeIndexTermPrefixComparisons,
    CrossNodeCommittedIndexComparisons,
    LaterTermLeaderPriorPrefixChecks,
    CommitFloorAdvances,
    CommitIndexWithinLocalLogBoundsChecks,
    PreTransitionJointCommitCertificates,
    PostAppendJointCommitCertificates,
    StableCommitCertificates,
    CurrentTermCommitCertificates,
    CurrentTermCommitCoveringPriorTermPrefix,
    CommittedPrefixHistoryComparisons,
    CrossNodeCommittedPrefixAgreementChecks,
    AppliesOrSnapshotBoundaries,
    ApplicationCursorComparisons,
    MultipleOrderedAppliesSameEpoch,
    SameIndexApplyPairs,
    SameIndexApplicationWitnessPairs,
    SameIndexConfigurationWitnessPairs,
    CrossEpochExecutionWitnessPairs,
    SameIndexApplicationResultPairs,
    SameIndexConfigurationResultPairs,
    StatesWithOneUncommittedConfiguration,
    CommittedConfigurationAdvances,
    SameIndexCommittedConfigurationIdentityChecks,
    RegisteredReadGrants,
    MatchedReadGrantRegistrations,
    ReadGrantCommittedFloorComparisons,
    CompletedReads,
    CompletedWriteBeforeReadHistories,
    DurableRestartComparisons,
    RestartTermComparisons,
    RestartTermVoteComparisons,
    RestartLogComparisons,
    RestartCommitConfigurationComparisons,
    RestartSnapshotComparisons,
    RestartAcknowledgedEntryComparisons,
    RestartRecoveriesWithNonzeroAppliedFloor,
    RestartNonemptyExpectedReplayComparisons,
    RestartAppliedFloorBoundComparisons,
    ExpectedSnapshotInstallsChecked,
    SnapshotBoundaryAdvances,
    SnapshotPayloadBindingsChecked,
    SnapshotTransferIdentitiesChecked,
    SnapshotCoveredPrefixesChecked,
    SnapshotNextRetainedIndicesChecked,
    SnapshotPersistedBoundariesChecked,
    SnapshotChunkIdentitiesChecked,
    SnapshotChunkOffsetsChecked,
    SnapshotInstallCompletenessChecked,
    PendingSnapshotLifecyclesChecked,
    NodesWithNonzeroSnapshotIndex,
    PartialSnapshotTransfersChecked,
    SameBoundarySnapshotInstallPairs,
    ProductionConfigCommitObserved,
    WindowOneBackpressureObserved,
    LeaseFastPathReadGranted,
    JointConfigRestartSnapshotRecovered,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ObservationSet(u128);

impl ObservationSet {
    pub(super) fn mark(&mut self, observation: Observation) {
        self.0 |= 1_u128 << observation as u8;
    }

    pub(super) const fn contains(self, observation: Observation) -> bool {
        self.0 & (1_u128 << observation as u8) != 0
    }

    pub(super) const fn union_with(&mut self, other: Self) {
        self.0 |= other.0;
    }

    pub(super) fn labels(self) -> impl Iterator<Item = &'static str> {
        ALL.into_iter()
            .filter(move |observation| self.contains(*observation))
            .map(Observation::label)
    }
}

// Coverage is aggregated out-of-band and must never split model states.
impl Hash for ObservationSet {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

#[cfg(test)]
#[path = "observations_test.rs"]
mod tests;
