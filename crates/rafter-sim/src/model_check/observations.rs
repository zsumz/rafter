use std::hash::{Hash, Hasher};

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
    HigherTermAuthorityDeliveries,
    StaleAuthorityResponses,
    PreVoteRequestDeliveries,
    StalePreVoteResponses,
    SameTermLeaderLogGrowth,
    SuccessfulNonemptyAppendObservations,
    CrossNodeIndexTermPrefixComparisons,
    CrossNodeCommittedIndexComparisons,
    LaterTermLeaderPriorPrefixChecks,
    CommitFloorAdvances,
    PreTransitionJointCommitCertificates,
    PostAppendJointCommitCertificates,
    CurrentTermCommitCertificates,
    AppliesOrSnapshotBoundaries,
    SameIndexApplyPairs,
    StatesWithOneUncommittedConfiguration,
    CommittedConfigurationAdvances,
    RegisteredReadGrants,
    CompletedReads,
    CompletedWriteBeforeReadHistories,
    DurableRestartComparisons,
    RestartRecoveriesWithNonzeroAppliedFloor,
    ExpectedSnapshotInstallsChecked,
    NodesWithNonzeroSnapshotIndex,
    PartialSnapshotTransfersChecked,
    SameBoundarySnapshotInstallPairs,
}

const ALL: [Observation; 33] = [
    Observation::WellFormedStatesChecked,
    Observation::TermAdvances,
    Observation::SameTermVoteReobservations,
    Observation::SameTermVotedRestarts,
    Observation::NonvoterVoteDecisions,
    Observation::StaleLogVoteDecisions,
    Observation::ElectionCertificates,
    Observation::HigherTermAuthorityDeliveries,
    Observation::StaleAuthorityResponses,
    Observation::PreVoteRequestDeliveries,
    Observation::StalePreVoteResponses,
    Observation::SameTermLeaderLogGrowth,
    Observation::SuccessfulNonemptyAppendObservations,
    Observation::CrossNodeIndexTermPrefixComparisons,
    Observation::CrossNodeCommittedIndexComparisons,
    Observation::LaterTermLeaderPriorPrefixChecks,
    Observation::CommitFloorAdvances,
    Observation::PreTransitionJointCommitCertificates,
    Observation::PostAppendJointCommitCertificates,
    Observation::CurrentTermCommitCertificates,
    Observation::AppliesOrSnapshotBoundaries,
    Observation::SameIndexApplyPairs,
    Observation::StatesWithOneUncommittedConfiguration,
    Observation::CommittedConfigurationAdvances,
    Observation::RegisteredReadGrants,
    Observation::CompletedReads,
    Observation::CompletedWriteBeforeReadHistories,
    Observation::DurableRestartComparisons,
    Observation::RestartRecoveriesWithNonzeroAppliedFloor,
    Observation::ExpectedSnapshotInstallsChecked,
    Observation::NodesWithNonzeroSnapshotIndex,
    Observation::PartialSnapshotTransfersChecked,
    Observation::SameBoundarySnapshotInstallPairs,
];

impl Observation {
    pub(super) const fn label(self) -> &'static str {
        match self {
            Self::WellFormedStatesChecked => "well_formed_states_checked",
            Self::TermAdvances => "term_advances",
            Self::SameTermVoteReobservations => "same_term_vote_reobservations",
            Self::SameTermVotedRestarts => "same_term_voted_restarts",
            Self::NonvoterVoteDecisions => "nonvoter_vote_decisions",
            Self::StaleLogVoteDecisions => "stale_log_vote_decisions",
            Self::ElectionCertificates => "election_certificates",
            Self::HigherTermAuthorityDeliveries => "higher_term_authority_deliveries",
            Self::StaleAuthorityResponses => "stale_authority_responses",
            Self::PreVoteRequestDeliveries => "pre_vote_request_deliveries",
            Self::StalePreVoteResponses => "stale_pre_vote_responses",
            Self::SameTermLeaderLogGrowth => "same_term_leader_log_growth",
            Self::SuccessfulNonemptyAppendObservations => "successful_nonempty_append_observations",
            Self::CrossNodeIndexTermPrefixComparisons => "cross_node_index_term_prefix_comparisons",
            Self::CrossNodeCommittedIndexComparisons => "cross_node_committed_index_comparisons",
            Self::LaterTermLeaderPriorPrefixChecks => "later_term_leader_prior_prefix_checks",
            Self::CommitFloorAdvances => "commit_floor_advances",
            Self::PreTransitionJointCommitCertificates => {
                "pre_transition_joint_commit_certificates"
            }
            Self::PostAppendJointCommitCertificates => "post_append_joint_commit_certificates",
            Self::CurrentTermCommitCertificates => "current_term_commit_certificates",
            Self::AppliesOrSnapshotBoundaries => "applies_or_snapshot_boundaries",
            Self::SameIndexApplyPairs => "same_index_apply_pairs",
            Self::StatesWithOneUncommittedConfiguration => {
                "states_with_one_uncommitted_configuration"
            }
            Self::CommittedConfigurationAdvances => "committed_configuration_advances",
            Self::RegisteredReadGrants => "registered_read_grants",
            Self::CompletedReads => "completed_reads",
            Self::CompletedWriteBeforeReadHistories => "completed_write_before_read_histories",
            Self::DurableRestartComparisons => "durable_restart_comparisons",
            Self::RestartRecoveriesWithNonzeroAppliedFloor => {
                "restart_recoveries_with_nonzero_applied_floor"
            }
            Self::ExpectedSnapshotInstallsChecked => "expected_snapshot_installs_checked",
            Self::NodesWithNonzeroSnapshotIndex => "nodes_with_nonzero_snapshot_index",
            Self::PartialSnapshotTransfersChecked => "partial_snapshot_transfers_checked",
            Self::SameBoundarySnapshotInstallPairs => "same_boundary_snapshot_install_pairs",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ObservationSet(u64);

impl ObservationSet {
    pub(super) fn mark(&mut self, observation: Observation) {
        self.0 |= 1_u64 << observation as u8;
    }

    pub(super) const fn contains(self, observation: Observation) -> bool {
        self.0 & (1_u64 << observation as u8) != 0
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
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::{Observation, ObservationSet};

    #[test]
    fn observations_are_named_but_do_not_change_state_hashes() {
        let empty = ObservationSet::default();
        let mut observed = empty;
        observed.mark(Observation::ElectionCertificates);

        assert_ne!(empty, observed);
        assert_eq!(
            observed.labels().collect::<Vec<_>>(),
            ["election_certificates"]
        );
        assert_eq!(hash(empty), hash(observed));
    }

    fn hash(value: ObservationSet) -> u64 {
        let mut hasher = DefaultHasher::new();
        value.hash(&mut hasher);
        hasher.finish()
    }
}
