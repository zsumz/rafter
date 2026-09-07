//! Stable label text for the coverage observation vocabulary.
//!
//! Every observation carries one `snake_case` label that verification artifacts
//! quote verbatim, and `ALL` fixes the order those labels are reported in.

use super::Observation;

pub(super) const ALL: [Observation; 77] = [
    Observation::WellFormedStatesChecked,
    Observation::TermAdvances,
    Observation::SameTermVoteReobservations,
    Observation::SameTermVotedRestarts,
    Observation::NonvoterVoteDecisions,
    Observation::StaleLogVoteDecisions,
    Observation::ElectionCertificates,
    Observation::EligibleLeaderCertificates,
    Observation::StableElectionCertificates,
    Observation::JointElectionCertificates,
    Observation::HigherTermAuthorityDeliveries,
    Observation::StaleAuthorityResponses,
    Observation::StaleAuthorityStateComparisons,
    Observation::PreVoteRequestDeliveries,
    Observation::LeaderPreVoteRequestDeliveries,
    Observation::StalePreVoteResponses,
    Observation::SameTermLeaderLogGrowth,
    Observation::SuccessfulNonemptyAppendObservations,
    Observation::SuccessfulAppendPrevLogMatches,
    Observation::SuccessfulAppendStoredSuffixMatches,
    Observation::CrossNodeIndexTermPrefixComparisons,
    Observation::CrossNodeCommittedIndexComparisons,
    Observation::LaterTermLeaderPriorPrefixChecks,
    Observation::CommitFloorAdvances,
    Observation::CommitIndexWithinLocalLogBoundsChecks,
    Observation::PreTransitionJointCommitCertificates,
    Observation::PostAppendJointCommitCertificates,
    Observation::StableCommitCertificates,
    Observation::CurrentTermCommitCertificates,
    Observation::CurrentTermCommitCoveringPriorTermPrefix,
    Observation::CommittedPrefixHistoryComparisons,
    Observation::CrossNodeCommittedPrefixAgreementChecks,
    Observation::AppliesOrSnapshotBoundaries,
    Observation::ApplicationCursorComparisons,
    Observation::MultipleOrderedAppliesSameEpoch,
    Observation::SameIndexApplyPairs,
    Observation::SameIndexApplicationWitnessPairs,
    Observation::SameIndexConfigurationWitnessPairs,
    Observation::CrossEpochExecutionWitnessPairs,
    Observation::SameIndexApplicationResultPairs,
    Observation::SameIndexConfigurationResultPairs,
    Observation::StatesWithOneUncommittedConfiguration,
    Observation::CommittedConfigurationAdvances,
    Observation::SameIndexCommittedConfigurationIdentityChecks,
    Observation::RegisteredReadGrants,
    Observation::MatchedReadGrantRegistrations,
    Observation::ReadGrantCommittedFloorComparisons,
    Observation::CompletedReads,
    Observation::CompletedWriteBeforeReadHistories,
    Observation::DurableRestartComparisons,
    Observation::RestartTermComparisons,
    Observation::RestartTermVoteComparisons,
    Observation::RestartLogComparisons,
    Observation::RestartCommitConfigurationComparisons,
    Observation::RestartSnapshotComparisons,
    Observation::RestartAcknowledgedEntryComparisons,
    Observation::RestartRecoveriesWithNonzeroAppliedFloor,
    Observation::RestartNonemptyExpectedReplayComparisons,
    Observation::RestartAppliedFloorBoundComparisons,
    Observation::ExpectedSnapshotInstallsChecked,
    Observation::SnapshotBoundaryAdvances,
    Observation::SnapshotPayloadBindingsChecked,
    Observation::SnapshotTransferIdentitiesChecked,
    Observation::SnapshotCoveredPrefixesChecked,
    Observation::SnapshotNextRetainedIndicesChecked,
    Observation::SnapshotPersistedBoundariesChecked,
    Observation::SnapshotChunkIdentitiesChecked,
    Observation::SnapshotChunkOffsetsChecked,
    Observation::SnapshotInstallCompletenessChecked,
    Observation::PendingSnapshotLifecyclesChecked,
    Observation::NodesWithNonzeroSnapshotIndex,
    Observation::PartialSnapshotTransfersChecked,
    Observation::SameBoundarySnapshotInstallPairs,
    Observation::ProductionConfigCommitObserved,
    Observation::WindowOneBackpressureObserved,
    Observation::LeaseFastPathReadGranted,
    Observation::JointConfigRestartSnapshotRecovered,
];

impl Observation {
    pub(in crate::model_check) const fn label(self) -> &'static str {
        match self {
            Self::WellFormedStatesChecked => "well_formed_states_checked",
            Self::TermAdvances => "term_advances",
            Self::SameTermVoteReobservations => "same_term_vote_reobservations",
            Self::SameTermVotedRestarts => "same_term_voted_restarts",
            Self::NonvoterVoteDecisions => "nonvoter_vote_decisions",
            Self::StaleLogVoteDecisions => "stale_log_vote_decisions",
            Self::ElectionCertificates => "election_certificates",
            Self::EligibleLeaderCertificates => "eligible_leader_certificates",
            Self::StableElectionCertificates => "stable_election_certificates",
            Self::JointElectionCertificates => "joint_election_certificates",
            Self::HigherTermAuthorityDeliveries => "higher_term_authority_deliveries",
            Self::StaleAuthorityResponses => "stale_authority_responses",
            Self::StaleAuthorityStateComparisons => "stale_authority_state_comparisons",
            Self::PreVoteRequestDeliveries => "pre_vote_request_deliveries",
            Self::LeaderPreVoteRequestDeliveries => "leader_pre_vote_request_deliveries",
            Self::StalePreVoteResponses => "stale_pre_vote_responses",
            Self::SameTermLeaderLogGrowth => "same_term_leader_log_growth",
            Self::SuccessfulNonemptyAppendObservations => "successful_nonempty_append_observations",
            Self::SuccessfulAppendPrevLogMatches => "successful_append_prev_log_matches",
            Self::SuccessfulAppendStoredSuffixMatches => "successful_append_stored_suffix_matches",
            Self::CrossNodeIndexTermPrefixComparisons => "cross_node_index_term_prefix_comparisons",
            Self::CrossNodeCommittedIndexComparisons => "cross_node_committed_index_comparisons",
            Self::LaterTermLeaderPriorPrefixChecks => "later_term_leader_prior_prefix_checks",
            Self::CommitFloorAdvances => "commit_floor_advances",
            Self::CommitIndexWithinLocalLogBoundsChecks => {
                "commit_index_within_local_log_bounds_checks"
            }
            Self::PreTransitionJointCommitCertificates => {
                "pre_transition_joint_commit_certificates"
            }
            Self::PostAppendJointCommitCertificates => "post_append_joint_commit_certificates",
            Self::StableCommitCertificates => "stable_commit_certificates",
            Self::CurrentTermCommitCertificates => "current_term_commit_certificates",
            Self::CurrentTermCommitCoveringPriorTermPrefix => {
                "current_term_commit_covering_prior_term_prefix"
            }
            Self::CommittedPrefixHistoryComparisons => "committed_prefix_history_comparisons",
            Self::CrossNodeCommittedPrefixAgreementChecks => {
                "cross_node_committed_prefix_agreement_checks"
            }
            Self::AppliesOrSnapshotBoundaries => "applies_or_snapshot_boundaries",
            Self::ApplicationCursorComparisons => "application_cursor_comparisons",
            Self::MultipleOrderedAppliesSameEpoch => "multiple_ordered_applies_same_epoch",
            Self::SameIndexApplyPairs => "same_index_apply_pairs",
            Self::SameIndexApplicationWitnessPairs => "same_index_application_witness_pairs",
            Self::SameIndexConfigurationWitnessPairs => "same_index_configuration_witness_pairs",
            Self::CrossEpochExecutionWitnessPairs => "cross_epoch_execution_witness_pairs",
            Self::SameIndexApplicationResultPairs => "same_index_application_result_pairs",
            Self::SameIndexConfigurationResultPairs => "same_index_configuration_result_pairs",
            Self::StatesWithOneUncommittedConfiguration => {
                "states_with_one_uncommitted_configuration"
            }
            Self::CommittedConfigurationAdvances => "committed_configuration_advances",
            Self::SameIndexCommittedConfigurationIdentityChecks => {
                "same_index_committed_configuration_identity_checks"
            }
            Self::RegisteredReadGrants => "registered_read_grants",
            Self::MatchedReadGrantRegistrations => "matched_read_grant_registrations",
            Self::ReadGrantCommittedFloorComparisons => "read_grant_committed_floor_comparisons",
            Self::CompletedReads => "completed_reads",
            Self::CompletedWriteBeforeReadHistories => "completed_write_before_read_histories",
            Self::DurableRestartComparisons => "durable_restart_comparisons",
            Self::RestartTermComparisons => "restart_term_comparisons",
            Self::RestartTermVoteComparisons => "restart_term_vote_comparisons",
            Self::RestartLogComparisons => "restart_log_comparisons",
            Self::RestartCommitConfigurationComparisons => {
                "restart_commit_configuration_comparisons"
            }
            Self::RestartSnapshotComparisons => "restart_snapshot_comparisons",
            Self::RestartAcknowledgedEntryComparisons => "restart_acknowledged_entry_comparisons",
            Self::RestartRecoveriesWithNonzeroAppliedFloor => {
                "restart_recoveries_with_nonzero_applied_floor"
            }
            Self::RestartNonemptyExpectedReplayComparisons => {
                "restart_nonempty_expected_replay_comparisons"
            }
            Self::RestartAppliedFloorBoundComparisons => "restart_applied_floor_bound_comparisons",
            Self::ExpectedSnapshotInstallsChecked => "expected_snapshot_installs_checked",
            Self::SnapshotBoundaryAdvances => "snapshot_boundary_advances",
            Self::SnapshotPayloadBindingsChecked => "snapshot_payload_bindings_checked",
            Self::SnapshotTransferIdentitiesChecked => "snapshot_transfer_identities_checked",
            Self::SnapshotCoveredPrefixesChecked => "snapshot_covered_prefixes_checked",
            Self::SnapshotNextRetainedIndicesChecked => "snapshot_next_retained_indices_checked",
            Self::SnapshotPersistedBoundariesChecked => "snapshot_persisted_boundaries_checked",
            Self::SnapshotChunkIdentitiesChecked => "snapshot_chunk_identities_checked",
            Self::SnapshotChunkOffsetsChecked => "snapshot_chunk_offsets_checked",
            Self::SnapshotInstallCompletenessChecked => "snapshot_install_completeness_checked",
            Self::PendingSnapshotLifecyclesChecked => "pending_snapshot_lifecycles_checked",
            Self::NodesWithNonzeroSnapshotIndex => "nodes_with_nonzero_snapshot_index",
            Self::PartialSnapshotTransfersChecked => "partial_snapshot_transfers_checked",
            Self::SameBoundarySnapshotInstallPairs => "same_boundary_snapshot_install_pairs",
            Self::ProductionConfigCommitObserved => "production_config_commit_observed",
            Self::WindowOneBackpressureObserved => "window_one_backpressure_observed",
            Self::LeaseFastPathReadGranted => "lease_fast_path_read_granted",
            Self::JointConfigRestartSnapshotRecovered => "joint_config_restart_snapshot_recovered",
        }
    }
}
