use std::collections::{BTreeMap, BTreeSet};

use rafter::{CommittedConfiguration, LogIndex, NodeId, SharedPayload};

#[path = "application.rs"]
mod application;
mod application_history;
mod client;
mod commit;
mod coverage;
mod election;
mod logical_log;
mod purpose;
mod restart_snapshot;
mod seeds;
mod snapshot;

mod accessors;
mod exploration;

use self::application::InstrumentedCluster;
pub(super) use self::application::{
    apply_pending_application_replay_seed, apply_scheduled_operation,
    apply_snapshot_bootstrap_seeds, apply_soak_action, apply_to_restart_snapshot_state,
    apply_to_state, restart_node, restart_node_losing_application_state, try_apply_soak_action,
    PendingApplicationReplaySeed, SnapshotBootstrapSeed,
};
#[cfg(test)]
pub(super) use self::application::{
    record_execution_corruption, rewind_execution_cursor_for_fixture, ExecutionRecorderCorruption,
};
use self::application_history::ApplicationHistory;
use super::observations::ObservationSet;
pub(super) use client::{
    ClientHistory, ClientRead, ClientReadOutcome, ClientReadProof, ClientWriteStatus,
};
#[cfg(test)]
pub(super) use client::{ClientWrite, ClientWriteUnknownReason};
#[cfg(test)]
pub(super) use commit::CommitTransitionContext;
pub(super) use commit::{CommitHistory, ConfigurationAppend};
#[cfg(test)]
pub(super) use election::ElectionCertificate;
pub(super) use election::ElectionHistory;
pub(super) use election::{AuthorityTransitionViolationKind, PreVoteViolationKind};
pub(super) use logical_log::LogicalLogHistory;
#[cfg(test)]
pub(super) use logical_log::{LogPrefixWitness, LogicalLogView};
use purpose::PurposeWitnessHistory;
pub(super) use restart_snapshot::{ExpectedSnapshot, RestartSnapshotState};
pub(super) use snapshot::snapshot_payload_binding_issue;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct TransitionInstrumentationError {
    pub(in crate::model_check) kind: super::FailureKind,
    pub(in crate::model_check) invariant: &'static str,
    pub(in crate::model_check) message: String,
}

#[derive(Clone, Debug, Hash)]
pub(super) struct ExplorationState {
    cluster: InstrumentedCluster,
    proposals_issued: u64,
    restarts_issued: u64,
    read_indexes_issued: u64,
    membership_changes_issued: u64,
    transfers_issued: u64,
    partitions_issued: u64,
    lossy_restarts_issued: u64,
    commit_floor_by_node: BTreeMap<NodeId, LogIndex>,
    committed_configuration_floor_by_node: BTreeMap<NodeId, Option<CommittedConfiguration>>,
    application_history: ApplicationHistory,
    client_history: ClientHistory,
    required_applied_payloads: BTreeMap<(NodeId, LogIndex), SharedPayload>,
    required_committed_configurations: BTreeMap<(NodeId, LogIndex), CommittedConfiguration>,
    required_commit_indexes: BTreeSet<(NodeId, LogIndex)>,
    election_history: ElectionHistory,
    logical_log_history: LogicalLogHistory,
    commit_history: CommitHistory,
    snapshot_history: snapshot::SnapshotHistory,
    purpose_witness_history: PurposeWitnessHistory,
    observations: ObservationSet,
    transition_instrumentation_errors: BTreeSet<TransitionInstrumentationError>,
}

impl ExplorationState {
    #[cfg(test)]
    pub(in crate::model_check) fn inject_bootstrap_state(
        &mut self,
        node_id: NodeId,
        bootstrap: rafter::BootstrapState,
    ) -> Result<(), rafter::BootstrapValidationError> {
        self.cluster.restart_node_from_bootstrap(node_id, bootstrap)
    }

    #[cfg(test)]
    pub(in crate::model_check) fn inject_message(
        &mut self,
        from: NodeId,
        to: NodeId,
        message: rafter::Message,
    ) {
        self.cluster.queue_message(from, to, message);
    }

    #[cfg(test)]
    pub(in crate::model_check) fn drop_all_messages(&mut self) {
        self.cluster.drop_matching(|_| true);
    }
}
