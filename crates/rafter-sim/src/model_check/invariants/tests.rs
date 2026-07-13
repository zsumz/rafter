use std::collections::BTreeSet;

use rafter::{
    AppendEntries, AppendEntriesResponse, BootstrapLogEntry, CommittedConfiguration,
    ConfigurationEntry, ConfigurationId, LogEntry, LogEntryKind, MembershipConfig, MembershipSet,
    Message, NodeConfig, NodeId, PendingSnapshotTransfer, PreVote, PreVoteResponse, RequestVote,
    RequestVoteResponse, SharedEntries, SnapshotTransferId, Term,
};

use super::super::helpers::{
    bootstrap_state, bootstrap_with_snapshot, elect_node_one, test_snapshot, three_node_configs,
};
use super::super::state::{
    ClientRead, ClientReadProof, ClientWrite, ClientWriteUnknownReason, CommitTransitionContext,
    ElectionCertificate, LogPrefixWitness, LogicalLogHistory, LogicalLogView,
};
use super::*;
use crate::{
    Applied, Cluster, DurableStateDigest, Envelope, ExecutedLogEntry, ExecutionWitness,
    ReferenceState, SnapshotInstalled,
};

fn one_node_cluster() -> Cluster {
    let config =
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3).expect("test config is valid");
    Cluster::new(vec![config])
}

fn stable_membership(voters: &[u64], learners: &[u64]) -> MembershipConfig {
    MembershipConfig::stable(
        MembershipSet::new(ids(voters), ids(learners)).expect("test membership is valid"),
    )
}

fn joint_membership(old: &[u64], new: &[u64]) -> MembershipConfig {
    MembershipConfig::joint(
        MembershipSet::new(ids(old), Vec::new()).expect("old membership is valid"),
        MembershipSet::new(ids(new), Vec::new()).expect("new membership is valid"),
    )
}

fn ids(values: &[u64]) -> Vec<NodeId> {
    values.iter().copied().map(NodeId).collect()
}

fn execution_witness(
    node_id: u64,
    application_epoch: u64,
    index: u64,
    term: u64,
    kind: LogEntryKind,
    prior_state: ReferenceState,
) -> ExecutionWitness {
    let entry = ExecutedLogEntry {
        index: LogIndex(index),
        term: Term(term),
        kind,
    };
    let mut resulting_state = prior_state.clone();
    match &entry.kind {
        LogEntryKind::Application(payload) => {
            resulting_state.application_value.clone_from(payload);
        }
        LogEntryKind::Configuration(configuration) => {
            resulting_state.committed_membership = configuration.membership_config();
            resulting_state.committed_configuration = Some(CommittedConfiguration {
                index: entry.index,
                config_id: configuration.config_id(),
            });
        }
        LogEntryKind::Noop => {}
    }
    ExecutionWitness {
        node_id: NodeId(node_id),
        application_epoch,
        commit_index_at_emit: LogIndex(index),
        entry,
        prior_state,
        resulting_state,
    }
}

fn initial_reference_state() -> ReferenceState {
    ReferenceState {
        application_value: Vec::new().into(),
        committed_membership: stable_membership(&[1, 2, 3], &[]),
        committed_configuration: None,
    }
}

fn state_with_committed_application_witness(payload: &[u8]) -> ExplorationState {
    let mut cluster = one_node_cluster();
    let mut bootstrap = bootstrap_state(Term(1), &[(1, Term(1), payload)]);
    bootstrap.commit_index = LogIndex(1);
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap)
        .expect("committed application witness bootstrap is valid");
    ExplorationState::new(cluster)
}

fn state_with_committed_configuration_witness(
    configuration: ConfigurationEntry,
) -> ExplorationState {
    let mut cluster = one_node_cluster();
    let mut bootstrap = bootstrap_state(Term(2), &[]);
    bootstrap.commit_index = LogIndex(1);
    bootstrap.committed_configuration = Some(CommittedConfiguration {
        index: LogIndex(1),
        config_id: configuration.config_id(),
    });
    bootstrap.log.push(BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(1),
        configuration,
    ));
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap)
        .expect("committed configuration witness bootstrap is valid");
    ExplorationState::new(cluster)
}

fn grant_set(values: &[u64]) -> BTreeSet<NodeId> {
    ids(values).into_iter().collect()
}

fn election_certificate(
    term: u64,
    leader: u64,
    membership: MembershipConfig,
    grants: &[u64],
) -> ElectionCertificate {
    ElectionCertificate {
        leader_id: NodeId(leader),
        term: Term(term),
        membership,
        granted_by: grant_set(grants),
        last_log_index: LogIndex::ZERO,
        last_log_term: Term::default(),
    }
}

fn state_with_recorded_certificate(certificate: ElectionCertificate) -> ExplorationState {
    let mut state = ExplorationState::new(one_node_cluster());
    state.election_history_mut().record_election(certificate);
    state
}

mod election;

fn two_node_cluster() -> Cluster {
    Cluster::new(vec![
        NodeConfig::new(NodeId(1), vec![NodeId(2)], 3).expect("node-1 config is valid"),
        NodeConfig::new(NodeId(2), vec![NodeId(1)], 3).expect("node-2 config is valid"),
    ])
}

mod commit_basics;

fn append_entries_transition_state(
    before_entries: &[(u64, Term, &[u8])],
    after_entries: &[(u64, Term, &[u8])],
    request: AppendEntries,
    response: AppendEntriesResponse,
) -> ExplorationState {
    let mut before = two_node_cluster();
    before
        .restart_node_from_bootstrap(NodeId(2), bootstrap_state(Term(2), before_entries))
        .expect("before follower bootstrap is valid");

    let mut after = two_node_cluster();
    after
        .restart_node_from_bootstrap(NodeId(2), bootstrap_state(Term(2), after_entries))
        .expect("after follower bootstrap is valid");
    let mut state = ExplorationState::new(after);
    let delivered = Envelope {
        from: NodeId(1),
        to: NodeId(2),
        message: Message::AppendEntries(request),
    };
    let emitted = [Envelope {
        from: NodeId(2),
        to: NodeId(1),
        message: Message::AppendEntriesResponse(response),
    }];
    state.record_log_transition(&before, Some(&delivered), &emitted);
    state
}

fn append_request(prev_log_term: Term, entries: Vec<LogEntry>) -> AppendEntries {
    AppendEntries {
        term: Term(2),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(1),
        prev_log_term,
        sequence: 9,
        entries: SharedEntries::from(entries),
        leader_commit: LogIndex::ZERO,
    }
}

fn append_success(match_index: LogIndex) -> AppendEntriesResponse {
    AppendEntriesResponse {
        term: Term(2),
        follower_id: NodeId(2),
        success: true,
        match_index,
        sequence: 9,
    }
}

mod log_history;

mod commit_history;
mod commit_history_ledger;
mod commit_history_snapshot;
mod commit_history_transition;

mod application_epoch;
mod snapshot_application;

mod persistence_read;

mod client_history;

mod applied_agreement;
