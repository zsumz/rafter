//! Shared cluster and configuration fixtures for membership scenarios.

pub(super) use super::super::helpers::{
    assert_append_entries_response, assert_vote_response, elect_leader, node,
};
pub(super) use super::super::snapshot::support::{snapshot_source, test_snapshot};
pub(super) use super::*;

pub(super) fn joint_configuration(config_id: ConfigurationId) -> ConfigurationEntry {
    let old = membership(&[1, 2, 3]);
    let new = membership(&[1, 3, 4]);
    ConfigurationEntry::joint(config_id, JointMembership::new(old, new))
}

pub(super) fn learner_configuration() -> ConfigurationEntry {
    learner_configuration_with_id(ConfigurationId(2))
}

pub(super) fn learner_configuration_with_id(config_id: ConfigurationId) -> ConfigurationEntry {
    ConfigurationEntry::stable(
        config_id,
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
            .expect("learner membership is valid"),
    )
}

pub(super) fn stable_configuration(
    config_id: ConfigurationId,
    voters: &[u64],
) -> ConfigurationEntry {
    ConfigurationEntry::stable(config_id, membership(voters))
}

pub(super) fn committed_leader_with_learner_config() -> Node {
    let mut leader = leader_with_log(vec![BootstrapLogEntry::configuration(
        LogIndex(1),
        Term(2),
        learner_configuration(),
    )]);
    let committed = leader.last_log_index();
    leader.volatile.commit_index = committed;
    leader.volatile.applied_index = committed;
    leader
}

pub(super) fn leader_with_snapshot_and_learner_suffix() -> (Node, crate::InMemorySnapshotChunkSource)
{
    let snapshot = test_snapshot(1, 1, 2, b"learner snapshot");
    let source = snapshot_source(&snapshot, b"learner snapshot".to_vec());
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3), NodeId(4)], 3)
            .expect("test Raft node config is valid"),
        BootstrapState {
            current_term: Term(2),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot),
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(2),
                Term(2),
                learner_configuration(),
            )],
        },
    )
    .expect("leader log bootstraps");
    leader.become_leader();
    (leader, source)
}

pub(super) fn node_with_configuration(
    id: u64,
    peers: &[u64],
    configuration: ConfigurationEntry,
) -> Node {
    Node::from_bootstrap(
        NodeConfig::new(NodeId(id), peers.iter().copied().map(NodeId).collect(), 3)
            .expect("test Raft node config is valid"),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(1),
                Term(1),
                configuration,
            )],
        },
    )
    .expect("configured node bootstraps")
}

pub(super) fn leader_with_log(log: Vec<BootstrapLogEntry>) -> Node {
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3), NodeId(4)], 3)
            .expect("test Raft node config is valid"),
        BootstrapState {
            current_term: Term(2),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log,
        },
    )
    .expect("leader log bootstraps");
    leader.become_leader();
    leader
}

pub(super) fn acknowledge(
    leader: &mut Node,
    follower_id: NodeId,
    match_index: LogIndex,
) -> Vec<Output> {
    leader.step(Input::Message {
        from: follower_id,
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id,
            success: true,
            match_index,
        }),
    })
}

pub(super) fn assert_pre_vote_response(
    outputs: &[Output],
    to: NodeId,
    term: Term,
    vote_granted: bool,
) {
    assert_eq!(outputs.len(), 1);
    let Output::Send {
        to: actual_to,
        message,
    } = &outputs[0]
    else {
        panic!("expected pre-vote response");
    };
    assert_eq!(*actual_to, to);
    let Message::PreVoteResponse(response) = message else {
        panic!("expected pre-vote response");
    };
    assert_eq!(response.term, term);
    assert_eq!(response.vote_granted, vote_granted);
}

pub(super) fn membership(voters: &[u64]) -> MembershipSet {
    MembershipSet::new(voters.iter().copied().map(NodeId).collect(), Vec::new())
        .expect("membership is valid")
}

pub(super) fn send_targets(outputs: &[Output]) -> Vec<NodeId> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::Send { to, .. } => Some(*to),
            Output::LocalProposalAppended { .. }
            | Output::LocalProposalDropped { .. }
            | Output::Apply { .. }
            | Output::ApplySnapshot { .. }
            | Output::SendSnapshotChunk { .. }
            | Output::StageSnapshotChunk { .. }
            | Output::RejectProposal { .. }
            | Output::LeadershipTransferRejected { .. }
            | Output::ReadIndexGranted { .. }
            | Output::ReadIndexRejected { .. }
            | Output::ReadIndexCanceled { .. } => None,
        })
        .collect()
}

pub(super) fn append_entries_entry_count(output: &Output) -> Option<usize> {
    let Output::Send { message, .. } = output else {
        return None;
    };
    let Message::AppendEntries(request) = message else {
        return None;
    };
    Some(request.entries.len())
}

pub(super) fn append_entries_to(outputs: &[Output], target: NodeId) -> Option<&AppendEntries> {
    outputs.iter().find_map(|output| match output {
        Output::Send {
            to,
            message: Message::AppendEntries(request),
        } if *to == target => Some(request),
        Output::LocalProposalAppended { .. }
        | Output::LocalProposalDropped { .. }
        | Output::Apply { .. }
        | Output::ApplySnapshot { .. }
        | Output::SendSnapshotChunk { .. }
        | Output::StageSnapshotChunk { .. }
        | Output::RejectProposal { .. }
        | Output::LeadershipTransferRejected { .. }
        | Output::ReadIndexGranted { .. }
        | Output::ReadIndexRejected { .. }
        | Output::ReadIndexCanceled { .. }
        | Output::Send { .. } => None,
    })
}

pub(super) fn grant_vote(node: &mut Node, voter_id: NodeId) -> Vec<Output> {
    node.step(Input::Message {
        from: voter_id,
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: node.current_term(),
            voter_id,
            vote_granted: true,
        }),
    })
}

/// Grants the pre-candidate's pending poll, which proposes one term past the
/// poller's current term.
pub(super) fn grant_pre_vote(node: &mut Node, voter_id: NodeId) -> Vec<Output> {
    node.step(Input::Message {
        from: voter_id,
        message: Message::PreVoteResponse(PreVoteResponse {
            term: node.current_term().next(),
            voter_id,
            vote_granted: true,
        }),
    })
}
