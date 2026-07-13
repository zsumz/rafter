//! Shared leaders, progress mutation, and message inspection for pipelining scenarios.

use super::*;

pub(super) fn payload(index: u64) -> Vec<u8> {
    let byte = u8::try_from(index).expect("test entry indexes fit into a payload byte");
    vec![byte; PAYLOAD_BYTES]
}

pub(super) fn one_entry_batch_bytes() -> usize {
    LogEntry::application(Term(1), payload(1)).replication_bytes()
}

pub(super) fn pipelining_leader(
    entry_count: u64,
    configure: impl FnOnce(NodeConfig) -> NodeConfig,
) -> Node {
    let log = (1..=entry_count)
        .map(|index| bootstrap_entry(index, 1, &payload(index)))
        .collect();
    let mut leader = Node::from_bootstrap(
        configure(
            NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
                .expect("test Raft node config is valid")
                .with_max_append_entries_bytes(ONE_ENTRY_BATCH_BUDGET),
        ),
        BootstrapState {
            current_term: Term(2),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log,
        },
    )
    .expect("leader bootstraps from a prior-term log");
    leader.become_leader();
    leader
}

pub(super) fn seed_replicating(leader: &mut Node, follower: NodeId, match_index: LogIndex) {
    *leader
        .try_follower_progress_mut(follower)
        .expect("active follower") = Progress {
        match_index,
        next_index: match_index.next(),
        mode: ProgressMode::Replicate,
        inflights: Inflights::default(),
    };
}

pub(super) fn follower_progress(leader: &Node, follower: NodeId) -> &Progress {
    leader
        .leader
        .progress
        .get(follower)
        .expect("active follower")
}

pub(super) fn replication_state(leader: &Node, follower: NodeId) -> ReplicationState {
    leader
        .leader_replication_progress()
        .into_iter()
        .find(|progress| progress.follower_id == follower)
        .expect("the leader reports every follower")
        .state
}

pub(super) fn appends_to(outputs: &[Output], to: NodeId) -> Vec<&AppendEntries> {
    outputs
        .iter()
        .filter_map(|output| {
            let Output::Send {
                to: actual_to,
                message: Message::AppendEntries(request),
            } = output
            else {
                return None;
            };
            (*actual_to == to).then_some(request)
        })
        .collect()
}

pub(super) fn snapshot_chunks_to(outputs: &[Output], to: NodeId) -> Vec<&crate::SnapshotChunkSend> {
    outputs
        .iter()
        .filter_map(|output| {
            let Output::SendSnapshotChunk {
                to: actual_to,
                chunk,
            } = output
            else {
                return None;
            };
            (*actual_to == to).then_some(chunk)
        })
        .collect()
}

pub(super) fn deliver_append_response(
    leader: &mut Node,
    follower: NodeId,
    success: bool,
    match_index: LogIndex,
) -> Vec<Output> {
    leader.step(Input::Message {
        from: follower,
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: follower,
            success,
            match_index,
        }),
    })
}
