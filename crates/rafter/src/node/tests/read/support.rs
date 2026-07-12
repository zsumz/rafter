//! Shared healthy-leader and message fixtures for read scenarios.

pub(super) use super::super::helpers::{elect_leader, node};
pub(super) use super::*;

/// A three-voter leader that has committed one entry in its current term,
/// making it eligible to serve read barriers.
pub(super) fn leader_with_current_term_commit() -> Node {
    commit_first_entry(node(1, &[2, 3]))
}

/// Elects `leader` and commits one current-term entry through node 2's
/// acknowledgement.
pub(super) fn commit_first_entry(mut leader: Node) -> Node {
    let _ = elect_leader(&mut leader);
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(1),
            sequence: 0,
        }),
    });
    assert_eq!(leader.commit_index(), LogIndex(1));
    leader
}

pub(super) fn ack(leader: &mut Node, follower: u64, sequence: u64) -> Vec<Output> {
    let term = leader.current_term();
    let match_index = leader.last_log_index();
    leader.step(Input::Message {
        from: NodeId(follower),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term,
            follower_id: NodeId(follower),
            success: true,
            match_index,
            sequence,
        }),
    })
}

pub(super) fn heartbeat_round(outputs: &[Output]) -> u64 {
    outputs
        .iter()
        .find_map(|output| match output {
            Output::Send {
                message: Message::AppendEntries(AppendEntries { sequence, .. }),
                ..
            } => Some(*sequence),
            _ => None,
        })
        .expect("leader tick broadcasts a heartbeat")
}

pub(super) fn heartbeat_rounds_to(outputs: &[Output], to: NodeId) -> Vec<u64> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::Send {
                to: target,
                message: Message::AppendEntries(AppendEntries { sequence, .. }),
            } if *target == to => Some(*sequence),
            _ => None,
        })
        .collect()
}

pub(super) fn read_index(read_id: u64) -> Input {
    Input::ReadIndex {
        read_id: ReadId(read_id),
    }
}

pub(super) fn granted(outputs: &[Output]) -> Vec<(ReadId, LogIndex)> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::ReadIndexGranted {
                read_id,
                read_index,
            } => Some((*read_id, *read_index)),
            _ => None,
        })
        .collect()
}

pub(super) fn canceled(outputs: &[Output]) -> Vec<(ReadId, ReadIndexCancelReason)> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::ReadIndexCanceled { read_id, reason } => Some((*read_id, *reason)),
            _ => None,
        })
        .collect()
}
