//! Shared leadership-transfer fixtures and `TimeoutNow` inspection.

pub(super) use super::*;

pub(super) fn leader_with_acknowledged_follower() -> Node {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    // Follower 2 acknowledges the leader's initial no-op entry.
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(1),
        }),
    });
    leader
}

pub(super) fn timeout_now_to(outputs: &[Output], target: NodeId) -> Option<TimeoutNow> {
    outputs.iter().find_map(|output| match output {
        Output::Send {
            to,
            message: Message::TimeoutNow(request),
        } if *to == target => Some(*request),
        _ => None,
    })
}
