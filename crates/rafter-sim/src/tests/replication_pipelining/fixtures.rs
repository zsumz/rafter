use super::super::*;
use rafter::{Message, ReplicationProgress, RequestVoteResponse};

pub(super) const LEADER: NodeId = NodeId(1);
pub(super) const BEHIND_FOLLOWER: NodeId = NodeId(2);
pub(super) const CAUGHT_UP_FOLLOWER: NodeId = NodeId(3);

pub(super) fn follower_progress(cluster: &Cluster, follower: NodeId) -> ReplicationProgress {
    cluster
        .leader_replication_progress(LEADER)
        .into_iter()
        .find(|progress| progress.follower_id == follower)
        .expect("leader must report progress for its follower")
}

/// Messages deliverable right now — the round's in-flight generation.
pub(super) fn ready_message_count(cluster: &Cluster) -> usize {
    let now = cluster.clock().now();
    cluster
        .network
        .iter()
        .filter(|queued| queued.ready_at <= now)
        .count()
}

/// Delivers every message in flight at the call exactly once; responses
/// provoked by these deliveries stay queued for the next round.
pub(super) fn deliver_ready_generation(cluster: &mut Cluster) {
    for _ in 0..ready_message_count(cluster) {
        assert!(
            cluster.deliver_one_matching(|_| true),
            "a counted in-flight message must be deliverable"
        );
    }
}

pub(super) fn vote_response(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(
                envelope.message,
                Message::RequestVoteResponse(RequestVoteResponse { .. })
            )
    }
}

pub(super) fn bootstrap_state(
    current_term: Term,
    entries: &[(u64, Term, &[u8])],
) -> BootstrapState {
    BootstrapState {
        current_term,
        voted_for: None,
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
        snapshot: None,
        log: entries
            .iter()
            .map(|(index, term, payload)| {
                BootstrapLogEntry::application(LogIndex(*index), *term, (*payload).to_vec())
            })
            .collect(),
    }
}
