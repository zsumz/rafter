use rafter::{LogIndex, Message, NodeId, RequestVote, Term};
use rafter_service::AuthenticatedPeerEnvelope;

use super::*;
use crate::queue::ReceiveMemoryBudget;
use crate::{InboundQueueLimits, OutboundQueueLimits};

fn limits() -> RuntimeLimits {
    let outbound = OutboundQueueLimits::new(4, 400, 1, 100, 1).expect("valid outbound limits");
    let inbound = InboundQueueLimits::new(1, 100, 2, 200).expect("valid inbound limits");
    RuntimeLimits::new(outbound, inbound, 2).expect("valid runtime limits")
}

#[test]
fn peer_and_global_capacity_release_exactly_on_drain() {
    let queue = InboundQueue::new(limits());
    let peer_a = PeerId::new("peer-a").expect("peer A");
    let peer_b = PeerId::new("peer-b").expect("peer B");
    let peer_c = PeerId::new("peer-c").expect("peer C");

    queue
        .try_push(peer_a.clone(), 80, envelope(&peer_a, NodeId(1)), permit(80))
        .expect("first peer A frame");
    assert_eq!(
        queue.try_push(peer_a.clone(), 1, envelope(&peer_a, NodeId(1)), permit(1),),
        Err(InboundQueueError::Full(InboundQueueFull::Peer))
    );
    queue
        .try_push(peer_b.clone(), 80, envelope(&peer_b, NodeId(2)), permit(80))
        .expect("peer B frame within global limit");
    assert_eq!(
        queue.try_push(peer_c.clone(), 1, envelope(&peer_c, NodeId(3)), permit(1),),
        Err(InboundQueueError::Full(InboundQueueFull::Global))
    );

    assert_eq!(queue.drain(1).expect("drain one").len(), 1);
    queue
        .try_push(peer_a.clone(), 80, envelope(&peer_a, NodeId(1)), permit(80))
        .expect("drain released peer and global capacity");
    assert_eq!(queue.depth().expect("queue depth").frames, 2);
}

fn permit(frame_bytes: usize) -> ReceiveMemoryPermit {
    ReceiveMemoryBudget::new(crate::ReceiveMemoryLimits::default())
        .try_acquire(frame_bytes)
        .expect("test receive-memory permit")
}

fn envelope(peer: &PeerId, sender: NodeId) -> AuthenticatedPeerEnvelope<String, PeerId> {
    AuthenticatedPeerEnvelope {
        group_id: "group".to_owned(),
        authenticated_peer: peer.clone(),
        raft_from: sender,
        raft_to: NodeId(9),
        message: Message::RequestVote(RequestVote {
            term: Term(2),
            candidate_id: sender,
            last_log_index: LogIndex(3),
            last_log_term: Term(1),
        }),
    }
}
