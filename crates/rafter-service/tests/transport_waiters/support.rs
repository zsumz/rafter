//! The fixtures the waiter scenarios are driven from, and the watchdog they run
//! under.
//!
//! An elected leader whose follower never answers, so every future a scenario
//! starts is genuinely unresolved; a worker-thread watchdog, so a reproduced
//! deadlock names the drop that never returned instead of hanging the suite;
//! and the transport that drops a client future from inside `send`, which is
//! the re-entrancy hazard no caller of `with_group` is involved in.

use std::{
    future::Future,
    pin::Pin,
    sync::{mpsc, Arc, Mutex},
    time::Duration,
};

use rafter::{PreVoteResponse, RequestVoteResponse};
use rafter_service::{
    AuthenticatedPeerEnvelope, PeerEnvelope, PeerPolicy, RaftTransport, SnapshotChunkEnvelope,
    TransportRaftDriver,
};

use super::support::transport::{
    cluster, elect, Driver, Principal, QueueTransport, TransportError, Validator, GROUP,
};
use super::support::{KvStateMachine, Message, NodeId, Role, WriteError, WriteReceipt};

/// How long a re-entrancy test waits before calling a hang a hang.
///
/// The tests below reproduce deadlocks. They run their fixture on a worker
/// thread and watch it from the test thread, so a regression reports which drop
/// never returned instead of hanging the whole suite.
const WATCHDOG: Duration = Duration::from_secs(5);

/// An elected two-voter leader whose follower never answers, so a write stays
/// appended-and-unacknowledged and a read stays reserved. Every future dropped
/// below is genuinely unresolved, which is the only state a guard has work in.
pub(super) fn leader_with_a_silent_follower() -> Driver {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    driver.clone()
}

/// Runs `body` on a worker thread and fails with `stuck` if it does not finish.
pub(super) fn watched(stuck: &'static str, body: impl FnOnce() + Send + 'static) {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        body();
        let _ = sender.send(());
    });
    receiver.recv_timeout(WATCHDOG).unwrap_or_else(|_| {
        panic!("{stuck}");
    });
}

/// A transport whose `send` drops a client future somebody stashed in it.
///
/// This is the hazard away from `with_group`: the driver calls a transport
/// while routing a report, under its own lock, and an embedder's transport may
/// own a client future — one it kept to retry, or one belonging to a task it
/// resumes. Nothing warns it, because it never reads `with_group`'s contract.
pub(super) type StashedWrite =
    Pin<Box<dyn Future<Output = Result<WriteReceipt<Option<String>>, WriteError>> + Send>>;

#[derive(Clone, Default)]
pub(super) struct DropOnSendTransport {
    pub(super) link: QueueTransport,
    stashed: Arc<Mutex<Option<StashedWrite>>>,
}

impl DropOnSendTransport {
    pub(super) fn stash(&self, future: StashedWrite) {
        *self
            .stashed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(future);
    }
}

impl RaftTransport<u64> for DropOnSendTransport {
    type PeerPrincipal = Principal;
    type Error = TransportError;

    fn send(&self, envelope: PeerEnvelope<u64>) -> Result<(), Self::Error> {
        drop(
            self.stashed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
        self.link.send(envelope)
    }

    fn send_snapshot_chunk(&self, envelope: SnapshotChunkEnvelope<u64>) -> Result<(), Self::Error> {
        self.link.send_snapshot_chunk(envelope)
    }

    fn update_peers(
        &self,
        group_id: &u64,
        policy: PeerPolicy<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error> {
        self.link.update_peers(group_id, policy)
    }
}

pub(super) type DropOnSendDriver = TransportRaftDriver<
    u64,
    KvStateMachine,
    rafter_runtime::DurableRaftNode,
    DropOnSendTransport,
    Validator,
>;

/// Elects a lone replica by answering its own election frames with grants.
///
/// The support cluster cannot do this one: it is typed over `QueueTransport`,
/// and the whole point of this fixture is a different transport. The follower
/// exists only as a voter, so once the election is over it never answers again
/// and a write stays appended-and-unacknowledged — which is the state a live
/// waiter needs.
pub(super) fn elect_by_granting_its_own_votes(driver: &DropOnSendDriver, link: &QueueTransport) {
    for _ in 0..32 {
        if driver.handle().metrics().expect("metrics").current().role == Role::Leader {
            return;
        }
        driver.tick().expect("a tick advances the protocol");
        for envelope in link.take_deliverable() {
            let granted = match envelope.message {
                Message::PreVote(vote) => Message::PreVoteResponse(PreVoteResponse {
                    term: vote.term,
                    voter_id: NodeId(2),
                    vote_granted: true,
                }),
                Message::RequestVote(vote) => Message::RequestVoteResponse(RequestVoteResponse {
                    term: vote.term,
                    voter_id: NodeId(2),
                    vote_granted: true,
                }),
                _ => continue,
            };
            driver
                .deliver(AuthenticatedPeerEnvelope {
                    group_id: GROUP,
                    authenticated_peer: Principal::for_node(NodeId(2)),
                    raft_from: NodeId(2),
                    raft_to: NodeId(1),
                    message: granted,
                })
                .expect("a grant from an authorized voter is accepted");
        }
    }
    panic!("the replica never took leadership within the tick budget");
}
