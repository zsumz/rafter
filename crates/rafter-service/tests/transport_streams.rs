//! The report streams a driver owes its transport: snapshot chunks and the
//! peer set.
//!
//! `SendSnapshotChunk` is the leader's only snapshot-send path and the peer set
//! is the link layer's copy of who may speak, so both scenarios here are about
//! effects that leave the process.

#![allow(clippy::wildcard_imports)]

mod support;

use std::collections::BTreeSet;

use rafter::RaftSnapshot;
use rafter_app::group::GroupFatalState;
use rafter_runtime_api::PersistedRaftRuntime;
use rafter_service::{
    AuthenticatedPeerEnvelope, InboundEnvelopeError, TransportDriverOptions, TransportRaftDriver,
};
use support::transport::*;
use support::*;

// ---------------------------------------------------------------------------
// Report streams: snapshot chunks and membership.
// ---------------------------------------------------------------------------

/// A runtime that emits, on one tick, what the kernel emits on the snapshot
/// replication path: an ordinary peer message, a leader `SendSnapshotChunk`
/// directive, and the receiver-side `StageSnapshotChunk`.
///
/// `DurableRaftNode` resolves chunk directives against its own store before the
/// app layer sees them, so this scripted runtime is what an embedder with its
/// own `PersistedRaftRuntime` produces — the case the routing exists for.
#[derive(Clone, Debug, Eq, PartialEq)]
struct SnapshotEmittingRuntime {
    emitted: bool,
}

fn snapshot_payload() -> Vec<u8> {
    b"snapshot".to_vec()
}

fn snapshot_descriptor() -> RaftSnapshot {
    let metadata = rafter::RaftSnapshotMetadata::new(
        rafter::SnapshotGroupId::new("group-7").expect("snapshot group id is valid"),
        NodeId(1),
        LogIndex(5),
        Term(1),
        Term(1),
        rafter::ApplicationSnapshotMetadata::new(
            rafter::ApplicationSnapshotKind::new("kv").expect("snapshot kind is valid"),
            rafter::ApplicationSnapshotVersion::new(1).expect("snapshot version is valid"),
        ),
    )
    .expect("snapshot metadata is valid");
    RaftSnapshot::from_payload(metadata, &snapshot_payload())
}

impl PersistedRaftRuntime for SnapshotEmittingRuntime {
    type Error = rafter_runtime::RaftRuntimeError;

    fn id(&self) -> NodeId {
        NodeId(1)
    }
    fn leader_hint(&self) -> Option<NodeId> {
        Some(NodeId(1))
    }
    fn role(&self) -> Role {
        Role::Leader
    }
    fn current_term(&self) -> Term {
        Term(1)
    }
    fn commit_index(&self) -> LogIndex {
        LogIndex(5)
    }
    fn last_log_index(&self) -> LogIndex {
        LogIndex(5)
    }
    fn snapshot_index(&self) -> LogIndex {
        LogIndex(5)
    }
    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        std::cmp::min(index, LogIndex(5))
    }
    fn membership(&self) -> MembershipConfig {
        MembershipConfig::stable(
            MembershipSet::new(vec![NodeId(1), NodeId(2)], Vec::new())
                .expect("scripted membership is valid"),
        )
    }
    fn replication(&self) -> Vec<ReplicationProgress> {
        Vec::new()
    }
    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        (index <= LogIndex(5)).then_some(Term(1))
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error> {
        if !matches!(input, RaftInput::Tick) || self.emitted {
            return Ok(Vec::new());
        }
        self.emitted = true;
        let descriptor = snapshot_descriptor();
        Ok(vec![
            // An ordinary frame, so a test can tell "the driver ran" from "the
            // driver routed snapshots".
            RaftOutput::Send {
                to: NodeId(2),
                message: Message::RequestVote(RequestVote {
                    term: Term(1),
                    candidate_id: NodeId(1),
                    last_log_index: LogIndex(5),
                    last_log_term: Term(1),
                }),
            },
            RaftOutput::SendSnapshotChunk {
                to: NodeId(2),
                chunk: rafter::SnapshotChunkSend {
                    term: Term(1),
                    leader_id: NodeId(1),
                    transfer_id: descriptor.transfer_id(),
                    metadata: descriptor.metadata.clone(),
                    total_payload_len: descriptor.application_payload_len,
                    application_payload_crc32: descriptor.application_payload_crc32,
                    offset: 0,
                    len: u32::try_from(descriptor.application_payload_len)
                        .expect("the fixture payload is small"),
                    done: true,
                },
            },
            RaftOutput::StageSnapshotChunk {
                chunk: rafter::StagedSnapshotChunk {
                    leader_id: NodeId(1),
                    transfer_id: descriptor.transfer_id(),
                    metadata: descriptor.metadata,
                    total_payload_len: descriptor.application_payload_len,
                    application_payload_crc32: descriptor.application_payload_crc32,
                    offset: 0,
                    bytes: snapshot_payload(),
                    done: true,
                },
            },
        ])
    }

    fn step_proposal_batch(
        &mut self,
        _proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, Self::Error> {
        Ok(Vec::new())
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, Self::Error> {
        let mut outputs = Vec::new();
        for input in inputs {
            outputs.extend(self.step(input)?);
        }
        Ok(outputs)
    }
}

type SnapshotDriver =
    TransportRaftDriver<u64, KvStateMachine, SnapshotEmittingRuntime, QueueTransport, Validator>;

fn snapshot_driver() -> (SnapshotDriver, QueueTransport) {
    let transport = QueueTransport::default();
    transport.serve_snapshot(&snapshot_descriptor(), snapshot_payload());
    let validator = Validator {
        transport: transport.clone(),
        authorized: BTreeSet::from([NodeId(2)]),
        nameable: None,
    };
    let driver = TransportRaftDriver::new(
        RaftGroup::new(
            GROUP,
            NodeId(1),
            SnapshotEmittingRuntime { emitted: false },
            KvStateMachine::default(),
        ),
        Vec::new(),
        transport.clone(),
        validator,
        TransportDriverOptions::default(),
    )
    .expect("a quiescent group is adoptable");
    (driver, transport)
}

fn snapshot_frames(transport: &QueueTransport) -> usize {
    transport
        .observed()
        .iter()
        .filter(|envelope| matches!(envelope.message, Message::InstallSnapshotChunk(_)))
        .count()
}

/// `SendSnapshotChunk` is the leader's only snapshot-send path, so a driver
/// that dropped it left every follower below the snapshot boundary permanently
/// behind while reporting success.
#[test]
fn a_snapshot_chunk_directive_reaches_the_transport() {
    let (driver, transport) = snapshot_driver();

    driver.tick().expect("the tick reports success");

    let observed = transport.observed();
    assert_eq!(
        observed
            .iter()
            .filter(|envelope| matches!(envelope.message, Message::RequestVote(_)))
            .count(),
        1,
        "the ordinary frame from the same report was routed too"
    );
    assert_eq!(
        snapshot_frames(&transport),
        1,
        "the directive was resolved into a frame: {observed:?}"
    );
    assert_eq!(driver.refused_sends(), 0);
}

/// A directive the link cannot serve is dropped like a lost message and
/// counted, because the protocol re-sends. It must not fail the tick.
#[test]
fn an_unservable_snapshot_directive_is_counted_rather_than_propagated() {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: BTreeSet::from([NodeId(2)]),
        nameable: None,
    };
    let driver: SnapshotDriver = TransportRaftDriver::new(
        RaftGroup::new(
            GROUP,
            NodeId(1),
            SnapshotEmittingRuntime { emitted: false },
            KvStateMachine::default(),
        ),
        Vec::new(),
        transport.clone(),
        validator,
        TransportDriverOptions::default(),
    )
    .expect("a quiescent group is adoptable");

    driver.tick().expect("an unservable chunk is not a failure");

    assert_eq!(snapshot_frames(&transport), 0);
    assert_eq!(driver.refused_sends(), 1);
}

/// The receive side is already durable when the event exists: the runtime
/// contract forbids releasing an output whose snapshot obligation has not
/// completed. So the driver has nothing to do with a staged chunk, and in
/// particular must not put one on a link.
#[test]
fn a_staged_chunk_is_the_runtimes_obligation_and_reaches_no_transport() {
    let (driver, transport) = snapshot_driver();

    driver.tick().expect("the tick reports success");

    assert!(
        transport
            .observed()
            .iter()
            .all(|envelope| envelope.from == NodeId(1) && envelope.to == NodeId(2)),
        "no frame was invented for the staged chunk"
    );
    assert_eq!(
        snapshot_frames(&transport),
        1,
        "exactly the send directive became a frame, not the staged one"
    );
    assert!(driver
        .with_group(|group| matches!(group.fatal_state(), GroupFatalState::Healthy))
        .expect("the driver still holds its group"));
}

/// A runtime whose committed membership shrinks on the first tick.
///
/// A committed removal is the only fact that licenses fencing, and
/// `DurableRaftNode` over a static config never produces one, so the narrowing
/// half of the membership contract needs a runtime that does.
#[derive(Clone, Debug, Eq, PartialEq)]
struct ShrinkingMembershipRuntime {
    committed: Vec<u64>,
    ticked: bool,
}

impl ShrinkingMembershipRuntime {
    fn new() -> Self {
        Self {
            committed: vec![1, 2, 3],
            ticked: false,
        }
    }

    fn config(&self) -> MembershipConfig {
        let voters = self
            .committed
            .iter()
            .copied()
            .map(NodeId)
            .collect::<Vec<_>>();
        MembershipConfig::stable(
            MembershipSet::new(voters, Vec::new()).expect("scripted membership is valid"),
        )
    }
}

impl PersistedRaftRuntime for ShrinkingMembershipRuntime {
    type Error = rafter_runtime::RaftRuntimeError;

    fn id(&self) -> NodeId {
        NodeId(1)
    }
    fn leader_hint(&self) -> Option<NodeId> {
        Some(NodeId(1))
    }
    fn role(&self) -> Role {
        Role::Leader
    }
    fn current_term(&self) -> Term {
        Term(1)
    }
    fn commit_index(&self) -> LogIndex {
        LogIndex(5)
    }
    fn last_log_index(&self) -> LogIndex {
        LogIndex(5)
    }
    fn snapshot_index(&self) -> LogIndex {
        LogIndex(0)
    }
    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        std::cmp::min(index, LogIndex(5))
    }
    fn membership(&self) -> MembershipConfig {
        self.config()
    }
    fn committed_membership(&self) -> MembershipConfig {
        self.config()
    }
    fn replication(&self) -> Vec<ReplicationProgress> {
        Vec::new()
    }
    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        (index <= LogIndex(5)).then_some(Term(1))
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error> {
        if matches!(input, RaftInput::Tick) && !self.ticked {
            self.ticked = true;
            // Node 3 leaves by a committed configuration change.
            self.committed = vec![1, 2];
        }
        Ok(Vec::new())
    }

    fn step_proposal_batch(
        &mut self,
        _proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, Self::Error> {
        Ok(Vec::new())
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, Self::Error> {
        let mut outputs = Vec::new();
        for input in inputs {
            outputs.extend(self.step(input)?);
        }
        Ok(outputs)
    }
}

type ShrinkDriver =
    TransportRaftDriver<u64, KvStateMachine, ShrinkingMembershipRuntime, QueueTransport, Validator>;

fn shrink_driver(nameable: Option<BTreeSet<NodeId>>) -> (ShrinkDriver, QueueTransport) {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: BTreeSet::from([NodeId(2), NodeId(3)]),
        nameable,
    };
    let driver = TransportRaftDriver::new(
        RaftGroup::new(
            GROUP,
            NodeId(1),
            ShrinkingMembershipRuntime::new(),
            KvStateMachine::default(),
        ),
        Vec::new(),
        transport.clone(),
        validator,
        TransportDriverOptions::default(),
    )
    .expect("a quiescent group is adoptable");
    (driver, transport)
}

/// The transport's peer set is the link layer's copy of who may speak, and this
/// is the whole contract for keeping it level with the group.
///
/// Three of the four clauses run here: the set is published at construction —
/// a driver that published only on change left it undefined for the whole first
/// incarnation, which is where a group whose membership never changes stays
/// forever — narrowed on a committed change, and the replica the committed
/// change removed is fenced. The fourth, widening on an uncommitted `Appended`,
/// has no public entry point on this driver and is pinned in-crate beside the
/// router; see the test module in `driver/transport/state.rs`.
#[test]
fn membership_reaches_the_transports_peer_set() {
    let (driver, transport) = shrink_driver(None);

    assert_eq!(
        transport.peer_sets(),
        vec![vec![
            Principal::for_node(NodeId(2)),
            Principal::for_node(NodeId(3))
        ]],
        "the group's membership reached the link layer at construction, without \
         the local node"
    );
    assert!(
        !transport.is_fenced(NodeId(3)),
        "construction removes nobody, so it fences nobody"
    );

    driver.tick().expect("the tick advances the protocol");

    assert_eq!(
        transport.peer_sets(),
        vec![
            vec![
                Principal::for_node(NodeId(2)),
                Principal::for_node(NodeId(3))
            ],
            vec![Principal::for_node(NodeId(2))],
        ],
        "the committed change narrowed the published set"
    );
    assert!(
        transport.is_fenced(NodeId(3)),
        "a committed removal is the fact that licenses fencing"
    );
    assert!(
        !transport.is_fenced(NodeId(2)),
        "node 2 is still a member and must still be able to speak"
    );
    assert_eq!(driver.refused_peer_updates(), 0);
}

/// A membership this deployment cannot fully name is not published at all: a
/// partial peer set authorizes fewer replicas than the cluster has, which is a
/// quorum-splitting configuration change made by accident.
#[test]
fn a_membership_the_validator_cannot_name_is_not_published() {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: BTreeSet::from([NodeId(2), NodeId(3)]),
        nameable: Some(BTreeSet::from([NodeId(1), NodeId(2)])),
    };
    let driver: Driver = TransportRaftDriver::new(
        numbered_group(GROUP, 1, &[2, 3], 3),
        Vec::new(),
        transport.clone(),
        validator,
        TransportDriverOptions::default(),
    )
    .expect("a quiescent group is adoptable");

    assert!(
        transport.peer_sets().is_empty(),
        "a partial peer set is worse than a stale one, so none was published"
    );
    assert_eq!(driver.refused_peer_updates(), 1);
    assert_eq!(
        driver.refused_sends(),
        0,
        "a peer-set refusal is not a dropped frame"
    );
}

/// The all-or-nothing rule governs the peer set and stops there. Withholding a
/// peer set leaves the previous one in place, which is merely stale; withholding
/// a fence the same event licensed leaves a committed-removed replica able to
/// speak, forever — the driver's own record of the membership has already moved
/// past the removal, so no later event can re-derive it.
#[test]
fn a_committed_removal_is_fenced_even_when_the_peer_set_cannot_be_published() {
    // Node 2 cannot be named; node 3, the one the change removes, can.
    let (driver, transport) = shrink_driver(Some(BTreeSet::from([NodeId(1), NodeId(3)])));

    driver.tick().expect("the tick advances the protocol");

    assert!(
        transport.is_fenced(NodeId(3)),
        "node 3 was removed by a committed configuration change and must be fenced"
    );
    assert!(
        transport.peer_sets().is_empty(),
        "the peer set is still all-or-nothing, and node 2 has no principal"
    );
    assert_eq!(
        driver.refused_peer_updates(),
        2,
        "one publication withheld at construction and one at the committed change"
    );
}

/// The consequence at the boundary a consumer sees. `update_peers` is the
/// admission control in the one consumer that adopted this driver, so a fence
/// dropped alongside a withheld peer set is both controls missing at once.
#[test]
fn a_removed_replica_cannot_speak_after_the_publication_was_refused() {
    let (driver, _transport) = shrink_driver(Some(BTreeSet::from([NodeId(1), NodeId(3)])));

    driver.tick().expect("the tick advances the protocol");

    let refused = driver.deliver(AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(NodeId(3)),
        raft_from: NodeId(3),
        raft_to: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(3),
            last_log_index: LogIndex(5),
            last_log_term: Term(1),
        }),
    });

    assert!(
        matches!(refused, Err(InboundEnvelopeError::Rejected { .. })),
        "a committed-removed replica must not be able to speak, got {refused:?}"
    );
}

/// Fencing is per replica rather than all-or-nothing, so an unnameable removal
/// costs only its own fence — and is counted, because the link layer is behind
/// the group and nothing repairs a peer set on its own.
#[test]
fn an_unfenceable_removal_is_counted_rather_than_silent() {
    // The removed replica is the one with no principal; node 2 has one.
    let (driver, transport) = shrink_driver(Some(BTreeSet::from([NodeId(1), NodeId(2)])));

    driver.tick().expect("the tick advances the protocol");

    assert!(!transport.is_fenced(NodeId(3)));
    assert_eq!(
        transport.peer_sets(),
        vec![vec![Principal::for_node(NodeId(2))]],
        "the committed set is nameable in full, so it published"
    );
    assert_eq!(
        driver.refused_peer_updates(),
        2,
        "construction could not name node 3, and the removal could not fence it"
    );
}
