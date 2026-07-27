//! The report streams a driver owes its transport: snapshot chunks and the
//! peer set.
//!
//! `SendSnapshotChunk` is the leader's only snapshot-send path and the peer set
//! is the link layer's copy of who may speak, so both scenarios here are about
//! effects that leave the process.
//!
//! What a `(group, NodeId)` pair *becomes* — retirement, adoption, and the
//! lifecycle of the local replica's own identity — is the neighbouring
//! `transport_identity.rs`, and what reaches this driver through the membership
//! event stream is `transport_membership.rs`. All three drive the same fixtures
//! from `support::scripted`.

#![allow(clippy::wildcard_imports)]

mod support;

use std::collections::BTreeSet;

use rafter::RaftSnapshot;
use rafter_app::group::GroupFatalState;
use rafter_runtime_api::PersistedRaftRuntime;
use rafter_service::{
    AuthenticatedPeerEnvelope, InboundEnvelopeError, TransportDriverOptions, TransportRaftDriver,
};
use support::scripted::*;
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
    /// This runtime scripts snapshot outputs and no configuration change, so it
    /// is never mid-change. Asserted rather than inherited: only this fixture
    /// can make that claim about itself.
    fn committed_membership(&self) -> MembershipConfig {
        self.membership()
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
        nameable: Nameable::all(),
    };
    let driver = TransportRaftDriver::new(
        RaftGroup::new(
            GROUP,
            NodeId(1),
            SnapshotEmittingRuntime { emitted: false },
            // The runtime reports a snapshot boundary at 5, so its own state
            // machine is at 5 too: a leader compacts at its applied index, and
            // a group whose state machine sits below its boundary now refuses
            // to run rather than answering from state that is missing entries.
            KvStateMachine {
                applied_index: LogIndex(5),
                ..KvStateMachine::default()
            },
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
        nameable: Nameable::all(),
    };
    let driver: SnapshotDriver = TransportRaftDriver::new(
        RaftGroup::new(
            GROUP,
            NodeId(1),
            SnapshotEmittingRuntime { emitted: false },
            // The runtime reports a snapshot boundary at 5, so its own state
            // machine is at 5 too: a leader compacts at its applied index, and
            // a group whose state machine sits below its boundary now refuses
            // to run rather than answering from state that is missing entries.
            KvStateMachine {
                applied_index: LogIndex(5),
                ..KvStateMachine::default()
            },
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

/// The transport's peer set is the link layer's copy of who may speak, and this
/// is the whole contract for keeping it level with the group.
///
/// Four of the five clauses run here: the set is published at adoption — a
/// driver that published only on change left it undefined for the whole first
/// incarnation, which is where a group whose membership never changes stays
/// forever — narrowed on a committed change, the replica the committed change
/// removed is fenced, and no replica the committed membership still names is
/// fenced with it. The fifth, widening on an uncommitted `Appended`, has no
/// public entry point on this driver and is pinned in-crate beside the router;
/// see the test module in `driver/transport/control_plane.rs`.
#[test]
fn membership_reaches_the_transports_peer_set() {
    let (driver, transport) = shrink_driver(Nameable::all());

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
        nameable: Nameable::only(&[NodeId(1), NodeId(2)]),
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
/// speak until something retries it.
///
/// The two halves fail independently, which is the clause under test: the peer
/// set here can never be published — node 2 has no principal — and the fence
/// lands anyway.
#[test]
fn a_committed_removal_is_fenced_even_when_the_peer_set_cannot_be_published() {
    // Node 2 cannot be named; node 3, the one the change removes, can.
    let (driver, transport) = shrink_driver(Nameable::only(&[NodeId(1), NodeId(3)]));

    driver.tick().expect("the tick advances the protocol");

    assert!(
        transport.is_fenced(NodeId(3)),
        "node 3 was removed by a committed configuration change and must be fenced"
    );
    assert_eq!(
        driver.pending_peer_fences(),
        0,
        "so nothing is owed on the fence half"
    );
    assert!(
        transport.peer_sets().is_empty(),
        "the peer set is still all-or-nothing, and node 2 has no principal"
    );
    assert!(
        driver.peer_set_is_stale(),
        "and the peer-set half is still owed, which is the state a cumulative \
         refusal count cannot express"
    );
    assert!(
        driver.refused_peer_updates() > 0,
        "every withheld publication is counted; the count is a history of \
         attempts rather than a measure of what is outstanding"
    );
}

/// The consequence at the boundary a consumer sees. `update_peers` is the
/// admission control in the one consumer that adopted this driver, so a fence
/// dropped alongside a withheld peer set is both controls missing at once.
#[test]
fn a_removed_replica_cannot_speak_after_the_publication_was_refused() {
    let (driver, _transport) = shrink_driver(Nameable::only(&[NodeId(1), NodeId(3)]));

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
/// costs only its own fence — and the fence it could not make stays *owed*,
/// because a directory that cannot name a replica today can name it tomorrow.
///
/// This replaces a test that accepted the same situation as a counted condition
/// and asserted nothing further. The count was true and insufficient: a
/// committed removal the link layer never heard about is an authorization the
/// cluster retracted and the transport still honours, and a cumulative counter
/// records that it happened once while saying nothing about whether it is still
/// happening.
#[test]
fn an_unfenceable_removal_stays_owed_until_the_directory_can_name_it() {
    // The removed replica is the one with no principal; node 2 has one.
    let nameable = Nameable::only(&[NodeId(1), NodeId(2)]);
    let (driver, transport) = shrink_driver(nameable.clone());

    driver.tick().expect("the tick advances the protocol");

    assert!(
        !transport.is_fenced(NodeId(3)),
        "the fence could not be made: this deployment cannot name node 3"
    );
    assert_eq!(
        driver.pending_peer_fences(),
        1,
        "so it is owed, which is the state the old contract discarded"
    );
    assert_eq!(
        transport.peer_sets(),
        vec![vec![Principal::for_node(NodeId(2))]],
        "the committed set is nameable in full, so it published"
    );

    // The directory learns node 3's identity, and nothing about the membership
    // changes: no event, no configuration change, just a later entry point.
    nameable.learn(NodeId(3));
    driver.tick().expect("the tick advances the protocol");

    assert!(
        transport.is_fenced(NodeId(3)),
        "the committed removal was still owed, so the first entry point that \
         could name node 3 discharged it"
    );
    assert_eq!(driver.pending_peer_fences(), 0, "and nothing is owed now");
}

// ---------------------------------------------------------------------------
// The control plane is retried, because neither half of it repairs itself.
//
// `update_peers` and `fence_peer` are both allowed to fail, and the driver is
// the only thing that can try again: no later membership event re-derives a
// removal the cluster has already committed, because the driver's record of
// what the group says has moved past it.
// ---------------------------------------------------------------------------

/// A refused peer-set publication is retried at the next entry point, with no
/// second membership event to prompt it.
///
/// The tick that retries carries no membership change — `Applied` fires only
/// when the committed configuration moves — so a driver that published on
/// events alone leaves the link layer's peer set behind the group's until some
/// unrelated later change happens to arrive.
#[test]
fn a_refused_peer_set_publication_is_retried_without_another_membership_event() {
    let (driver, transport) = shrink_driver(Nameable::all());
    transport.refuse_next_peer_updates(1);

    driver.tick().expect("the tick advances the protocol");

    assert_eq!(
        transport.peer_sets(),
        vec![vec![
            Principal::for_node(NodeId(2)),
            Principal::for_node(NodeId(3))
        ]],
        "the committed change's publication was refused, so the link layer is \
         still holding the set from construction"
    );
    assert!(
        driver.peer_set_is_stale(),
        "and the driver reports the link layer as behind while that is true"
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
        "the narrowed set reached the link layer on retry, without a second \
         membership event to re-derive it"
    );
    assert!(
        !driver.peer_set_is_stale(),
        "and stops reporting it once the publication is accepted"
    );
}

/// A refused fence is retried until the link accepts it.
///
/// This is the half that cannot be left behind. A stale peer set authorizes a
/// replica the cluster still names; a missing fence authorizes one it has
/// committed the removal of, and `fence_peer` is documented as the operation
/// that stops later frames from that replica.
#[test]
fn a_refused_fence_is_retried_until_the_link_accepts_it() {
    let (driver, transport) = shrink_driver(Nameable::all());
    transport.refuse_next_fences(NodeId(3), 1);

    driver.tick().expect("the tick advances the protocol");

    assert!(
        !transport.is_fenced(NodeId(3)),
        "the link refused the first fence"
    );
    assert_eq!(
        transport.fence_attempts(),
        vec![NodeId(3)],
        "and the driver asked exactly once"
    );
    assert_eq!(
        driver.pending_peer_fences(),
        1,
        "the window is visible while it is open: one admission control is owed"
    );

    driver.tick().expect("the tick advances the protocol");

    assert!(
        transport.is_fenced(NodeId(3)),
        "the committed removal was still owed, so the next entry point retried it"
    );
    assert_eq!(
        transport.fence_attempts(),
        vec![NodeId(3), NodeId(3)],
        "asked twice, accepted once"
    );
    assert_eq!(
        driver.pending_peer_fences(),
        0,
        "and the window closed when the link accepted"
    );
}

/// A replica whose committed removal has not been fenced yet cannot speak to
/// this driver, whatever the link layer believes.
///
/// The fail-closed backstop. `fence_peer` is the link layer's admission control
/// and it is allowed to fail, so the window between "the cluster committed the
/// removal" and "the transport accepted the fence" is a window in which the
/// validator still authorizes the removed replica. The driver knows better than
/// its own transport here — its membership is committed ∪ effective — so it
/// refuses the frame itself rather than treating a transient control-plane
/// failure as an authorization.
#[test]
fn a_removed_replica_cannot_speak_while_its_fence_is_pending() {
    let (driver, transport) = shrink_driver(Nameable::all());
    // The link refuses every fence for the length of this test.
    transport.refuse_next_fences(NodeId(3), 16);

    driver.tick().expect("the tick advances the protocol");

    assert!(
        !transport.is_fenced(NodeId(3)),
        "the fixture's whole point: the external fence never took"
    );
    assert_eq!(
        driver.pending_peer_fences(),
        1,
        "so the driver still owes it"
    );

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
        matches!(
            refused,
            Err(InboundEnvelopeError::NotInMembership { node_id: NodeId(3) })
        ),
        "a replica the committed membership no longer names must not be able to \
         vote here just because its fence has not landed, got {refused:?}"
    );
    assert_eq!(
        driver.refused_non_member_frames(),
        1,
        "and the refusal is observable rather than silent"
    );
}

/// A legitimate joiner is not caught by the fail-closed rule.
///
/// The rule refuses a sender the driver's membership does not name, and that
/// membership is committed ∪ effective by construction — so a replica added by
/// a change that has appended and not committed is present, and its frames are
/// accepted. Without this the backstop would stall every join: a joiner has to
/// be able to speak before the change commits, or it can never catch up and the
/// change can never commit.
#[test]
fn a_replica_added_by_an_in_flight_change_may_still_speak() {
    // Committed {1,2}; effective {1,2,3}, an addition of node 3 in flight.
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2]);
    let (driver, _transport) = scripted_driver(runtime, Nameable::all());

    let delivered = driver.deliver(AuthenticatedPeerEnvelope {
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
        delivered.is_ok(),
        "node 3 is joining under an uncommitted change and must be able to \
         speak, got {delivered:?}"
    );
}

/// Control-plane work still owed travels across a release and re-adoption.
///
/// The supervisor pattern this driver documents is release, rebuild the runtime
/// from durable storage, adopt, and the adoption re-derives nothing here: the
/// driver's record of the membership already moved past the removal on the
/// tick, so `known_members` and the committed membership agree and their
/// difference is empty. The fence is owed rather than derivable, which is
/// precisely why it has to be state the driver carries rather than a value it
/// recomputes.
#[test]
fn control_plane_work_still_owed_survives_release_and_adoption() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    change_on_step(&handle, &[1, 2], &[1, 2]);
    let (driver, transport) = scripted_driver(runtime, Nameable::all());
    transport.refuse_next_fences(NodeId(3), 1);

    driver.tick().expect("the tick advances the protocol");

    assert!(
        !transport.is_fenced(NodeId(3)),
        "the link refused the fence the committed removal licensed"
    );

    let group = driver.release_group().expect("the driver holds a group");

    assert_eq!(
        driver.pending_peer_fences(),
        1,
        "releasing the group retires an incarnation, not the link layer's \
         obligations: the same transport serves the next one"
    );

    driver
        .adopt_group(group, Vec::new())
        .expect("the released group is adoptable");

    assert!(
        transport.is_fenced(NodeId(3)),
        "the adoption discharged work the release did not cancel; \
         fence attempts = {:?}",
        transport.fence_attempts()
    );
    assert!(
        !transport.is_fenced(NodeId(2)),
        "node 2 is still committed and must still be able to speak"
    );
}
