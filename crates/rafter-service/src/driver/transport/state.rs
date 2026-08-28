#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! The fields one transport driver holds, and the small shapes its steps speak
//! in.
//!
//! Everything here is private to the driver. It is a separate file because
//! the public surface and the mechanism behind it are read for different
//! reasons: one is a contract, the other is a loop. The loop itself lives in
//! [`super::step`], where one step's report goes in [`super::dispatch`], and what
//! finishes a read barrier in [`super::barrier`]; the lock all three run under is
//! [`super::shared`]. The waiter tables the loop resolves live in
//! [`super::waiters`], split the same way and for
//! the same reason: this file declares what a driver is, those answer what
//! happens to the client. The third answer — who may send a step's input
//! at all — is [`super::control_plane`] and the files beneath it, which own
//! every rule over the membership and policy fields declared below. This file
//! declares them and never derives anything from them.
//!
//! The fourth answer — what condition this driver is *in* — is
//! [`super::condition`], split off for the same reason again: a supervisor
//! polling a driver's standing and a reader following a step are reading for
//! different things, and the two types that answer the first hold no state and
//! read none.

use std::collections::BTreeSet;

use super::super::*;
use super::checkpoint::CurrentCommittedState;
use super::condition::Contradiction;
use super::reconciliation::StagedMembership;
use super::waiters::{ReadWaiter, WriteWaiter};
use super::TransportDriverOptions;

pub(super) use super::shared::{DriverShared, SharedState};

/// Why one group step did not run, or did not finish.
///
/// The group error travels unwrapped so each caller can report it in its own
/// vocabulary: a driver hears `ManagedDriverError`, and a client hears the
/// typed write or read category the same mapping gives `InMemoryRaftDriver`.
pub(super) enum StepFailure<E, RE> {
    NoGroup,
    Group(GroupError<E, RE>),
}

/// One authorization policy in this driver's own terms.
///
/// `NodeId`s rather than principals, because this is what a *comparison* needs:
/// the driver decides whether the link layer is level by comparing the policy it
/// would publish against the one the transport accepted, and a principal is
/// stable for the lifetime of its ID — see
/// [`crate::transport::AuthenticatedPeerValidator::principal_for_node`] — so the
/// identities decide the answer and the principals are derived at publication.
///
/// The two halves travel together because they are published together. A floor
/// that moved without the set, or a set that moved without the floor, is still a
/// policy the transport does not hold.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DesiredPeerPolicy {
    pub(super) peers: BTreeSet<NodeId>,
    pub(super) retirement_floor: Option<NodeId>,
}

/// Which waiter a dropped client future reclaims.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum WaiterId {
    Write(LocalProposalId),
    Read(ReadId),
}

/// What starting a read produced.
///
/// Two shapes because the driver serves two consistency levels and only one of
/// them waits. A linearizable read reserves a barrier that some later step
/// resolves, so starting it yields a name; a local read is answered by the call
/// that starts it, so starting it yields the answer. Collapsing the two into an
/// `Option<ReadId>` would leave the caller of a local read holding a `None` and
/// still needing somewhere to put the receipt.
pub(super) enum StartedRead<G, QR> {
    /// A barrier was reserved under this ID and its waiter is registered.
    Barrier(ReadId),
    /// The read was answered inside the call that started it. No waiter exists,
    /// no [`ReadId`] was allocated, and there is nothing to abandon.
    Answered(Result<QueryReceipt<G, QR>, ReadError>),
}

pub(super) struct TransportDriverState<G, A, R, T, V>
where
    A: ReplicatedStateMachine,
{
    pub(super) group_id: G,
    pub(super) node_id: NodeId,
    pub(super) group: Option<RaftGroup<G, A, R>>,
    pub(super) transport: T,
    pub(super) validator: V,
    pub(super) options: TransportDriverOptions,
    pub(super) metrics: MetricsPublisher<G>,
    pub(super) next_proposal_id: Option<u64>,
    pub(super) next_read_id: Option<u64>,
    pub(super) write_waiters: BTreeMap<LocalProposalId, WriteWaiter<A::CommandResult>>,
    pub(super) read_waiters: BTreeMap<ReadId, ReadWaiter<G, A::Query, A::QueryResult>>,
    pub(super) refused_sends: u64,
    pub(super) refused_peer_updates: u64,
    pub(super) refused_non_member_frames: u64,
    /// The configuration this replica is operating under, as last reported.
    ///
    /// **Assigned, never merged.** One of the two membership facts this driver
    /// tracks, and it is the one that can move in both directions: a
    /// configuration that appended and did not commit can be truncated back off
    /// the log by a new leader, and a driver holding a set that only ever grew
    /// could not express that at all — the replica an overwritten configuration
    /// named would stay authorized for the life of the incarnation, because
    /// nothing would ever commit its removal.
    ///
    /// It is a widening input and never a narrowing one: the peer set and the
    /// inbound check take it in union with `committed_members` and the register,
    /// so this alone can add authorization and never take any away.
    pub(super) effective_members: BTreeSet<NodeId>,
    /// The configuration the cluster has committed, as this replica's own
    /// runtime last reported it.
    ///
    /// The other fact, assigned from its own stream for the same reason. It is
    /// the only one that licenses narrowing what the peer set draws from the
    /// runtime, and one of the two the local replica's own service state is read
    /// from.
    ///
    /// Raw, exactly as the cluster reported it, including an identity a
    /// committed removal already spent — see `current_committed`, which is the
    /// part of it this driver can still honor. Keeping the raw fact is what
    /// makes a contract violation *nameable*: `readmitted_retired_peers` counts
    /// spent identities this replica's runtime names again, and a set that had
    /// quietly filtered them out would have nothing to count.
    ///
    /// **It is assigned from every committed fact this driver folds, whatever
    /// position that fact stands at, and that is deliberate.** It answers "what
    /// does this replica's own stream say the cluster has committed now", which
    /// no position has an opinion about; the positioned question is
    /// `current_committed`'s, and the two are separate fields because they are
    /// separate questions. An earlier revision claimed the two moved together,
    /// which was never what the code did and would have left this floor stale
    /// across a second recovery from one checkpoint.
    pub(super) committed_members: BTreeSet<NodeId>,
    /// The committed membership this driver believes is current, and where it
    /// observed it.
    ///
    /// **A versioned register, and the version is what makes it a register
    /// rather than a set.** Two honest records can disagree about the current
    /// membership without either being damaged — one simply looked later — and
    /// the only thing that decides between them is where each looked. A
    /// grow-only union of the two answers "who is a member now" with the union
    /// of two different nows, which keeps a replica that a committed removal
    /// took out at whichever position the other record had not reached.
    ///
    /// Its membership is the *live* one: the observation less every identity a
    /// committed removal has spent. Equal to `committed_members` on every
    /// cluster that keeps the single-use contract. A committed fact naming an
    /// already-spent identity is not a fact about who may speak — the retirement
    /// floor never falls, so the identity stays refused — and admitting it here
    /// would un-spend the ID and re-authorize a replica every correct link layer
    /// goes on refusing.
    ///
    /// **It is also an authorization input in its own right**, and not only the
    /// spent test's other half. A record legitimately stands ahead of a rebuilt
    /// runtime, so the replicas this names are ones the cluster's committed
    /// history calls members even while `committed_members` has not caught up;
    /// deriving the peer set without it published a floor that retired them. See
    /// [`super::policy`].
    ///
    /// With `committed_id_high_water` it is the whole of retirement: one
    /// scalar, one position, and a bounded set the size of the cluster.
    pub(super) current_committed: Option<CurrentCommittedState>,
    /// The greatest `NodeId` this driver has ever seen in a committed
    /// configuration.
    ///
    /// The whole of the retirement record, in one word. A `(group, NodeId)` is
    /// spent by a committed removal, and enumerating spent IDs needs a set that
    /// grows with every removal the group ever makes — unbounded state under a
    /// retention policy nobody wrote. Under monotonic allocation it is also
    /// unnecessary: every ID ever committed is at or below this mark, so an ID
    /// at or below it that the live committed configuration does not name is
    /// exactly an ID that has been spent.
    ///
    /// `None` before the first committed fact, and that is not the same as
    /// zero: with no committed configuration observed, nothing has been spent
    /// and no ID is refusable. `NodeId(0)` is a legal identity like any other,
    /// so it cannot stand in for "no mark".
    ///
    /// The consequence a deployment must hear is that allocation gaps below the
    /// mark are unallocatable: "fresh" means *greater than anything this group
    /// has ever committed*, not merely unused. See [`rafter::NodeId`], which
    /// states the contract this reads.
    pub(super) committed_id_high_water: Option<NodeId>,
    /// The policy this driver's transport last *accepted*, or `None` before it
    /// has accepted one.
    ///
    /// Tracking what the group says and tracking what the link layer took are
    /// two different facts, and a driver that keeps only the first cannot tell a
    /// published policy from a refused one. This is the second, and the
    /// difference between it and the policy derived from the membership facts is
    /// the whole of the control plane's outstanding work — there is no separate
    /// obligation ledger beside it any more, because retirement is a floor and a
    /// floor is re-derived from the mark on every attempt.
    ///
    /// `None` rather than an empty policy for "nothing accepted yet", because an
    /// empty peer set is a real and publishable statement — a single-voter group
    /// authorizes no peers — and a driver that could not tell the two apart
    /// would skip the first publication of exactly that group.
    pub(super) published_policy: Option<DesiredPeerPolicy>,
    /// The contradiction this driver's licensing inputs proved, if they have.
    ///
    /// Terminal for the incarnation, and deliberately not a counter. A
    /// contradiction means the facts that license this driver's permanent
    /// statement about who is retired disagree at one log position, so there is
    /// no correct policy to publish and no later fact that makes one appear: the
    /// committed membership at one index is one set, and this driver has been
    /// told two. What it does instead is keep stepping — a replica in this state
    /// is still a useful follower and still has to be able to catch up — while
    /// refusing client work, publishing nothing, and **freezing the two
    /// checkpointable fields beside it**.
    ///
    /// The freeze is the half that took three rounds to arrive, and without it
    /// the state is not terminal at all: ordinary reconciliation kept folding
    /// later batches into the mark and the register and kept advancing the epoch,
    /// so the embedder persisted a newer record carrying no trace of the fork, and
    /// a restart from it started clean. See
    /// [`TransportDriverState::route_membership_events`] for the guard and
    /// [`PeerControlPlaneCheckpoint::contradicted_at`] for the durable marker
    /// that makes the freeze survive the process.
    ///
    /// The supervisor's move is [`TransportRaftDriver::release_group`] and a
    /// deliberate reseed, which is why this is reported through
    /// [`DriverServiceState`] rather than raised at whichever client call
    /// happened to be next.
    pub(super) contradiction: Option<Contradiction>,
    /// The construction-wide membership transaction, while one is open.
    ///
    /// `Some` only between
    /// [`TransportDriverState::open_membership_transaction`] and
    /// [`TransportDriverState::commit_membership_transaction`], which is inside
    /// one constructor call and reachable by no other entry point. While it is
    /// open, membership routing folds into it and installs nothing — so the
    /// record, the replay's crossings, and the runtime's endpoint reach the link
    /// layer as one statement or not at all.
    ///
    /// A live report needs none of this: its batch is folded and installed inside
    /// the call that routes it, so its candidate is a local. Construction's
    /// inputs are separated by the group's own stepping machinery, which is the
    /// only reason a candidate has to live on the driver at all.
    pub(super) staged_membership: Option<StagedMembership>,
    /// How many times the checkpointable control-plane state has changed.
    ///
    /// The change signal an embedder persists against. Eq over the checkpoint
    /// itself would work and costs a set comparison per poll; this costs a
    /// `u64` load and cannot report equal for two different states. Monotone and
    /// **instance-local**: a fresh driver starts at zero whatever it restored,
    /// so a caller records the epoch it last persisted *for this driver* and
    /// compares against that.
    pub(super) checkpoint_epoch: u64,
    pub(super) shutting_down: bool,
}
