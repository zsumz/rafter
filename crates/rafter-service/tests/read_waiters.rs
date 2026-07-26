//! One barrier's fault, and what the rest of the pass does about it.
//!
//! Every scenario here comes from the adversarial review of
//! `TransportRaftDriver`, kept with its fixture and inverted where the review
//! recorded a defect. The runtime is the shape every real linearizable read
//! has and the shipped scripted fakes do not: a barrier is `Pending` when
//! `begin_read` returns and granted by a later step.

#![allow(clippy::wildcard_imports, dead_code)]

mod support;

use std::{
    collections::BTreeSet,
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

use rafter_runtime_api::PersistedRaftRuntime;
use rafter_service::{
    AuthenticatedPeerValidator, PeerEnvelope, PeerSet, RaftTransport, SnapshotChunkEnvelope,
    TransportDriverOptions, TransportRaftDriver,
};
use support::*;

const GROUP: u64 = 7;

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Principal(String);

#[derive(Debug)]
struct TransportError;

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("transport error")
    }
}

impl Error for TransportError {}

#[derive(Clone, Default)]
struct NullTransport;

impl RaftTransport<u64> for NullTransport {
    type PeerPrincipal = Principal;
    type Error = TransportError;

    fn send(&self, _envelope: PeerEnvelope<u64>) -> Result<(), Self::Error> {
        Ok(())
    }
    fn update_peers(
        &self,
        _group_id: &u64,
        _peers: PeerSet<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }
    fn fence_peer(&self, _group_id: &u64, _peer: Self::PeerPrincipal) -> Result<(), Self::Error> {
        Ok(())
    }
    /// This fixture's runtime emits no snapshot directives, so a chunk arriving
    /// here would be a routing defect rather than a transfer.
    fn send_snapshot_chunk(
        &self,
        _envelope: SnapshotChunkEnvelope<u64>,
    ) -> Result<(), Self::Error> {
        Err(TransportError)
    }
}

#[derive(Clone)]
struct Validator;

impl AuthenticatedPeerValidator<u64, Principal> for Validator {
    fn is_known_group(&self, group_id: &u64) -> bool {
        *group_id == GROUP
    }
    fn node_for_authenticated_peer(&self, _group_id: &u64, _peer: &Principal) -> Option<NodeId> {
        Some(NodeId(2))
    }
    fn principal_for_node(&self, _group_id: &u64, node_id: NodeId) -> Option<Principal> {
        Some(Principal(format!("replica-{}", node_id.0)))
    }
    fn is_authorized_peer(&self, _group_id: &u64, _node_id: NodeId) -> bool {
        true
    }
    fn is_fenced_peer(&self, _group_id: &u64, _node_id: NodeId) -> bool {
        false
    }
}

// ---------------------------------------------------------------------------
// A runtime that defers every grant to the next tick, so a barrier is Pending
// when `begin_read` returns and Granted when a later step observes it. This is
// the shape every real linearizable read has; the shipped scripted fakes grant
// synchronously and never exercise the deferred path.
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum GrantShape {
    #[default]
    Grant,
    /// One tick emits the grant and the cancellation for the same barrier, in
    /// that order — the "granted then cancelled in one deliver batch" case.
    GrantThenCancel,
    /// The reverse order, which the routing loop sees just as often.
    CancelThenGrant,
}

#[derive(Clone, Debug, Default)]
struct DelayedGrantRuntime {
    registered: Arc<Mutex<BTreeSet<ReadId>>>,
    shape: GrantShape,
}

impl PersistedRaftRuntime for DelayedGrantRuntime {
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
        LogIndex(1)
    }
    fn last_log_index(&self) -> LogIndex {
        LogIndex(1)
    }
    fn snapshot_index(&self) -> LogIndex {
        LogIndex::ZERO
    }
    /// The post-election log a barrier lands on: a committed `Noop` with no
    /// application entry under it, so the floor is ZERO and a fresh state
    /// machine satisfies it.
    fn committed_application_index_through(&self, _index: LogIndex) -> LogIndex {
        LogIndex::ZERO
    }
    fn membership(&self) -> MembershipConfig {
        MembershipConfig::stable(
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("membership is valid"),
        )
    }
    /// Never mid-change, so the two memberships are one. Asserted rather than
    /// inherited: only this fixture can make that claim about itself.
    fn committed_membership(&self) -> MembershipConfig {
        self.membership()
    }
    fn replication(&self) -> Vec<ReplicationProgress> {
        Vec::new()
    }
    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        (index <= LogIndex(1)).then_some(Term(1))
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error> {
        match input {
            RaftInput::ReadIndex { read_id } => {
                self.registered
                    .lock()
                    .expect("registered reads lock")
                    .insert(read_id);
                Ok(Vec::new())
            }
            RaftInput::Tick => {
                let ids = std::mem::take(&mut *self.registered.lock().expect("registered lock"));
                let mut outputs = Vec::new();
                for read_id in ids {
                    let granted = RaftOutput::ReadIndexGranted {
                        read_id,
                        read_index: LogIndex(1),
                    };
                    let canceled = RaftOutput::ReadIndexCanceled {
                        read_id,
                        reason: ReadIndexCancelReason::LeaderStateReset,
                    };
                    match self.shape {
                        GrantShape::Grant => outputs.push(granted),
                        GrantShape::GrantThenCancel => {
                            outputs.push(granted);
                            outputs.push(canceled);
                        }
                        GrantShape::CancelThenGrant => {
                            outputs.push(canceled);
                            outputs.push(granted);
                        }
                    }
                }
                Ok(outputs)
            }
            _ => Ok(Vec::new()),
        }
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

/// A state machine whose query path fails. Nothing else about it fails, and it
/// never poisons the group: `read_state_machine` maps the failure to
/// `GroupError::StateMachine` without entering the poisoned state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct RefusingReadStateMachine;

#[derive(Debug)]
struct RefusedRead;

impl fmt::Display for RefusedRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("the state machine refused this query")
    }
}

impl Error for RefusedRead {}

impl ReplicatedStateMachine for RefusingReadStateMachine {
    type Command = String;
    type CommandResult = ();
    type Query = String;
    type QueryResult = ();
    type Error = RefusedRead;

    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Unsupported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(LogIndex::ZERO)
    }
    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        Ok(command.clone().into_bytes())
    }
    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        Ok(String::from_utf8_lossy(payload).into_owned())
    }
    fn apply_batch(
        &mut self,
        _batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        Ok(Vec::new())
    }
    fn read(&self, _query: Self::Query, _barrier: ReadBarrier) -> Result<(), Self::Error> {
        Err(RefusedRead)
    }
}

type RefusingDriver = TransportRaftDriver<
    u64,
    RefusingReadStateMachine,
    DelayedGrantRuntime,
    NullTransport,
    Validator,
>;

fn refusing_driver() -> RefusingDriver {
    driver_with(GrantShape::Grant, TransportDriverOptions::default())
}

fn driver_with(shape: GrantShape, options: TransportDriverOptions) -> RefusingDriver {
    let group = RaftGroup::new(
        GROUP,
        NodeId(1),
        DelayedGrantRuntime {
            registered: Arc::default(),
            shape,
        },
        RefusingReadStateMachine,
    );
    TransportRaftDriver::new(group, Vec::new(), NullTransport, Validator, options)
        .expect("a quiescent group is adoptable")
}

// ---------------------------------------------------------------------------
// Held-under-attack: one step that both grants and cancels the same barrier.
// ---------------------------------------------------------------------------

#[test]
fn held_a_barrier_granted_then_canceled_in_one_step_resolves_as_canceled() {
    for shape in [GrantShape::GrantThenCancel, GrantShape::CancelThenGrant] {
        let driver = driver_with(shape, TransportDriverOptions::default());
        let handle = driver.handle();
        let mut read = Box::pin(handle.read("a".to_owned(), ReadConsistency::Linearizable));
        assert!(poll_once(&mut read).is_none(), "{shape:?}: barrier pending");

        driver.tick().expect("the tick succeeds");
        driver
            .drive_pending_reads()
            .expect("{shape:?}: no barrier is left to collect");

        let outcome = poll_once(&mut read).expect("{shape:?}: the client resolves");
        assert!(
            matches!(outcome, Err(ReadError::Canceled { .. })),
            "{shape:?}: expected a cancellation, got {outcome:?}"
        );
        assert!(
            driver.pending_reads().is_empty(),
            "{shape:?}: no unresolved barrier remains"
        );
    }
}

/// The bound counts unresolved waiters. Abandon, resolve, and release must each
/// return exactly the slots they retired — no leak, no double credit.
#[test]
fn held_the_waiter_bound_accounts_exactly_across_abandon_and_release() {
    let driver = driver_with(
        GrantShape::Grant,
        TransportDriverOptions::default().with_max_pending_waiters(2),
    );
    let handle = driver.handle();

    let mut first = Box::pin(handle.read("a".to_owned(), ReadConsistency::Linearizable));
    let mut second = Box::pin(handle.read("b".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut first).is_none());
    assert!(poll_once(&mut second).is_none());
    assert_eq!(driver.pending_reads().len(), 2);

    // The bound is closed.
    let mut third = Box::pin(handle.read("c".to_owned(), ReadConsistency::Linearizable));
    let refused = poll_once(&mut third).expect("the third read is refused in place");
    assert!(
        matches!(refused, Err(ReadError::Transport { .. })),
        "{refused:?}"
    );

    // Abandoning one returns exactly one slot, and abandoning it again returns
    // none.
    let ids = driver.pending_reads();
    assert!(driver.abandon_read(ids[0]));
    assert!(
        !driver.abandon_read(ids[0]),
        "no second credit for one slot"
    );
    assert_eq!(driver.pending_reads().len(), 1);

    let mut fourth = Box::pin(handle.read("d".to_owned(), ReadConsistency::Linearizable));
    assert!(
        poll_once(&mut fourth).is_none(),
        "the freed slot admits exactly one more"
    );
    assert_eq!(driver.pending_reads().len(), 2);

    let mut fifth = Box::pin(handle.read("e".to_owned(), ReadConsistency::Linearizable));
    assert!(
        poll_once(&mut fifth).is_some(),
        "and no more than one: the bound is closed again"
    );

    // Release returns every slot at once, and re-adoption starts from a full
    // budget rather than a leaked one.
    let group = driver.release_group().expect("the driver holds a group");
    assert!(driver.pending_reads().is_empty());
    driver
        .adopt_group(group, Vec::new())
        .expect("re-adoption succeeds");

    let mut sixth = Box::pin(handle.read("f".to_owned(), ReadConsistency::Linearizable));
    let mut seventh = Box::pin(handle.read("g".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut sixth).is_none());
    assert!(poll_once(&mut seventh).is_none());
    assert_eq!(
        driver.pending_reads().len(),
        2,
        "the full budget came back, and no more than the full budget"
    );
}

/// Release with a write, an ungranted barrier, and a granted-but-uncollected
/// barrier all outstanding at once.
#[test]
fn held_release_drains_every_kind_of_in_flight_waiter_at_once() {
    let driver = driver_with(GrantShape::Grant, TransportDriverOptions::default());
    let handle = driver.handle();

    let mut granted = Box::pin(handle.read("a".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut granted).is_none());
    driver.tick().expect("the tick grants the first barrier");

    let mut ungranted = Box::pin(handle.read("b".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut ungranted).is_none());
    let mut write = Box::pin(handle.write("cmd".to_owned()));
    let _ = poll_once(&mut write);

    assert_eq!(driver.pending_reads().len(), 2);

    let group = driver.release_group().expect("the driver holds a group");
    assert!(driver.pending_reads().is_empty());
    assert!(driver.pending_writes().is_empty());

    for outcome in [
        poll_once(&mut granted).expect("the granted barrier's client resolves"),
        poll_once(&mut ungranted).expect("the ungranted barrier's client resolves"),
    ] {
        assert!(
            matches!(
                outcome,
                Err(ReadError::Abandoned {
                    reason: ReadAbandonReason::DriverReleased,
                    ..
                })
            ),
            "{outcome:?}"
        );
    }

    // The retired group is quiescent in reads, which is what makes it adoptable.
    assert_eq!(
        group.metrics().reserved_reads,
        0,
        "release cancelled every barrier through the group"
    );
    driver
        .adopt_group(group, Vec::new())
        .expect("a read-quiescent group is adoptable");
}

// ---------------------------------------------------------------------------
// A per-barrier fault is the barrier's own. It resolves that client, keeps its
// cause, and leaves the rest of the pass alone.
//
// The review found all three broken at once: the failure escaped
// `drive_pending_reads` as a driver error, every other ready barrier in the
// pass was skipped, and the client was eventually told that the *driver* had
// failed to route a terminal read event — which never happened, because the
// state machine refused the query.
// ---------------------------------------------------------------------------

#[test]
fn one_barriers_state_machine_failure_resolves_only_that_barrier() {
    let driver = refusing_driver();
    let handle = driver.handle();

    let mut first = Box::pin(handle.read("a".to_owned(), ReadConsistency::Linearizable));
    let mut second = Box::pin(handle.read("b".to_owned(), ReadConsistency::Linearizable));
    assert!(
        poll_once(&mut first).is_none(),
        "the first barrier is pending"
    );
    assert!(
        poll_once(&mut second).is_none(),
        "the second barrier is pending"
    );
    assert_eq!(driver.pending_reads().len(), 2);

    // The tick grants both barriers; the driver records both proofs as ready.
    driver.tick().expect("the granting tick succeeds");

    // One barrier's fault must not deny service to the rest, so the pass runs
    // to the end and reports nothing to the driver: nothing here is unrelated
    // to a single barrier.
    driver
        .drive_pending_reads()
        .expect("a per-barrier failure is not a driver failure");

    for (label, outcome) in [
        ("first", poll_once(&mut first)),
        ("second", poll_once(&mut second)),
    ] {
        let outcome = outcome.unwrap_or_else(|| panic!("{label}: the client resolved"));
        let error = outcome.expect_err("a refused query cannot produce a receipt");
        assert!(
            matches!(
                error,
                ReadError::StateMachine {
                    operation: StateMachineOperation::Read,
                    ..
                }
            ),
            "{label}: {error:?}"
        );
    }
    assert!(
        driver.pending_reads().is_empty(),
        "both barriers are accounted for"
    );
}

/// The failure keeps its own cause and names its own component.
///
/// The proof is consumed and dropped by the failing read — `read_linearizable`
/// removes it before running the state machine — so the barrier really is gone
/// from the group afterwards. That is a fact about this fixture, not a licence
/// to report a routing defect: nothing failed to route, and the error the
/// client receives says so and carries the state machine's own error under it.
#[test]
fn a_refused_query_is_reported_as_a_state_machine_failure() {
    let driver = refusing_driver();
    let handle = driver.handle();

    let mut read = Box::pin(handle.read("a".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut read).is_none(), "the barrier is pending");
    driver.tick().expect("the granting tick succeeds");

    driver
        .drive_pending_reads()
        .expect("the proof-consuming read resolves its own barrier");

    let outcome = poll_once(&mut read).expect("the client is resolved");
    let error = outcome.expect_err("a refused query cannot produce a receipt");

    let ReadError::StateMachine {
        operation, cause, ..
    } = &error
    else {
        panic!("expected a state-machine failure, got {error:?}");
    };
    assert_eq!(*operation, StateMachineOperation::Read);
    assert!(
        cause.downcast_ref::<RefusedRead>().is_some(),
        "the state machine's own error is preserved: {cause:?}"
    );

    // And a second pass has nothing left to do, rather than re-reserving the
    // spent `ReadId` and reporting an invariant violation for it.
    driver
        .drive_pending_reads()
        .expect("no barrier remains to collect");
}

// ---------------------------------------------------------------------------
// Held-under-attack probes. These PASS: they record orderings that behave.
// ---------------------------------------------------------------------------

/// A grant recorded on a waiter that abandonment already resolved must not
/// resurrect it into `drive_pending_reads`.
#[test]
fn held_a_grant_arriving_after_abandonment_resolves_nothing() {
    let driver = refusing_driver();
    let handle = driver.handle();
    let mut read = Box::pin(handle.read("a".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut read).is_none());

    let read_id = driver.pending_reads()[0];
    assert!(driver.abandon_read(read_id), "the waiter is retired");

    // The tick grants the (now cancelled) barrier: the group dropped it, so no
    // grant is emitted at all, and nothing resurrects the waiter.
    driver.tick().expect("the tick succeeds");
    driver
        .drive_pending_reads()
        .expect("no barrier is ready to collect");

    let outcome = poll_once(&mut read).expect("the abandoned client is resolved");
    assert!(
        matches!(
            outcome,
            Err(ReadError::Abandoned {
                reason: ReadAbandonReason::DriveBoundReached,
                ..
            })
        ),
        "abandonment stays the first outcome: {outcome:?}"
    );
    assert!(
        driver.pending_reads().is_empty(),
        "no unresolved barrier remains"
    );
}

/// `release_group` then `adopt_group` must never reissue an ID a stale future
/// still names.
#[test]
fn held_ids_do_not_regress_across_release_and_re_adoption() {
    let driver = refusing_driver();
    let handle = driver.handle();
    let mut stale = Box::pin(handle.read("a".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut stale).is_none());
    let stale_id = driver.pending_reads()[0];

    let released = driver.release_group().expect("the driver holds a group");
    drop(released);

    // A brand-new group whose watermarks restart at zero.
    let fresh = RaftGroup::new(
        GROUP,
        NodeId(1),
        DelayedGrantRuntime::default(),
        RefusingReadStateMachine,
    );
    driver
        .adopt_group(fresh, Vec::new())
        .expect("a fresh group is adoptable");

    let mut next = Box::pin(handle.read("b".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut next).is_none());
    let next_id = driver.pending_reads()[0];

    assert!(
        next_id > stale_id,
        "the re-adopted driver reissued {next_id} at or below the stale {stale_id}"
    );

    // The stale future still answers for its own release.
    let outcome = poll_once(&mut stale).expect("the stale client resolved at release");
    assert!(
        matches!(
            outcome,
            Err(ReadError::Abandoned {
                reason: ReadAbandonReason::DriverReleased,
                ..
            })
        ),
        "{outcome:?}"
    );
}

/// A runtime that emits no proposal lifecycle event told the app layer nothing
/// about whether the entry was appended, and `GroupError::ProposalDidNotStart`
/// is the app layer saying exactly that. The driver used to call it
/// `WriteFate::NotAppended` — "it cannot commit, now or later" — which is a
/// claim nobody made.
#[test]
fn a_write_with_no_lifecycle_event_is_unresolved_rather_than_refused() {
    let driver = refusing_driver();
    let handle = driver.handle();
    let mut write = Box::pin(handle.write("cmd".to_owned()));
    let outcome = poll_once(&mut write).expect("the failing step resolves in place");
    let error = outcome.expect_err("no lifecycle event means no receipt");

    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::RuntimeDroppedProposal,
                ..
            }
        ),
        "got {error:?}"
    );
    assert!(
        error.fate().may_commit(),
        "an unobserved proposal may still commit"
    );
}

/// Abandonment ordering that does hold: abandoning an already-resolved waiter,
/// and abandoning one whose future already polled it out, are both no-ops.
#[test]
fn held_abandoning_a_resolved_or_polled_write_is_a_no_op() {
    let driver = refusing_driver();
    let handle = driver.handle();
    let mut write = Box::pin(handle.write("cmd".to_owned()));

    // The waiter resolved inside `begin_write`, before anything polled it.
    assert!(
        driver.pending_writes().is_empty(),
        "a resolved waiter stops counting immediately"
    );
    assert!(
        !driver.abandon_write(LocalProposalId(1)),
        "abandoning a resolved waiter retires nothing"
    );

    let first = poll_once(&mut write).expect("the client is resolved");
    assert!(first.is_err());

    assert!(
        !driver.abandon_write(LocalProposalId(1)),
        "abandoning a polled-out waiter retires nothing"
    );
    assert!(
        !driver.abandon_write(LocalProposalId(9_999)),
        "abandoning an ID this driver never issued retires nothing"
    );
}

/// Shutdown is terminal, and adoption does not walk it back.
///
/// The review found `shutdown` → `release_group` → `adopt_group` producing a
/// driver that served again, which made the entry's own distinction — a
/// supervisor restarting a replica releases, a supervisor stopping one shuts
/// down and then releases — a distinction with no consequence.
#[test]
fn adoption_does_not_reverse_a_completed_shutdown() {
    let driver = refusing_driver();
    let handle = driver.handle();
    block_on(handle.shutdown()).expect("the driver shuts down");
    assert!(
        block_on(handle.shutdown()).is_err(),
        "shutdown is once-only"
    );

    let group = driver.release_group().expect("the group comes back");
    let error = driver
        .adopt_group(group, Vec::new())
        .expect_err("a shut-down driver adopts nothing");
    assert!(
        matches!(error, ManagedDriverError::ShuttingDown),
        "got {error:?}"
    );

    let mut read = Box::pin(handle.read("a".to_owned(), ReadConsistency::Linearizable));
    let outcome = poll_once(&mut read).expect("a shut-down driver refuses in place");
    assert!(
        matches!(outcome, Err(ReadError::ShuttingDown)),
        "got {outcome:?}"
    );
}
