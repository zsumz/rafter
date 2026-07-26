//! The lock service as three real processes: fencing, failover, restart, gating.
//!
//! **This suite is integration evidence only.** `docs/reference-consumers.md`
//! defines two process-composition levels, and this is the first: it proves
//! process boundaries, routing, kill and restart recovery, client session
//! deduplication across a real leader failure, that the readiness gate gates,
//! and that a fencing token issued by one process still fences after every
//! process in the cluster has been killed and restarted from its own durable
//! store. It proves nothing about production composition, because the link
//! between these processes authenticates nothing and the client protocol
//! authenticates nothing. No test here closes the 1.0 production-composition
//! criterion, and one of them exists to demonstrate that rather than let the
//! prose carry it alone.
//!
//! # Running it
//!
//! The suite is `#[ignore]`d by default and runs on request:
//!
//! ```text
//! scripts/reference-process-check
//! scripts/reference-source-check -- -p rafter-reference-fenced-lock \
//!     --test process_cluster -- --ignored
//! ```
//!
//! Ignoring it by default is the program document's own lane split rather than
//! a hedge: durable process tests belong to the main/nightly lane, while the
//! every-PR lane wants a package build and the deterministic suites.
//! `--all-targets` still compiles this file and the `lock-node` binary in both
//! dependency modes, so a consumer that stopped building is caught by every
//! lane; only the running of it is deferred.
//!
//! # What each test does about the kill window
//!
//! Every test that kills a process has to say what it assumes about *where* the
//! kill landed, because a real `SIGKILL` cannot be aimed. For this store there
//! are two windows, not one: where the kill landed relative to the commit
//! point, and whether it landed between a durable image and its seal — the
//! second of which decides whether the replica restarts at all. Each test says
//! which windows it is exposed to and asserts only what holds across them:
//!
//! - `three_processes_elect_a_leader_and_serve_the_lock` kills nothing.
//! - `a_session_retry_after_the_leader_is_killed_returns_the_cached_result`
//!   kills an idle leader: every command was acknowledged before the kill, so
//!   the commit window is empty and the retry must observe the cached result.
//!   The seal window is open, and the killed replica is never restarted, so it
//!   cannot affect the assertions.
//! - `a_killed_replica_recovers_its_durable_floor_and_rejoins` kills a follower
//!   mid-write. The survivors hold a quorum, so the write's outcome is settled
//!   by retrying its identity; what the kill decides is the killed replica's
//!   own durable floor, and the test asserts only that it recovered one and
//!   caught up past it. The seal window is open and the restart escalates when
//!   the process says it must, which the test asserts about rather than hides.
//! - `fencing_survives_a_killed_owner_and_a_cluster_wide_restart` kills the
//!   owner's replica mid-tenure and later kills every replica. Both windows are
//!   open on every replica. The fencing property is asserted against the
//!   guarded resource, which is outside the cluster and therefore indifferent
//!   to where any kill landed.
//! - `token_marks_are_monotone_across_a_cluster_wide_restart` kills every
//!   replica while idle. The commit window is empty; the seal window is open,
//!   and monotonicity is asserted as an inequality that holds under a reseed as
//!   well as under a plain restart.
//! - `readiness_refuses_service_until_recovery_completes` reaches its refusal
//!   without any kill, and stops its incumbent cleanly. There is no window.
//! - `a_damaged_slot_refuses_a_plain_restart_and_names_its_way_out` kills a
//!   follower while idle and then damages a slot itself. Both windows are shut:
//!   the damage is the test's, not the kill's, so the refusal it asserts is
//!   reached deliberately rather than raced for.
//! - `an_unauthenticated_client_may_claim_any_identity` kills nothing.

// The command builders are shared by every suite, and this one uses a subset:
// it never needs to present an envelope that disagrees with its own operation,
// because the line protocol derives the fingerprint from the operation. The
// allowance is scoped to this target and says nothing about the others.
#[allow(dead_code, reason = "this suite uses a subset of the shared builders")]
mod support;

#[path = "support/process.rs"]
mod process;
#[path = "support/scratch.rs"]
mod scratch;

use std::collections::BTreeMap;

use rafter::{LogIndex, NodeId};
use rafter_reference_fenced_lock::{
    store::{raw_slot, SlotIndex, SLOT_HEADER_LEN, SLOT_TRAILER_LEN},
    ApplyDisposition, Command, FencingToken, GuardedRejection, GuardedResource, GuardedWrite,
    HistoryEvent, LockConfig, LockRejection, LockResponse, Operation, OperationResult,
    RequestRejection, ResourceName,
};

use process::{ProcessCluster, SubmitOutcome};
use support::{acquire, config, expire_through, open_session, release, renew, resource, submit};

/// The bounds every process test runs under.
///
/// Small on purpose: a durable publication is a whole state image, so a larger
/// bound would make every transaction bigger without testing anything more.
/// Four client slots is what the fencing test needs — an owner, an expiration
/// driver, a successor, and one more tenure after the restart.
fn process_config() -> LockConfig {
    config(4, 4)
}

/// Asserts a submission committed and returns it.
#[track_caller]
fn applied(outcome: &SubmitOutcome) -> (ApplyDisposition, LockResponse) {
    match outcome {
        SubmitOutcome::Applied {
            disposition,
            response,
        } => (*disposition, *response),
        other => panic!("expected a committed command, observed {other:?}"),
    }
}

/// Asserts an operation committed with an exact result.
#[track_caller]
fn assert_operation(outcome: &SubmitOutcome, expected: OperationResult) {
    assert_eq!(
        outcome.operation(),
        expected,
        "the replicated result must be exactly the contract's result"
    );
}

/// Asserts the recorded history has one invocation and one terminal event per
/// operation, and that it placed at least one of them.
///
/// The lock has no linearizability checker of its own — the ledger owns that
/// code and the two consumers share nothing — so this is a structural check on
/// the recorder rather than a decision about orderings, and it is described as
/// exactly that. What the history is genuinely load bearing for is
/// [`assert_tokens_never_reissue`], which is a real safety property read off
/// the client-visible events alone.
#[track_caller]
fn assert_history_well_formed(cluster: &ProcessCluster) {
    let mut invoked = BTreeMap::new();
    let mut terminal = BTreeMap::new();
    for event in cluster.history() {
        let operation_id = event.operation_id();
        let counter = match event {
            HistoryEvent::Invoked { .. } => &mut invoked,
            HistoryEvent::Completed { .. }
            | HistoryEvent::Unknown { .. }
            | HistoryEvent::NotCommitted { .. } => &mut terminal,
        };
        *counter.entry(operation_id).or_insert(0_u32) += 1;
    }
    assert!(
        !invoked.is_empty(),
        "a process history that recorded no operations proves nothing"
    );
    for (operation_id, count) in &invoked {
        assert_eq!(
            *count, 1,
            "operation {operation_id:?} was invoked {count} times"
        );
        assert_eq!(
            terminal.get(operation_id).copied().unwrap_or(0),
            1,
            "operation {operation_id:?} has no single terminal event"
        );
    }
    assert_eq!(
        invoked.len(),
        terminal.len(),
        "every terminal event must belong to an invocation"
    );
}

/// One request's identity, minus the fingerprint the protocol does not carry.
///
/// Comparable, so it can key a map. A retry repeats this triple exactly, which
/// is what makes it a retry.
type Identity = (u32, u64, u64);

/// Asserts no two *distinct* requests ever received the same fencing token for
/// one resource.
///
/// Read off the client-visible history alone: the invocation supplies the
/// resource name and the request identity, the completion supplies the token,
/// and the two are correlated by operation id. Tokens for different resource
/// names are never compared, because the contract says they are unrelated.
///
/// "Distinct requests" is the whole of the property and not a softening of it.
/// A replayed retry legitimately returns the token its original execution
/// issued — that is what a session cache is for — and the history vocabulary
/// carries no disposition, so a checker reading it cannot tell an execution
/// from a replay and must not try. Keying on the identity is what makes the
/// assertion decidable from the history alone: within one identity a repeated
/// token is the cache working, and across two identities it is the failure the
/// guarded resource exists to catch.
#[track_caller]
fn assert_tokens_never_reissue(cluster: &ProcessCluster) {
    let mut invocations: BTreeMap<u64, (ResourceName, Identity)> = BTreeMap::new();
    for event in cluster.history() {
        if let HistoryEvent::Invoked {
            operation_id,
            command:
                Command::Submit {
                    request,
                    operation: Operation::Acquire { resource, .. },
                },
        } = event
        {
            invocations.insert(
                operation_id.get(),
                (
                    *resource,
                    (
                        request.client_id.get(),
                        request.session_epoch.get(),
                        request.sequence.get(),
                    ),
                ),
            );
        }
    }

    let mut issued: BTreeMap<(ResourceName, FencingToken), Identity> = BTreeMap::new();
    for event in cluster.history() {
        let HistoryEvent::Completed {
            operation_id,
            response: LockResponse::Operation(OperationResult::Acquired { token, .. }),
        } = event
        else {
            continue;
        };
        let Some((resource, identity)) = invocations.get(&operation_id.get()) else {
            panic!("an acquisition completed without a recorded invocation: {event:?}");
        };
        match issued.insert((*resource, *token), *identity) {
            None => {}
            Some(previous) => assert_eq!(
                previous,
                *identity,
                "resource {} issued token {} to two different requests",
                resource.as_str(),
                token.get()
            ),
        }
    }
}

/// Presents one write to the guarded resource under `token`.
///
/// The resource name comes off the guarded resource itself, because a write
/// naming a different one is a separate rejection the lock service has no part
/// in and no test here is about.
fn guarded_write(
    guarded: &mut GuardedResource,
    token: FencingToken,
    value: u64,
) -> Result<u64, GuardedRejection> {
    let resource = guarded.resource();
    guarded.apply(GuardedWrite {
        resource,
        token,
        value,
    })
}

/// Kills every replica with `SIGKILL` and restarts each from its own store.
///
/// Each replica dies wherever it happened to be, so each recovers from its own
/// crash window independently — including the window that leaves a durable
/// image whose seal never landed, which a plain restart refuses. `restart`
/// escalates only to the mode the refusing process itself named, and the
/// escalations are left on the cluster for a caller to assert about.
fn restart_whole_cluster(cluster: &mut ProcessCluster) -> NodeId {
    for node_id in cluster.live_nodes() {
        cluster.kill(node_id);
    }
    for node_id in process::NODE_IDS {
        cluster.restart(node_id);
    }
    let leader = cluster.wait_for_leader();
    assert!(
        process::NODE_IDS.contains(&leader),
        "a restarted replica leads the recovered cluster"
    );
    leader
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn three_processes_elect_a_leader_and_serve_the_lock() {
    let mut cluster = ProcessCluster::start("elect-and-serve", process_config());

    let leader = cluster.wait_for_leader();
    assert_eq!(
        leader,
        NodeId(1),
        "in a first election nobody is sticky, so the shortest election timeout wins"
    );
    assert_eq!(
        cluster.wait_for_agreed_leader(),
        leader,
        "every replica routes clients to the replica that actually leads"
    );
    for node_id in cluster.live_nodes() {
        let status = cluster
            .status(node_id)
            .unwrap_or_else(|| panic!("replica {} answers STATUS", node_id.0));
        assert!(
            status.ready,
            "every replica announced readiness before serving"
        );
    }

    let session = cluster.submit_to_leader(open_session(0, 1));
    assert_eq!(
        applied(&session).0,
        ApplyDisposition::SessionOpened,
        "an unused client slot accepts its first epoch"
    );

    let acquired = cluster.submit_to_leader(submit(0, 1, 1, acquire("vault", 10)));
    let token = acquired.acquired_token();
    assert_eq!(
        token,
        FencingToken::first(),
        "the first acquisition of a resource receives token one"
    );
    assert_operation(
        &acquired,
        OperationResult::Acquired {
            token,
            expiry: support::time(10),
        },
    );

    let view = cluster.get_lock(resource("vault"));
    assert_eq!(view.owner, Some(0), "a linearizable read sees the holder");
    assert_eq!(view.held_token, Some(token.get()));
    assert_eq!(view.token_floor, Some(token.get()));
    assert_eq!(
        view.logical_time, 0,
        "nothing has advanced replicated logical time, because nothing expired"
    );

    // A second client cannot take a held lock, and the refusal names the
    // holder. This is also the only place a lock-level rejection crosses the
    // process boundary, so it is what pairs the binary's rendering of one
    // against this suite's independent parsing of it.
    cluster.submit_to_leader(open_session(1, 1));
    let contended = cluster.submit_to_leader(submit(1, 1, 1, acquire("vault", 10)));
    assert_operation(
        &contended,
        OperationResult::Rejected(LockRejection::LockHeld {
            owner: support::client(0),
            token,
            expiry: support::time(10),
        }),
    );

    // Renewal extends a tenure without issuing a token.
    let renewed = cluster.submit_to_leader(submit(0, 1, 2, renew("vault", token.get(), 20)));
    assert_operation(
        &renewed,
        OperationResult::Renewed {
            token,
            expiry: support::time(20),
        },
    );

    let released = cluster.submit_to_leader(submit(0, 1, 3, release("vault", token.get())));
    assert_operation(&released, OperationResult::Released);

    let after_release = cluster.get_lock(resource("vault"));
    assert!(!after_release.is_held(), "release ends the tenure");
    assert_eq!(
        after_release.token_floor,
        Some(token.get()),
        "the high-water mark survives release"
    );

    let reacquired = cluster.submit_to_leader(submit(0, 1, 4, acquire("vault", 10)));
    assert!(
        reacquired.acquired_token() > token,
        "a new tenure is strictly above the resource's high-water mark"
    );

    assert_history_well_formed(&cluster);
    assert_tokens_never_reissue(&cluster);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_session_retry_after_the_leader_is_killed_returns_the_cached_result() {
    // The commit window here is empty by construction: every command below was
    // acknowledged before the leader was killed, so the retry must observe the
    // cached result and must not execute anything.
    let mut cluster = ProcessCluster::start("dedup-across-failover", process_config());
    let leader = cluster.wait_for_leader();

    cluster.submit_to_leader(open_session(0, 1));
    let acquired = cluster.submit_to_leader(submit(0, 1, 1, acquire("vault", 10)));
    let token = acquired.acquired_token();

    cluster.kill(leader);
    let failover_leader = cluster.wait_for_leader();
    assert!(
        cluster.live_nodes().contains(&failover_leader),
        "a surviving replica takes over"
    );
    assert_ne!(
        failover_leader, leader,
        "the killed replica cannot still be leading"
    );

    // Retrying the same identity is the contract's answer. A fresh identity
    // would be a second acquisition attempt against a lock this client already
    // holds, which is a different operation with a different result.
    let retry = cluster.submit_to_leader(submit(0, 1, 1, acquire("vault", 10)));
    assert_eq!(
        applied(&retry).0,
        ApplyDisposition::Replayed,
        "an exact retry of the highest completed sequence is replayed, not executed"
    );
    assert_eq!(
        retry.acquired_token(),
        token,
        "a replay returns the original token rather than issuing a second one"
    );

    let view = cluster.get_lock(resource("vault"));
    assert_eq!(
        view.held_token,
        Some(token.get()),
        "the tenure survived the failover intact"
    );
    assert_eq!(
        view.token_floor,
        Some(token.get()),
        "the replay issued no token, so the high-water mark did not move"
    );

    assert_history_well_formed(&cluster);
    assert_tokens_never_reissue(&cluster);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_killed_replica_recovers_its_durable_floor_and_rejoins() {
    // The killed replica is a follower, so the surviving two hold a quorum and
    // the writes below are settled by retrying an identity. What the kill
    // decides is how far the follower's own durable store got, and whether it
    // got there in a state a plain restart can adopt. The test asserts only
    // that it recovered a floor, by whichever path the process itself named,
    // and then caught up past it.
    let mut cluster = ProcessCluster::start("restart-and-catch-up", process_config());
    let leader = cluster.wait_for_leader();
    let victim = *cluster
        .live_nodes()
        .iter()
        .find(|node_id| **node_id != leader)
        .expect("a three-replica cluster has a follower");

    cluster.submit_to_leader(open_session(0, 1));
    let token = cluster
        .submit_to_leader(submit(0, 1, 1, acquire("vault", 10)))
        .acquired_token();

    let in_flight = cluster.begin_submit(leader, submit(0, 1, 2, renew("vault", token.get(), 30)));
    cluster.kill(victim);
    // A quorum survives, but that does not make this write's outcome certain: a
    // leader under load can still lose leadership and drop the proposal.
    // Retrying the same identity settles it either way.
    let lost = cluster.resolve_submit(in_flight);
    let settled = cluster.submit_to_leader(submit(0, 1, 2, renew("vault", token.get(), 30)));
    assert_operation(
        &settled,
        OperationResult::Renewed {
            token,
            expiry: support::time(30),
        },
    );
    assert!(
        matches!(
            applied(&settled).0,
            ApplyDisposition::Applied | ApplyDisposition::Replayed
        ),
        "the retry either executed the never-committed renewal or replayed the committed one; \
         the lost attempt reported {lost:?}"
    );

    let leader_applied = cluster
        .status(leader)
        .expect("the leader answers STATUS")
        .applied;

    let recovered_floor = cluster.restart(victim);
    assert!(
        recovered_floor > LogIndex::ZERO,
        "the restarted replica recovered a durable applied floor from its own store"
    );
    assert!(
        recovered_floor.0 <= leader_applied,
        "a replica cannot recover past what the cluster committed"
    );
    // Whether the restart had to escalate is not under this harness's control,
    // and it is asserted about rather than assumed away: an escalation that
    // happened must have been the one the process itself named.
    for escalation in cluster.escalations() {
        assert_eq!(escalation.node_id, victim);
        assert!(
            escalation.mode == "repair" || escalation.mode == "reseed",
            "a refusal must name one of the two modes above the plain one, named {}",
            escalation.mode
        );
    }

    cluster.wait_applied_through(victim, LogIndex(leader_applied));
    let rejoined = cluster.local_read(victim, resource("vault"));
    assert_eq!(
        rejoined.view().held_token,
        Some(token.get()),
        "the rejoined replica's own state agrees with the cluster it caught up to"
    );
    assert_eq!(
        rejoined.view().token_floor,
        Some(token.get()),
        "and it holds the same fencing high-water mark"
    );

    assert_history_well_formed(&cluster);
    assert_tokens_never_reissue(&cluster);
    cluster.shutdown();
}

/// The crown jewel: fencing across a process boundary, and across a restart.
///
/// A client acquires a lock and writes to a resource the lock service knows
/// nothing about. Its replica is then killed while it still holds the lock —
/// the tenure has not lapsed, and nothing in the cluster has expired it. The
/// surviving majority elects a leader and expires the lease *through consensus*,
/// a later client acquires a strictly higher token and writes, and the original
/// client — which has learned nothing and still believes it holds the lock — is
/// refused by the guarded resource.
///
/// Then every replica is killed and restarted from its own durable state, and
/// the whole thing has to still be true: the high-water mark comes back, the
/// next tenure is above it, and both retired tokens stay refused.
///
/// Nothing here waits for a lease to lapse in real time. Expiry is a replicated
/// command with a deterministic effect, which is why this test has no timing in
/// it at all beyond waiting for elections.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn fencing_survives_a_killed_owner_and_a_cluster_wide_restart() {
    let vault = resource("vault");
    let mut guarded = GuardedResource::new(vault);
    let mut cluster = ProcessCluster::start("fencing-across-processes", process_config());
    let leader = cluster.wait_for_leader();

    // Client 0 takes the lock and writes under its tenure.
    cluster.submit_to_leader(open_session(0, 1));
    let first = cluster
        .submit_to_leader(submit(0, 1, 1, acquire("vault", 10)))
        .acquired_token();
    assert_eq!(
        guarded_write(&mut guarded, first, 11),
        Ok(11),
        "the holder's write is accepted by the guarded resource"
    );

    // Its replica dies mid-tenure. The lock is still held: logical time has not
    // moved, so nothing has expired.
    cluster.kill(leader);
    let failover = cluster.wait_for_leader();
    assert_ne!(failover, leader, "a survivor takes over");
    assert_eq!(
        cluster.get_lock(vault).held_token,
        Some(first.get()),
        "killing the owner's replica does not end the owner's tenure"
    );

    // The majority expires the lease through consensus. The expiration driver
    // is an ordinary client submitting an ordinary replicated command; the
    // contract puts its authorization outside the state machine, and at this
    // composition level there is none.
    cluster.submit_to_leader(open_session(1, 1));
    let expired = cluster.submit_to_leader(submit(1, 1, 1, expire_through(10)));
    assert_operation(
        &expired,
        OperationResult::Expired {
            released_locks: 1,
            logical_time: support::time(10),
        },
    );

    // A later owner acquires a strictly higher token and writes.
    cluster.submit_to_leader(open_session(2, 1));
    let second = cluster
        .submit_to_leader(submit(2, 1, 1, acquire("vault", 10)))
        .acquired_token();
    assert!(
        second > first,
        "expiration does not lower a resource's high-water mark"
    );
    assert_eq!(
        guarded_write(&mut guarded, second, 22),
        Ok(22),
        "the later owner's write is accepted"
    );

    // The original client has learned nothing and still believes it holds the
    // lock. This is the property the whole design exists for.
    assert_eq!(
        guarded_write(&mut guarded, first, 99),
        Err(GuardedRejection::StaleFencingToken {
            highest_accepted: second
        }),
        "a stale former owner cannot modify the guarded resource after a later owner is \
         established"
    );
    assert_eq!(guarded.value(), 22, "the refused write changed nothing");
    assert_eq!(guarded.refused_writes(), 1);

    // Every replica is killed and restarted from its own durable state.
    restart_whole_cluster(&mut cluster);

    let recovered = cluster.get_lock(vault);
    assert_eq!(
        recovered.token_floor,
        Some(second.get()),
        "the fencing high-water mark survived a real restart from durable state"
    );
    assert_eq!(
        recovered.held_token,
        Some(second.get()),
        "and so did the tenure that mark belongs to"
    );
    assert_eq!(
        recovered.logical_time, 10,
        "replicated logical time survived the restart"
    );

    // A fresh tenure after the restart is strictly above everything the guarded
    // resource ever accepted.
    cluster.submit_to_leader(submit(1, 1, 2, expire_through(20)));
    cluster.submit_to_leader(open_session(3, 1));
    let third = cluster
        .submit_to_leader(submit(3, 1, 1, acquire("vault", 10)))
        .acquired_token();
    assert!(
        third > second,
        "a tenure issued after a cluster-wide restart is above every acknowledged token"
    );
    assert_eq!(guarded_write(&mut guarded, third, 33), Ok(33));
    for retired in [first, second] {
        assert_eq!(
            guarded_write(&mut guarded, retired, 99),
            Err(GuardedRejection::StaleFencingToken {
                highest_accepted: third
            }),
            "token {} is retired and stays retired across the restart",
            retired.get()
        );
    }
    assert_eq!(guarded.value(), 33);
    assert_eq!(guarded.accepted_writes(), 3);

    assert_history_well_formed(&cluster);
    assert_tokens_never_reissue(&cluster);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn token_marks_are_monotone_across_a_cluster_wide_restart() {
    // Every replica is killed while idle, so each recovers from its own crash
    // window independently. Monotonicity is asserted as an inequality, which
    // holds whether a replica restarted plainly, repaired, or reseeded: a
    // reseeded replica refills from the replicated log, and the log is what the
    // marks came from.
    let mut cluster = ProcessCluster::start("marks-across-restart", process_config());
    cluster.submit_to_leader(open_session(0, 1));

    // Two resources with independent token spaces, each taken through a full
    // tenure and released, so both have a high-water mark above zero and
    // neither is held when the cluster dies.
    let mut acknowledged: BTreeMap<&str, u64> = BTreeMap::new();
    let mut sequence = 1_u64;
    for name in ["alpha", "beta"] {
        let token = cluster
            .submit_to_leader(submit(0, 1, sequence, acquire(name, 10)))
            .acquired_token();
        sequence += 1;
        cluster.submit_to_leader(submit(0, 1, sequence, release(name, token.get())));
        sequence += 1;
        let reacquired = cluster
            .submit_to_leader(submit(0, 1, sequence, acquire(name, 10)))
            .acquired_token();
        sequence += 1;
        assert!(reacquired > token, "a second tenure is above the first");
        cluster.submit_to_leader(submit(0, 1, sequence, release(name, reacquired.get())));
        sequence += 1;
        acknowledged.insert(name, reacquired.get());
    }

    restart_whole_cluster(&mut cluster);

    for (name, mark) in &acknowledged {
        let view = cluster.get_lock(resource(name));
        assert!(
            !view.is_held(),
            "{name} was released before the restart and must not come back held"
        );
        assert_eq!(
            view.token_floor,
            Some(*mark),
            "the high-water mark of {name} survived the restart exactly"
        );
    }

    // And the next tenure of each resource is strictly above what was
    // acknowledged before the cluster died. Token spaces are per resource, so
    // this is asserted per resource and never across them.
    for (name, mark) in &acknowledged {
        let token = cluster
            .submit_to_leader(submit(0, 1, sequence, acquire(name, 10)))
            .acquired_token();
        sequence += 1;
        assert!(
            token.get() > *mark,
            "{name} reissued or lowered a token across the restart: {} is not above {mark}",
            token.get()
        );
    }

    assert_history_well_formed(&cluster);
    assert_tokens_never_reissue(&cluster);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn readiness_refuses_service_until_recovery_completes() {
    // The refusal being tested is reached without any kill: a second process
    // for an owned directory cannot recover, so it cannot serve. That makes the
    // negative assertions deterministic rather than a race against recovery.
    let mut cluster = ProcessCluster::start("readiness-gates", process_config());
    cluster.submit_to_leader(open_session(0, 1));
    let token = cluster
        .submit_to_leader(submit(0, 1, 1, acquire("vault", 10)))
        .acquired_token();

    let mut contender = cluster.spawn_contender(NodeId(2));
    contender.wait_for_line("WAITING_FOR_OWNERSHIP");
    assert!(
        !contender.has_announced("READY"),
        "a replica that has not recovered must not announce readiness"
    );

    let refused_submit = contender
        .ask("SUBMIT 0 1 2 RELEASE vault 1")
        .expect("a recovering replica still answers its client port");
    assert!(
        refused_submit.starts_with("NOTREADY "),
        "a recovering replica must refuse to replicate, observed {refused_submit:?}"
    );
    let refused_query = contender
        .ask("QUERY LOCK vault")
        .expect("a recovering replica still answers its client port");
    assert!(
        refused_query.starts_with("NOTREADY "),
        "a recovering replica must refuse to read, observed {refused_query:?}"
    );
    let status = contender.ask("STATUS").expect("STATUS is never gated");
    assert!(
        status.starts_with("STATUS recovering "),
        "readiness must be observable while it is closed, observed {status:?}"
    );
    // The refusals above are asserted rather than recorded. A request the
    // replica never handed to `rafter-service` reached no log, so it constrains
    // no ordering; asserting the exact refusal is the stronger statement.

    // Only now does the incumbent release its directory, which is what lets the
    // contender finish recovering. The gate opens because recovery completed,
    // not because time passed. The incumbent is stopped cleanly rather than
    // killed on purpose: this test is about the gate, and a crash window here
    // would mix a recovery outcome into it.
    cluster.stop(NodeId(2));
    let recovered_floor = contender.wait_ready();
    assert!(
        recovered_floor > LogIndex::ZERO,
        "the replacement recovered the incumbent's durable state"
    );

    // The gate now answers the request it previously refused. It answers with
    // this replica's *own* recovered state, which is the exact scope of the
    // claim: readiness means recovery finished, not that this replica has
    // caught up with its cluster. A local read here may legitimately be behind,
    // and asserting current state at this instant would be asserting something
    // readiness never promised.
    let served = contender
        .ask("LOCAL LOCK vault")
        .expect("a ready replica serves");
    assert!(
        served.starts_with("OK LOCK vault "),
        "the request that was refused before recovery is answered after it, observed {served:?}"
    );

    cluster.adopt(NodeId(2), contender);
    let released = cluster.submit_to_leader(submit(0, 1, 2, release("vault", token.get())));
    assert_operation(&released, OperationResult::Released);

    // Currency is a separate condition from readiness, and it is waited for
    // separately.
    let leader = cluster.wait_for_leader();
    let leader_applied = cluster
        .status(leader)
        .expect("the leader answers STATUS")
        .applied;
    cluster.wait_applied_through(NodeId(2), LogIndex(leader_applied));
    let caught_up = cluster.local_read(NodeId(2), resource("vault"));
    assert!(
        !caught_up.view().is_held(),
        "once caught up, the recovered replica holds the cluster's state"
    );
    assert_eq!(
        caught_up.view().token_floor,
        Some(token.get()),
        "including the fencing high-water mark it never issued itself"
    );

    assert_history_well_formed(&cluster);
    assert_tokens_never_reissue(&cluster);
    cluster.shutdown();
}

/// The refusal a plain restart cannot talk round, and the escalation it names.
///
/// `LockStore::open` refuses any damaged slot it cannot prove was the one being
/// written, whether or not the partner is intact, because adopting the partner
/// would silently roll the store back a generation — and a generation of this
/// store can contain a fencing high-water mark a guarded resource has already
/// accepted. An ordinary `SIGKILL` can produce such a slot on its own, between
/// a durable image and its seal, so this is not an exotic path: it is the one
/// [`ProcessCluster::restart`] escalates through whenever a kill lands there.
/// This test reaches it deliberately, because a path only a race reaches is a
/// path that is never actually exercised.
///
/// What it establishes is that the refusal is announced in a line a supervisor
/// can match on, that the readiness gate stays shut while it stands, that the
/// process names the mode that follows, and that the named mode opens the store
/// and says what it discarded. The surviving quorum serves throughout, which is
/// what makes the refusal affordable: one replica declining to guess is not an
/// outage.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn a_damaged_slot_refuses_a_plain_restart_and_names_its_way_out() {
    let mut cluster = ProcessCluster::start("needs-decision", process_config());
    let leader = cluster.wait_for_leader();
    let victim = *cluster
        .live_nodes()
        .iter()
        .find(|node_id| **node_id != leader)
        .expect("a three-replica cluster has a follower");

    cluster.submit_to_leader(open_session(0, 1));
    let mut sequence = 1_u64;
    let mut token = cluster
        .submit_to_leader(submit(0, 1, sequence, acquire("vault", 10)))
        .acquired_token();
    // Several publications, so both slots have been written and either one is
    // a legitimate thing to damage.
    for _ in 0..2 {
        sequence += 1;
        cluster.submit_to_leader(submit(0, 1, sequence, release("vault", token.get())));
        sequence += 1;
        token = cluster
            .submit_to_leader(submit(0, 1, sequence, acquire("vault", 10)))
            .acquired_token();
    }
    let acknowledged = token;
    cluster.wait_applied_through(victim, LogIndex(2));

    // Killed while idle: the damage below is applied by this test rather than
    // by the kill, so there is no window to reason about.
    let app_dir = process::NodeProcess::app_dir(cluster.root(), victim);
    cluster.kill(victim);
    damage_a_slot(&app_dir);

    // A plain restart refuses, says so, and never announces readiness.
    let mut refused = process::NodeProcess::spawn(cluster.root(), victim, process_config());
    let announced = refused.wait_for_line("NEEDS_DECISION");
    assert!(
        announced.contains("--recover"),
        "the refusal must name the way out, observed {announced:?}"
    );
    assert!(
        !refused.has_announced("READY"),
        "a replica whose store will not open must never announce readiness"
    );
    assert!(
        !refused.has_announced("REPAIRED"),
        "a plain restart must never repair anything"
    );
    drop(refused);

    // The surviving quorum served throughout.
    let quorum_leader = cluster.wait_for_agreed_leader();
    assert_ne!(quorum_leader, victim, "the refusing replica cannot lead");
    sequence += 1;
    cluster.submit_to_leader(submit(0, 1, sequence, release("vault", acknowledged.get())));

    // Restarting escalates to exactly the mode the process named, and the
    // escalation is recorded rather than absorbed.
    let recovered_floor = cluster.restart(victim);
    let escalations = cluster.escalations();
    assert_eq!(
        escalations.len(),
        1,
        "one refusal earns one escalation, observed {escalations:?}"
    );
    assert_eq!(escalations[0].node_id, victim);
    assert_eq!(
        escalations[0].mode, "repair",
        "an unreadable slot beside an intact one is a repair, not a reseed"
    );
    assert!(
        cluster.node_mut(victim).has_announced("REPAIRED"),
        "the mode that opened the store must say what it discarded"
    );
    assert!(
        recovered_floor > LogIndex::ZERO,
        "the repaired replica adopted the partner slot's durable floor"
    );

    // And it catches up through ordinary replication rather than through
    // anything the repair did, high-water mark included.
    let leader_applied = cluster
        .status(quorum_leader)
        .expect("the leader answers STATUS")
        .applied;
    cluster.wait_applied_through(victim, LogIndex(leader_applied));
    let caught_up = cluster.local_read(victim, resource("vault"));
    assert_eq!(
        caught_up.view().token_floor,
        Some(acknowledged.get()),
        "the repaired replica holds the fencing high-water mark the cluster acknowledged"
    );

    assert_history_well_formed(&cluster);
    assert_tokens_never_reissue(&cluster);
    cluster.shutdown();
}

/// Corrupts one durable slot so that recovery reaches it and cannot verify it.
///
/// The flipped byte is in the payload, so the framing is untouched and the
/// commit checksum is what fails. Whichever slot is currently live, damaging
/// the longer of the two hits a slot that has actually been published, and
/// `open` refuses either way — an unreadable slot refuses the store whether or
/// not its partner is intact.
fn damage_a_slot(app_dir: &std::path::Path) {
    let slots = [SlotIndex::Zero, SlotIndex::One].map(|slot| {
        (
            slot,
            raw_slot::read(app_dir, slot).expect("a killed replica's slot reads back"),
        )
    });
    let (slot, mut bytes) = slots
        .into_iter()
        .max_by_key(|(_, bytes)| bytes.len())
        .expect("two slots exist");
    assert!(
        bytes.len() > SLOT_HEADER_LEN + SLOT_TRAILER_LEN,
        "the damaged slot must hold a payload to damage, observed {} bytes",
        bytes.len()
    );
    let target = bytes.len() - SLOT_TRAILER_LEN - 1;
    bytes[target] ^= 0xFF;
    raw_slot::write(app_dir, slot, &bytes).expect("the slot rewrites");
}

/// The boundary this composition does not close, demonstrated rather than only
/// written down.
///
/// `CONTRACT.md` says the client protocol authenticates nothing and that a
/// client id is a deduplication slot rather than a principal. That sentence is
/// cheap; this is what it means. A caller that presents no credential — because
/// there is no field in which to present one — acts as an established client,
/// releases a lock it never took, and drives replicated logical time, which the
/// contract says only the service's authorized expiration driver should do.
///
/// Both are expected behaviour at the integration level and both are release
/// blockers at the production level. The test exists so that a future change
/// which believed it had closed either hole would have to come here and delete
/// an assertion, rather than quietly leave a paragraph true and a claim false.
#[test]
#[ignore = "spawns real processes; run with --ignored (see the module docs)"]
fn an_unauthenticated_client_may_claim_any_identity() {
    let mut cluster = ProcessCluster::start("unauthenticated-identity", process_config());
    let leader = cluster.wait_for_leader();

    cluster.submit_to_leader(open_session(0, 1));
    let token = cluster
        .submit_to_leader(submit(0, 1, 1, acquire("vault", 10)))
        .acquired_token();

    // A raw line on a fresh connection, carrying nothing that names its sender.
    let impersonated = cluster
        .node_mut(leader)
        .ask(&format!("SUBMIT 0 1 2 RELEASE vault {}", token.get()))
        .expect("the replica answers");
    assert!(
        impersonated.starts_with("OK APPLIED OP RELEASED"),
        "nothing in this protocol distinguishes client 0 from anybody claiming to be it, \
         observed {impersonated:?}"
    );
    assert!(
        !cluster.get_lock(resource("vault")).is_held(),
        "the lock was released by a caller that never took it"
    );

    // And the expiration driver is not a role anything enforces. The contract
    // puts that authorization outside the replicated state machine; at this
    // level it is outside everything.
    cluster.submit_to_leader(open_session(1, 1));
    let driven = cluster.submit_to_leader(submit(1, 1, 1, expire_through(1)));
    assert_operation(
        &driven,
        OperationResult::Expired {
            released_locks: 0,
            logical_time: support::time(1),
        },
    );

    // What the state machine *does* still enforce is everything it was ever
    // asked to: the identity is unauthenticated, not unchecked. Sequence 1 is
    // below the highest completed sequence of the session the impersonation
    // advanced, so it is stale rather than replayable — only an exact retry of
    // the highest completed sequence replays.
    let stale = cluster.submit_to_leader(submit(0, 1, 1, acquire("vault", 10)));
    let (disposition, response) = applied(&stale);
    assert_eq!(
        disposition,
        ApplyDisposition::Rejected,
        "an unauthenticated caller is still held to the session protocol"
    );
    assert_eq!(
        response,
        LockResponse::Rejected(RequestRejection::StaleSequence {
            highest: support::sequence(2)
        }),
        "and the rejection names the sequence the impersonated session reached"
    );

    assert_history_well_formed(&cluster);
    assert_tokens_never_reissue(&cluster);
    cluster.shutdown();
}
