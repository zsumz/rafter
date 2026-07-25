//! Replicated lock histories over the consumer-owned `rafter-service` driver.
//!
//! Each test is one scenario told end to end: what the clients did, which
//! links were cut, and what the guarded resource downstream was allowed to
//! believe afterwards. Every client operation goes through the public managed
//! handle, and every terminal outcome is recorded in the contract's history
//! vocabulary and checked against the structurally independent oracle.

mod support;

#[path = "support/cluster.rs"]
mod cluster;
#[path = "support/observe.rs"]
mod observe;
#[path = "support/transport.rs"]
mod transport;

use std::collections::BTreeMap;

use rafter::{LogIndex, NodeId, ReadIndexCancelReason, Role};
use rafter_reference_fenced_lock::{
    unknown_outcome_reason, ApplyDisposition, Command, FencingToken, GuardedRejection,
    GuardedResource, GuardedWrite, HistoryEvent, LockRejection, LockResponse, LogicalTime,
    OperationId, OperationResult, QueryOutcome, ReferenceLockService, RequestFingerprint,
    RequestRejection, ResourceStatus, SubmitOutcome,
};
use rafter_service::{ReadError, UnknownOutcomeReason, WriteError, WriteErrorKind, WriteFate};

use cluster::{LockCluster, MAX_ROUNDS};
use support::{
    acquire, client, config, epoch, expire_through, open_session, release, renew, resource,
    sequence, submit, submit_with_fingerprint, time, token,
};

/// The one contended resource these histories use.
const RESOURCE: &str = "orders/shard-0";

/// Client slot reserved for the service's authorized expiration driver.
///
/// The replicated state machine deliberately cannot tell this slot apart from
/// any other. Restricting who may submit `ExpireThrough` is the deployment's
/// job, so the reservation lives here, above the replicated boundary.
const EXPIRATION_DRIVER: u32 = 2;

#[test]
fn acquire_renew_and_release_round_trip_with_fencing_tokens() {
    let mut cluster = LockCluster::new(config(4, 4));
    let leader = cluster.elect_leader();
    committed(&mut cluster, leader, open_session(0, 1));

    let acquired = committed(&mut cluster, leader, submit(0, 1, 1, acquire(RESOURCE, 10)));
    let (first_token, first_expiry) = acquisition(acquired);
    assert_eq!(
        first_token,
        token(1),
        "a resource's first tenure gets token 1"
    );
    assert_eq!(first_expiry, time(10), "expiry is logical time plus lease");

    let renewed = committed(
        &mut cluster,
        leader,
        submit(0, 1, 2, renew(RESOURCE, 1, 25)),
    );
    assert_eq!(
        renewed,
        LockResponse::Operation(OperationResult::Renewed {
            token: token(1),
            expiry: time(25),
        }),
        "renewal extends a tenure without minting a new token"
    );

    let status = answered(cluster.get_lock(leader, resource(RESOURCE)));
    assert_eq!(status.holder.map(|holder| holder.token), Some(token(1)));
    assert_eq!(status.token_floor, Some(token(1)));

    let released = committed(&mut cluster, leader, submit(0, 1, 3, release(RESOURCE, 1)));
    assert_eq!(released, LockResponse::Operation(OperationResult::Released));

    let reacquired = committed(&mut cluster, leader, submit(0, 1, 4, acquire(RESOURCE, 10)));
    let (second_token, _) = acquisition(reacquired);
    assert_eq!(
        second_token,
        token(2),
        "the high-water mark outlived the released tenure"
    );

    cluster.settle();
    assert_replicas_agree(&cluster, leader);
    assert_history_agrees_with_oracle(&cluster, leader);
}

#[test]
fn a_leadership_loss_closes_the_outcome_window_and_a_retry_replays_it() {
    let mut cluster = LockCluster::new(config(4, 4));
    let first_leader = cluster.elect_leader();
    committed(&mut cluster, first_leader, open_session(0, 1));
    cluster.settle();

    // The acquisition reaches the followers' logs, but every acknowledgement is
    // dropped, so the leader can never prove the entry committed.
    cluster.isolate_inbound(first_leader);
    let pending = cluster.begin_submit(first_leader, submit(0, 1, 1, acquire(RESOURCE, 10)));
    let lost = cluster.resolve(pending, 6);
    let SubmitOutcome::Unknown { error } = &lost else {
        panic!("an isolated leader cannot prove the outcome of its proposal, got {lost:?}");
    };
    assert_eq!(
        unknown_outcome_reason(error),
        Some(UnknownOutcomeReason::DriveBoundReached),
        "the outcome window closed rather than resolving, got {error:?}"
    );
    // The window is open because the driver could not prove it closed, which is
    // the one fact a retrying client may act on.
    assert!(
        error.fate().may_commit(),
        "an entry that reached the followers' logs may still commit"
    );
    assert!(
        cluster.believes_it_leads(first_leader),
        "the isolated node has heard nothing that would make it step down"
    );
    assert!(
        cluster.dropped_inbound() > 0,
        "the followers acknowledged the entry and the network ate the answer"
    );

    // Cutting the last link lets the majority elect and commit the entry the
    // former leader could not finish.
    cluster.isolate(first_leader);
    let second_leader = cluster.elect_leader();
    assert_ne!(second_leader, first_leader, "leadership moved");
    cluster.settle();
    assert!(
        cluster.driver(first_leader).refused_sends() > 0,
        "a fully isolated replica's transport refuses its own frames"
    );

    let retry = cluster.submit(second_leader, submit(0, 1, 1, acquire(RESOURCE, 10)));
    let SubmitOutcome::Completed { outcome, .. } = &retry else {
        panic!("the retry under the same identity must resolve, got {retry:?}");
    };
    assert_eq!(
        outcome.disposition,
        ApplyDisposition::Replayed,
        "the session cache answered the retry instead of executing it again"
    );
    let (retry_token, _) = acquisition(outcome.response);
    assert_eq!(
        retry_token,
        token(1),
        "a replayed acquisition returns the original token, never a fresh one"
    );

    cluster.settle();
    assert_eq!(
        cluster
            .state_machine(second_leader)
            .service()
            .status(resource(RESOURCE))
            .token_floor,
        Some(token(1)),
        "the identity executed exactly once, so exactly one token was issued"
    );
    assert!(
        cluster
            .history()
            .iter()
            .any(|event| matches!(event, HistoryEvent::Unknown { .. })),
        "the history retains the unknown-outcome window"
    );
    assert_history_agrees_with_oracle(&cluster, second_leader);
}

#[test]
fn a_stale_former_owner_is_refused_by_the_guarded_resource() {
    let mut cluster = LockCluster::new(config(4, 4));
    let first_leader = cluster.elect_leader();
    committed(&mut cluster, first_leader, open_session(0, 1));
    committed(&mut cluster, first_leader, open_session(1, 1));
    committed(
        &mut cluster,
        first_leader,
        open_session(EXPIRATION_DRIVER, 1),
    );

    // Client A takes the lock on the leader and starts writing under token 1.
    let acquired = committed(
        &mut cluster,
        first_leader,
        submit(0, 1, 1, acquire(RESOURCE, 5)),
    );
    let (token_a, _) = acquisition(acquired);
    cluster.settle();

    let mut guarded = GuardedResource::new(resource(RESOURCE));
    assert_eq!(
        guarded.apply(GuardedWrite {
            resource: resource(RESOURCE),
            token: token_a,
            value: 7,
        }),
        Ok(7),
        "the current owner may write"
    );

    // A's leader is cut off mid-tenure. Nothing has yet contradicted it, so it
    // still believes it leads and would happily tell A so.
    cluster.isolate(first_leader);
    cluster.run_rounds(1);
    assert!(
        cluster.believes_it_leads(first_leader),
        "a freshly cut-off leader has heard nothing that would make it step down"
    );

    let second_leader = cluster.elect_leader();
    assert_ne!(second_leader, first_leader);

    // The majority expires A's tenure through consensus and hands the resource
    // to client B, which receives a strictly greater token.
    let expired = committed(
        &mut cluster,
        second_leader,
        submit(EXPIRATION_DRIVER, 1, 1, expire_through(5)),
    );
    assert_eq!(
        expired,
        LockResponse::Operation(OperationResult::Expired {
            released_locks: 1,
            logical_time: time(5),
        }),
        "the horizon released exactly A's lease"
    );
    let reacquired = committed(
        &mut cluster,
        second_leader,
        submit(1, 1, 1, acquire(RESOURCE, 10)),
    );
    let (token_b, _) = acquisition(reacquired);
    assert!(token_b > token_a, "a later owner gets a later token");

    assert_eq!(
        guarded.apply(GuardedWrite {
            resource: resource(RESOURCE),
            token: token_b,
            value: 9,
        }),
        Ok(9),
        "the new owner establishes itself downstream"
    );

    // This is the whole reason this consumer exists. A still holds token N and
    // may still believe its old leader; the guarded resource refuses it anyway.
    assert_eq!(
        guarded.apply(GuardedWrite {
            resource: resource(RESOURCE),
            token: token_a,
            value: 11,
        }),
        Err(GuardedRejection::StaleFencingToken {
            highest_accepted: token_b,
        }),
        "a stale former owner cannot modify the guarded resource"
    );
    assert_eq!(guarded.value(), 9, "the refused write changed nothing");

    // A also cannot learn a stale answer from the leader it still trusts: the
    // linearizable barrier cannot be satisfied without a majority.
    let stale_read = cluster.get_lock(first_leader, resource(RESOURCE));
    assert!(
        matches!(stale_read, QueryOutcome::Unavailable { .. }),
        "an isolated former leader must answer nothing, got {stale_read:?}"
    );

    cluster.settle();
    assert_history_agrees_with_oracle(&cluster, second_leader);
}

#[test]
fn expiration_advances_logical_time_only_through_committed_horizons() {
    let mut cluster = LockCluster::new(config(4, 4));
    let leader = cluster.elect_leader();
    committed(&mut cluster, leader, open_session(0, 1));
    committed(&mut cluster, leader, open_session(EXPIRATION_DRIVER, 1));
    committed(&mut cluster, leader, submit(0, 1, 1, acquire(RESOURCE, 4)));

    assert_eq!(
        cluster.logical_time(leader),
        LogicalTime::ZERO,
        "acquisition does not advance replicated logical time"
    );

    // A horizon one short of the expiry leaves the lease alive: `E` is the
    // first logical time at which the lease no longer holds.
    let survived = committed(
        &mut cluster,
        leader,
        submit(EXPIRATION_DRIVER, 1, 1, expire_through(3)),
    );
    assert_eq!(
        survived,
        LockResponse::Operation(OperationResult::Expired {
            released_locks: 0,
            logical_time: time(3),
        })
    );

    // An equal horizon is rejected rather than treated as idempotent, so a
    // driver replaying a stale horizon under a fresh sequence cannot hide.
    let equal = committed(
        &mut cluster,
        leader,
        submit(EXPIRATION_DRIVER, 1, 2, expire_through(3)),
    );
    assert_eq!(
        equal,
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::LogicalTimeNotAdvanced { current: time(3) }
        ))
    );
    let stale = committed(
        &mut cluster,
        leader,
        submit(EXPIRATION_DRIVER, 1, 3, expire_through(1)),
    );
    assert_eq!(
        stale,
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::LogicalTimeNotAdvanced { current: time(3) }
        ))
    );

    let released = committed(
        &mut cluster,
        leader,
        submit(EXPIRATION_DRIVER, 1, 4, expire_through(4)),
    );
    assert_eq!(
        released,
        LockResponse::Operation(OperationResult::Expired {
            released_locks: 1,
            logical_time: time(4),
        })
    );

    cluster.settle();
    let status = answered(cluster.get_lock(leader, resource(RESOURCE)));
    assert_eq!(status.holder, None, "the expired tenure ended");
    assert_eq!(
        status.token_floor,
        Some(token(1)),
        "expiration never reclaims a high-water mark"
    );
    assert_eq!(status.logical_time, time(4));

    // The replicated state machine gives the driver slot no privilege at all.
    // Any client's committed horizon advances time; authorization is the
    // deployment's job, above this boundary.
    committed(&mut cluster, leader, submit(0, 1, 2, expire_through(9)));
    assert_eq!(cluster.logical_time(leader), time(9));

    assert_replicas_agree(&cluster, leader);
    assert_history_agrees_with_oracle(&cluster, leader);
}

#[test]
fn a_linearizable_query_resolves_to_a_non_answer_when_leadership_is_lost() {
    let mut cluster = LockCluster::new(config(4, 4));
    let first_leader = cluster.elect_leader();
    committed(&mut cluster, first_leader, open_session(0, 1));
    committed(
        &mut cluster,
        first_leader,
        submit(0, 1, 1, acquire(RESOURCE, 10)),
    );
    cluster.settle();

    // The barrier goes out and its acknowledgements never come back.
    cluster.isolate_inbound(first_leader);
    let pending = cluster.begin_query(first_leader, resource(RESOURCE));

    // The majority elects a new leader in a later term. When the former leader
    // hears that term it steps down and cancels the barrier it cannot finish.
    cluster.isolate(first_leader);
    let second_leader = cluster.elect_leader();
    assert_ne!(second_leader, first_leader);
    cluster.heal();

    let outcome = cluster.resolve_query(pending, MAX_ROUNDS);
    let QueryOutcome::Unavailable { error } = &outcome else {
        panic!("a barrier that lost its leadership must not answer, got {outcome:?}");
    };
    assert!(
        matches!(
            error,
            ReadError::Canceled {
                reason: ReadIndexCancelReason::LeadershipLost,
                ..
            } | ReadError::Rejected { .. }
                | ReadError::Transport { .. }
        ),
        "the read resolved to a typed non-answer, got {error:?}"
    );

    // The lock itself is unaffected: a lost read never changes replicated state.
    cluster.settle();

    // The new leader answers the same query with no intervening write. Its
    // only entry in the new term is a Raft noop the state machine never sees,
    // and the barrier requires only the highest committed application entry at
    // or below the read index, so a read-only workload survives the election.
    let status = answered(cluster.get_lock(second_leader, resource(RESOURCE)));
    assert_eq!(status.holder.map(|holder| holder.owner), Some(client(0)));
    assert_history_agrees_with_oracle(&cluster, second_leader);
}

#[test]
fn retries_gaps_and_epoch_displacement_survive_real_replication() {
    let mut cluster = LockCluster::new(config(4, 4));
    let leader = cluster.elect_leader();
    committed(&mut cluster, leader, open_session(0, 1));

    let applied = cluster.submit(leader, submit(0, 1, 1, acquire(RESOURCE, 10)));
    assert_eq!(disposition(&applied), ApplyDisposition::Applied);

    let replay = cluster.submit(leader, submit(0, 1, 1, acquire(RESOURCE, 10)));
    assert_eq!(disposition(&replay), ApplyDisposition::Replayed);
    assert_eq!(
        replay.committed().map(|outcome| outcome.response),
        applied.committed().map(|outcome| outcome.response),
        "an exact retry returns the original result"
    );

    let conflict = committed(&mut cluster, leader, submit(0, 1, 1, acquire(RESOURCE, 99)));
    assert_eq!(
        conflict,
        LockResponse::Rejected(RequestRejection::ConflictingRetry),
        "reusing the highest identity with another operation is a conflict"
    );

    let gap = committed(&mut cluster, leader, submit(0, 1, 7, release(RESOURCE, 1)));
    assert_eq!(
        gap,
        LockResponse::Rejected(RequestRejection::SequenceGap {
            expected: sequence(2)
        })
    );

    // An envelope whose fingerprint describes a different operation is
    // malformed wherever its sequence falls, and the versioned command frame
    // carries the client's digest verbatim so the replicas can say so.
    let claimed = RequestFingerprint::of(&acquire(RESOURCE, 1));
    let malformed = committed(
        &mut cluster,
        leader,
        submit_with_fingerprint(0, 1, 2, claimed, release(RESOURCE, 1)),
    );
    assert_eq!(
        malformed,
        LockResponse::Rejected(RequestRejection::FingerprintMismatch {
            expected: RequestFingerprint::of(&release(RESOURCE, 1)),
        })
    );

    committed(&mut cluster, leader, open_session(0, 2));
    let displaced = committed(&mut cluster, leader, submit(0, 1, 2, release(RESOURCE, 1)));
    assert_eq!(
        displaced,
        LockResponse::Rejected(RequestRejection::StaleSession { current: epoch(2) })
    );

    // A newer epoch clears deduplication state and nothing else. The lock the
    // old session took is still held, by the same client, under the same token.
    cluster.settle();
    let status = answered(cluster.get_lock(leader, resource(RESOURCE)));
    let holder = status.holder.expect("locks outlive sessions");
    assert_eq!(holder.owner, client(0));
    assert_eq!(holder.token, token(1));

    // The replacement session starts its sequence over and may still release.
    let released = committed(&mut cluster, leader, submit(0, 2, 1, release(RESOURCE, 1)));
    assert_eq!(released, LockResponse::Operation(OperationResult::Released));

    cluster.settle();
    assert_replicas_agree(&cluster, leader);
    assert_history_agrees_with_oracle(&cluster, leader);
}

#[test]
fn a_follower_cannot_serve_the_linearizable_query_itself() {
    let mut cluster = LockCluster::new(config(4, 4));
    let leader = cluster.elect_leader();
    let follower = other_node(&cluster, leader);
    committed(&mut cluster, leader, open_session(0, 1));
    committed(&mut cluster, leader, submit(0, 1, 1, acquire(RESOURCE, 10)));
    cluster.settle();

    let outcome = cluster.get_lock(follower, resource(RESOURCE));
    assert!(
        matches!(outcome, QueryOutcome::Unavailable { .. }),
        "a follower has no authority to grant a read barrier, got {outcome:?}"
    );
    assert!(
        matches!(
            cluster.get_lock(leader, resource(RESOURCE)),
            QueryOutcome::Answered { .. }
        ),
        "the leader still answers"
    );

    let watch = cluster
        .client(leader)
        .handle()
        .metrics()
        .expect("a handle opens a metrics watch for its own group");
    assert_eq!(
        watch.current().role,
        Role::Leader,
        "the handle observes its node through the managed metrics surface"
    );
}

#[test]
fn a_proposal_stranded_on_an_isolated_leader_is_dropped_and_retried_once() {
    let mut cluster = LockCluster::new(config(4, 4));
    let first_leader = cluster.elect_leader();
    committed(&mut cluster, first_leader, open_session(0, 1));
    cluster.settle();

    // Nothing leaves the leader at all, so the acquisition exists only in one
    // log and no majority has ever seen it.
    cluster.isolate(first_leader);
    let stranded = cluster.submit(first_leader, submit(0, 1, 1, acquire(RESOURCE, 10)));
    assert!(
        stranded.is_unknown(),
        "an isolated leader cannot prove anything about its proposal, got {stranded:?}"
    );

    let second_leader = cluster.elect_leader();
    assert_ne!(
        second_leader, first_leader,
        "the majority elected without it"
    );

    // The retry carries the same identity. Because the stranded entry never
    // committed, the session cache has nothing to replay and this executes for
    // the first time.
    let retry = cluster.submit(second_leader, submit(0, 1, 1, acquire(RESOURCE, 10)));
    assert_eq!(disposition(&retry), ApplyDisposition::Applied);
    let (retry_token, _) = acquisition(retry.committed().expect("the retry committed").response);
    assert_eq!(
        retry_token,
        token(1),
        "exactly one acquisition happened, so exactly one token was issued"
    );

    // Rejoining truncates the entry the former leader could never commit, and
    // the app layer says so rather than silently forgetting it.
    cluster.heal();
    cluster.run_rounds(6);
    cluster.settle();
    // The client had already stopped waiting, so this arrives on the future it
    // walked away from rather than through a driver counter: the app layer says
    // the outcome is lost, and the public client surface is where it is heard.
    assert!(
        cluster.runtime_unknown_outcomes(first_leader) > 0,
        "the former leader reported that it lost its proposal's outcome"
    );
    assert_replicas_agree(&cluster, second_leader);
    assert_history_agrees_with_oracle(&cluster, second_leader);
}

#[test]
fn a_refused_acquisition_is_recorded_as_provably_uncommitted() {
    let mut cluster = LockCluster::new(config(4, 4));
    let leader = cluster.elect_leader();
    let follower = other_node(&cluster, leader);
    committed(&mut cluster, leader, open_session(0, 1));
    cluster.settle();

    // The client aims its first acquisition at a replica that does not lead.
    // That replica refuses in its own admission check, before appending
    // anything, so no peer ever holds a copy of these bytes.
    let attempt = submit(0, 1, 1, acquire(RESOURCE, 10));
    let refused = cluster.submit(follower, attempt);
    let SubmitOutcome::Refused { error } = &refused else {
        panic!("a follower refuses a proposal before replicating it, got {refused:?}");
    };
    assert!(
        matches!(error, WriteError::NotLeader { leader_hint, .. } if *leader_hint == Some(leader)),
        "the service reported a pre-append refusal and redirected, got {error:?}"
    );
    // The stronger terminal event rests on exactly this: the driver reported
    // the fate it observed, rather than this application inferring one from the
    // category. `NotAppended` is what makes the request identity still unused.
    assert_eq!(error.kind(), WriteErrorKind::NotLeader);
    assert_eq!(error.fate(), WriteFate::NotAppended);
    assert!(!error.fate().may_commit());

    let refused_id = cluster
        .history()
        .iter()
        .find_map(|event| match event {
            HistoryEvent::NotCommitted { operation_id } => Some(*operation_id),
            _ => None,
        })
        .expect("a provable refusal earns the stronger terminal event, not merely `Unknown`");
    assert!(
        cluster.history().iter().any(|event| matches!(
            event,
            HistoryEvent::Invoked {
                operation_id,
                command,
            } if *operation_id == refused_id && *command == attempt
        )),
        "the terminal event names the acquisition that never replicated"
    );

    // The stronger event is honest only if the acquisition really is absent
    // everywhere. Three client-visible facts say so: the resource is untracked,
    // the session still expects the sequence the refusal carried, and
    // resubmitting that identity executes rather than replaying a cached
    // acquisition.
    let status = answered(cluster.get_lock(leader, resource(RESOURCE)));
    assert_eq!(status.holder, None, "no tenure opened");
    assert_eq!(
        status.token_floor, None,
        "a refused acquisition mints no fencing token, so the name stays untracked"
    );

    let gap = committed(&mut cluster, leader, submit(0, 1, 2, acquire(RESOURCE, 10)));
    assert_eq!(
        gap,
        LockResponse::Rejected(RequestRejection::SequenceGap {
            expected: sequence(1),
        }),
        "the replicated session never consumed the refused sequence"
    );

    let accepted = cluster.submit(leader, attempt);
    assert_eq!(
        disposition(&accepted),
        ApplyDisposition::Applied,
        "the request identity was still unused, so the retry executed"
    );
    let (issued, _) = acquisition(accepted.committed().expect("the retry committed").response);
    assert_eq!(
        issued,
        token(1),
        "the resource's first accepted tenure gets token 1"
    );

    cluster.settle();
    assert_eq!(
        cluster
            .history()
            .iter()
            .filter(|event| matches!(event, HistoryEvent::NotCommitted { .. }))
            .count(),
        1,
        "exactly one operation was refused; the rest reached the log"
    );
    assert!(
        !cluster
            .history()
            .iter()
            .any(|event| matches!(event, HistoryEvent::Unknown { .. })),
        "no outcome was lost here, so the weaker terminal event must not appear"
    );
    assert_replicas_agree(&cluster, leader);
    assert_history_agrees_with_oracle(&cluster, leader);
}

#[test]
fn a_restarted_replica_recovers_its_locks_and_keeps_replicating() {
    let mut cluster = LockCluster::new(config(4, 4));
    let leader = cluster.elect_leader();
    committed(&mut cluster, leader, open_session(0, 1));
    committed(&mut cluster, leader, submit(0, 1, 1, acquire(RESOURCE, 10)));
    cluster.settle();

    let follower = other_node(&cluster, leader);
    let before = cluster.state_machine(follower).service().view();
    let applied_before = cluster.applied_index(follower);
    let committed_before = cluster.committed_application_index(follower);
    assert!(
        committed_before > LogIndex::ZERO,
        "the replica has committed application entries to recover"
    );

    cluster.restart(follower);
    assert_eq!(
        cluster.state_machine(follower).service().view(),
        before,
        "the reopened replica recovered its lock table, sessions, and marks"
    );
    assert_eq!(
        cluster.applied_index(follower),
        applied_before,
        "the durable applied floor came back with the data"
    );
    // The new incarnation recovered from the stores the retired runtime handed
    // back, so it knows exactly the same committed application entries. A
    // restart that opened a different medium would report a lower floor here
    // and the readiness comparison would silently pass on an empty replica.
    assert_eq!(
        cluster.committed_application_index(follower),
        committed_before,
        "the reopened runtime recovered from the retired one's durable stores"
    );

    committed(&mut cluster, leader, submit(0, 1, 2, release(RESOURCE, 1)));
    committed(&mut cluster, leader, submit(0, 1, 3, acquire(RESOURCE, 10)));
    cluster.settle();
    assert_eq!(
        cluster
            .state_machine(follower)
            .service()
            .status(resource(RESOURCE))
            .token_floor,
        Some(token(2)),
        "the reopened replica kept replicating and the mark advanced once"
    );
    assert_replicas_agree(&cluster, leader);
}

/// Submits one command and asserts that it committed, returning its response.
fn committed(cluster: &mut LockCluster, node_id: NodeId, command: Command) -> LockResponse {
    match cluster.submit(node_id, command) {
        SubmitOutcome::Completed { outcome, .. } => outcome.response,
        other => panic!("expected a committed outcome, got {other:?}"),
    }
}

fn disposition(outcome: &SubmitOutcome) -> ApplyDisposition {
    outcome
        .committed()
        .expect("expected a committed outcome")
        .disposition
}

fn acquisition(response: LockResponse) -> (FencingToken, LogicalTime) {
    match response {
        LockResponse::Operation(OperationResult::Acquired { token, expiry }) => (token, expiry),
        other => panic!("expected an acquisition, observed {other:?}"),
    }
}

fn answered(outcome: QueryOutcome<cluster::LockGroupId>) -> ResourceStatus {
    match outcome {
        QueryOutcome::Answered { status, .. } => status,
        QueryOutcome::Unavailable { error } => {
            panic!("expected a fresh answer, got {error:?}")
        }
    }
}

fn other_node(cluster: &LockCluster, node_id: NodeId) -> NodeId {
    cluster
        .node_ids()
        .into_iter()
        .find(|candidate| *candidate != node_id)
        .expect("a three-node cluster has other nodes")
}

/// Asserts every reachable replica holds the same lock state.
fn assert_replicas_agree(cluster: &LockCluster, leader: NodeId) {
    let expected = cluster.state_machine(leader).service().view();
    for node_id in cluster.node_ids() {
        assert_eq!(
            cluster.state_machine(node_id).service().view(),
            expected,
            "replica {node_id} diverged"
        );
    }
}

/// Replays the real committed command sequence through the independent oracle
/// and checks every terminal client response against it.
///
/// Operations that ended in an unknown outcome constrain nothing, which is
/// exactly what the contract's `Unknown` event means.
fn assert_history_agrees_with_oracle(cluster: &LockCluster, node_id: NodeId) {
    let mut oracle = ReferenceLockService::new(cluster.config());
    let replayed = cluster
        .committed_commands(node_id)
        .into_iter()
        .map(|command| (command, oracle.apply(command).response))
        .collect::<Vec<_>>();

    assert_eq!(
        cluster.state_machine(node_id).service().view(),
        oracle.view(),
        "the replicated lock service diverged from the independent oracle"
    );

    let mut invoked = BTreeMap::<OperationId, Command>::new();
    for event in cluster.history() {
        if let HistoryEvent::Invoked {
            operation_id,
            command,
        } = event
        {
            invoked.insert(*operation_id, *command);
        }
    }

    let mut checked = 0_usize;
    for event in cluster.history() {
        let HistoryEvent::Completed {
            operation_id,
            response,
        } = event
        else {
            continue;
        };
        let command = invoked
            .get(operation_id)
            .expect("every completion follows its invocation");
        assert!(
            replayed
                .iter()
                .any(
                    |(replayed_command, replayed_response)| replayed_command == command
                        && replayed_response == response
                ),
            "no committed execution of {command:?} produced the observed response {response:?}"
        );
        checked += 1;
    }
    assert!(checked > 0, "the history checked no completed operations");
}
