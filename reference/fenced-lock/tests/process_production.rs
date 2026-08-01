//! Authenticated, bounded, durable production-composition acceptance.
//!
//! These ignored tests are intentionally separate from the insecure integration
//! process suite. They prove one concrete caller-owned composition, not a
//! generic server or transport product.

#![allow(clippy::wildcard_imports)]

#[allow(
    dead_code,
    reason = "the production harness reuses its independent wire decoder"
)]
#[path = "support/process.rs"]
mod process;
#[path = "support/production_process.rs"]
mod production_process;
#[path = "support/public_transport.rs"]
mod public_transport;
#[path = "support/scratch.rs"]
mod scratch;
#[allow(
    dead_code,
    reason = "the production suite uses a subset of shared command builders"
)]
mod support;

use std::{io::Write, net::TcpStream};

use rafter::{LogIndex, NodeId};
use rafter_reference_fenced_lock::{
    check_evidence,
    production::{
        allocate_replica, load_active_replica, retire_replica, transport_peer_id,
        AllocationCrashPoint, IdentityError, ReplicaIdentity,
    },
    ApplyDisposition, FencingToken, GuardedRejection, GuardedWrite, LockConfig, LockResponse,
    OperationResult, RecordingGuardedResource,
};
use rafter_transport_tls::{
    FileTransportSessionStore, ServerHelloStatus, ServerRefusal, TransportSessionStore,
};

use process::{QueuedRequest, SubmitOutcome};
use production_process::{wait_until, ProductionCluster, ProductionNode, NODE_IDS};
use support::{acquire, config, expire_through, open_session, resource, submit};

const GROUP_ID: u64 = 1;

fn process_config() -> LockConfig {
    config(4, 4)
}

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

#[track_caller]
fn assert_operation(outcome: &SubmitOutcome, expected: OperationResult) {
    assert_eq!(outcome.operation(), expected);
}

fn guarded_write(
    guarded: &mut RecordingGuardedResource,
    token: FencingToken,
    value: u64,
) -> Result<u64, GuardedRejection> {
    guarded.apply(GuardedWrite {
        resource: guarded.resource(),
        token,
        value,
    })
}

#[test]
#[ignore = "spawns authenticated real processes; run with --ignored"]
fn authenticated_cluster_serves_lock_and_exposes_bounded_operations_evidence() {
    let mut cluster = ProductionCluster::start("production-service", process_config());
    let leader = cluster.wait_for_leader();

    for node_id in NODE_IDS {
        let observation = cluster.observe(node_id);
        assert!(observation.boolean("ready"));
        assert_eq!(observation.number("frame_limit"), 2_163_089);
        assert_eq!(observation.number("replay_window"), 1);
        assert_eq!(observation.number("outbound_limit"), 256);
        assert_eq!(observation.number("inbound_peer_limit"), 128);
        assert_eq!(observation.number("inbound_global_limit"), 512);
        assert_eq!(observation.number("peer_connection_limit"), 16);
        assert_eq!(observation.number("client_connection_limit"), 16);
        assert_eq!(observation.number("client_pending_limit"), 64);
        assert!(
            observation.number("control_plane_epoch") > 0,
            "readiness follows a durable control-plane publication"
        );
    }
    wait_until("authenticated cluster channels to connect", || {
        let authenticated = cluster
            .live_nodes()
            .into_iter()
            .map(|node| cluster.observe(node).number("authenticated_connections"))
            .sum::<u64>();
        (authenticated >= 2).then_some(())
    });

    let vault = resource("vault");
    let mut guarded = RecordingGuardedResource::new(vault);
    assert_eq!(
        applied(&cluster.submit_to_leader(open_session(0, 1))).0,
        ApplyDisposition::SessionOpened
    );
    let first = cluster
        .submit_to_leader(submit(0, 1, 1, acquire("vault", 10)))
        .acquired_token();
    assert_eq!(guarded_write(&mut guarded, first, 11), Ok(11));

    cluster.submit_to_leader(open_session(1, 1));
    let expired = cluster.submit_to_leader(submit(1, 1, 1, expire_through(10)));
    assert_operation(
        &expired,
        OperationResult::Expired {
            released_locks: 1,
            logical_time: support::time(10),
        },
    );
    cluster.submit_to_leader(open_session(2, 1));
    let second = cluster
        .submit_to_leader(submit(2, 1, 1, acquire("vault", 10)))
        .acquired_token();
    assert!(second > first);
    assert_eq!(guarded_write(&mut guarded, second, 22), Ok(22));
    assert_eq!(
        guarded_write(&mut guarded, first, 99),
        Err(GuardedRejection::StaleFencingToken {
            highest_accepted: second
        })
    );
    assert_eq!(cluster.query(vault).view().held_token, Some(second.get()));

    let report = check_evidence(process_config(), cluster.history(), guarded.history())
        .unwrap_or_else(|error| panic!("independent production evidence failed: {error}"));
    assert!(report.lock.searched_operations() + report.lock.discharged_operations() > 0);
    assert!(report.guarded.checked_operations() > 0);
    assert_eq!(cluster.wait_for_leader(), leader);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns authenticated real processes; run with --ignored"]
fn peer_authentication_replay_and_removed_identity_refuse_before_raft() {
    let mut cluster = ProductionCluster::start("production-security", process_config());
    let target = NodeId(1);
    let peer_two = NodeId(2);
    let root = cluster.root().to_path_buf();
    let peer_addr = cluster.node_mut(target).peer_addr(&root);

    assert_authentication_refusals(&mut cluster, target, peer_two, peer_addr);
    let sessions = assert_replay_refusals(&mut cluster, target, peer_two, peer_addr, &root);
    remove_peer_and_assert_refused(&mut cluster, target, peer_two, &root, &sessions);
    cluster.shutdown();
}

fn assert_authentication_refusals(
    cluster: &mut ProductionCluster,
    target: NodeId,
    peer_two: NodeId,
    peer_addr: std::net::SocketAddr,
) {
    let before = cluster.observe(target).number("applied_index");

    let mut unauthenticated = TcpStream::connect(peer_addr).expect("peer listener accepts TCP");
    unauthenticated
        .write_all(b"not a TLS handshake")
        .expect("bad handshake bytes reach the listener");
    drop(unauthenticated);
    wait_counter(cluster, target, "authentication_failed", 1);

    public_transport::open_authenticated(peer_addr, NodeId(9));
    wait_counter(cluster, target, "unknown_certificate", 1);

    let refused = public_transport::send_sequences(
        peer_addr,
        public_transport::SequenceAttempt::authenticated(peer_two, NodeId(3), target, 1),
        &[],
    );
    assert_eq!(
        refused.status(),
        ServerHelloStatus::Refused(ServerRefusal::IdentityMismatch)
    );
    wait_counter(cluster, target, "identity_mismatch", 1);
    assert_eq!(
        cluster.observe(target).number("applied_index"),
        before,
        "authentication refusals never reached the application"
    );
}

fn assert_replay_refusals(
    cluster: &mut ProductionCluster,
    target: NodeId,
    peer_two: NodeId,
    peer_addr: std::net::SocketAddr,
    root: &std::path::Path,
) -> FileTransportSessionStore {
    cluster.stop(peer_two);
    let sessions = cluster.session_store(peer_two);
    let target_peer = transport_peer_id(target).expect("fixture peer identity");
    let first = sessions
        .allocate_outbound_session(&target_peer)
        .expect("first durable session");
    let second = sessions
        .allocate_outbound_session(&target_peer)
        .expect("second durable session");
    let third = sessions
        .allocate_outbound_session(&target_peer)
        .expect("third durable session");
    let fourth = sessions
        .allocate_outbound_session(&target_peer)
        .expect("fourth durable session");

    public_transport::send_sequences(
        peer_addr,
        public_transport::SequenceAttempt::authenticated(peer_two, peer_two, target, first.get())
            .with_outer_from(NodeId(3)),
        &[1],
    );
    wait_counter(cluster, target, "identity_mismatch", 2);

    public_transport::send_sequences(
        peer_addr,
        public_transport::SequenceAttempt::authenticated(peer_two, peer_two, target, second.get()),
        &[1, 1],
    );
    wait_counter(cluster, target, "replay_duplicate", 1);

    public_transport::send_sequences(
        peer_addr,
        public_transport::SequenceAttempt::authenticated(peer_two, peer_two, target, third.get()),
        &[1, 3],
    );
    wait_counter(cluster, target, "replay_outside_window", 2);

    public_transport::send_sequences(
        peer_addr,
        public_transport::SequenceAttempt::authenticated(peer_two, peer_two, target, fourth.get()),
        &[1],
    );
    let stale = public_transport::send_sequences(
        peer_addr,
        public_transport::SequenceAttempt::authenticated(peer_two, peer_two, target, third.get()),
        &[],
    );
    assert_eq!(
        stale.status(),
        ServerHelloStatus::Refused(ServerRefusal::StaleSession)
    );
    wait_counter(cluster, target, "replay_stale_session", 1);

    cluster.stop(target);
    cluster.restart(target, &NODE_IDS);
    let restarted_addr = cluster.node_mut(target).peer_addr(root);
    let stale_after_restart = public_transport::send_sequences(
        restarted_addr,
        public_transport::SequenceAttempt::authenticated(peer_two, peer_two, target, fourth.get()),
        &[],
    );
    assert_eq!(
        stale_after_restart.status(),
        ServerHelloStatus::Refused(ServerRefusal::StaleSession)
    );
    wait_counter(cluster, target, "replay_stale_session", 1);
    sessions
}

fn remove_peer_and_assert_refused(
    cluster: &mut ProductionCluster,
    target: NodeId,
    peer_two: NodeId,
    root: &std::path::Path,
    sessions: &FileTransportSessionStore,
) {
    let target_peer = transport_peer_id(target).expect("fixture peer identity");
    assert_eq!(
        cluster.ask_leader("REMOVE_NODE 2"),
        "OK MEMBERSHIP_ACCEPTED"
    );
    let leader = cluster.wait_for_leader();
    wait_until("removal joint phase to commit", || {
        (cluster.observe(leader).string("committed_membership_phase") == "joint").then_some(())
    });
    assert_eq!(cluster.ask_leader("LEAVE_JOINT"), "OK MEMBERSHIP_ACCEPTED");
    cluster.wait_for_membership(leader, peer_two, false);
    retire_replica(&ReplicaIdentity::path(cluster.root(), peer_two), GROUP_ID)
        .expect("committed removal permanently retires identity 2");

    let fifth = sessions
        .allocate_outbound_session(&target_peer)
        .expect("post-removal durable session");
    cluster.stop(target);
    cluster.restart(target, &[NodeId(1), NodeId(3)]);
    let removed_addr = cluster.node_mut(target).peer_addr(root);
    public_transport::send_sequences(
        removed_addr,
        public_transport::SequenceAttempt::authenticated(peer_two, peer_two, target, fifth.get()),
        &[1],
    );
    wait_counter(cluster, target, "unauthorized_peer", 1);
    let restored = cluster.observe(target);
    assert!(!restored.contains_member("committed_members", peer_two));
    assert!(restored.number("control_plane_epoch") > 0);
}

#[test]
#[ignore = "spawns authenticated real processes; run with --ignored"]
fn readiness_and_checkpoint_recovery_fail_closed() {
    let mut cluster = ProductionCluster::start("production-recovery", process_config());
    cluster.submit_to_leader(open_session(0, 1));
    cluster.submit_to_leader(submit(0, 1, 1, acquire("vault", 10)));

    let mut contender =
        ProductionNode::spawn(cluster.root(), NodeId(3), &NODE_IDS, cluster.config());
    assert!(
        contender
            .ask("OPEN_SESSION 1 1")
            .expect("the contender listener answers")
            .starts_with("NOTREADY"),
        "a listening process does not serve before directory ownership and recovery"
    );
    cluster.stop(NodeId(3));
    let recovered = contender.wait_ready();
    assert!(recovered > LogIndex::ZERO);
    cluster.adopt(NodeId(3), contender);

    let checkpoint = cluster.root().join("node-3").join("control-plane");
    let original = std::fs::read(&checkpoint).expect("checkpoint exists after readiness");
    let before = cluster.observe(NodeId(3));
    cluster.stop(NodeId(3));
    std::fs::remove_file(&checkpoint).expect("the negative test removes the checkpoint");
    let mut missing = ProductionNode::spawn(cluster.root(), NodeId(3), &NODE_IDS, cluster.config());
    let (status, output) = missing.wait_refused();
    assert!(!status.success());
    assert!(output.contains("control-plane checkpoint"));
    assert!(output.contains("missing"));

    std::fs::write(&checkpoint, &original).expect("the fixture restores the checkpoint");
    std::fs::write(&checkpoint, b"corrupt checkpoint\n")
        .expect("the negative test corrupts the checkpoint");
    let mut corrupt = ProductionNode::spawn(cluster.root(), NodeId(3), &NODE_IDS, cluster.config());
    let (status, output) = corrupt.wait_refused();
    assert!(!status.success());
    assert!(output.contains("control-plane checkpoint"));
    assert!(output.contains("malformed"));

    std::fs::write(&checkpoint, &original).expect("the fixture restores the checkpoint again");

    let node_dir = cluster.root().join("node-3");
    let session_path = rafter_reference_fenced_lock::production::transport_session_path(&node_dir);
    let session_state = std::fs::read(&session_path).expect("transport state exists");

    std::fs::remove_file(&session_path).expect("the negative test removes transport state");
    let mut missing_transport =
        ProductionNode::spawn(cluster.root(), NodeId(3), &NODE_IDS, cluster.config());
    let (status, output) = missing_transport.wait_refused();
    assert!(!status.success());
    assert!(output.contains("transport session state"));
    assert!(output.contains("does not exist"));

    std::fs::write(&session_path, b"corrupt transport state\n")
        .expect("the negative test corrupts transport state");
    let mut corrupt_transport =
        ProductionNode::spawn(cluster.root(), NodeId(3), &NODE_IDS, cluster.config());
    let (status, output) = corrupt_transport.wait_refused();
    assert!(!status.success());
    assert!(output.contains("transport session state"));
    assert!(output.contains("could not decode") || output.contains("checksum"));

    std::fs::write(&session_path, session_state).expect("the fixture restores transport state");
    cluster.restart(NodeId(3), &NODE_IDS);
    let after = cluster.observe(NodeId(3));
    assert_eq!(
        after.string("committed_members"),
        before.string("committed_members")
    );
    assert!(after.number("control_plane_epoch") > 0);
    cluster.shutdown();
}

#[test]
#[ignore = "spawns authenticated real processes; run with --ignored"]
fn removal_spends_identity_and_replacement_joins_as_caught_up_learner() {
    let mut cluster = ProductionCluster::start("production-replacement", process_config());
    let replacement = allocate_replica(cluster.root(), GROUP_ID, AllocationCrashPoint::None)
        .expect("replacement allocation succeeds");
    assert_eq!(replacement.node_id, NodeId(4));
    cluster.start_process(NodeId(4), &NODE_IDS);
    assert_eq!(
        cluster.ask_leader("ADD_LEARNER 4"),
        "OK MEMBERSHIP_ACCEPTED"
    );
    let leader = cluster.wait_for_leader();
    cluster.wait_for_membership(leader, NodeId(4), true);
    let learner = cluster.observe(leader);
    assert!(
        !learner.contains_member("voters", NodeId(4)),
        "ADD_LEARNER must not promote the replacement: {learner:?}"
    );

    wait_until("replacement learner to catch up", || {
        let observation = cluster.observe(leader);
        let matched = observation.replication_match(NodeId(4))?;
        (matched.0 >= observation.number("commit_index")).then_some(matched)
    });
    cluster.wait_ready(NodeId(4));

    cluster.stop(NodeId(2));
    assert_eq!(
        cluster.ask_leader("REMOVE_NODE 2"),
        "OK MEMBERSHIP_ACCEPTED"
    );
    wait_until("removal joint phase to commit", || {
        (cluster.observe(leader).string("committed_membership_phase") == "joint").then_some(())
    });
    assert_eq!(cluster.ask_leader("LEAVE_JOINT"), "OK MEMBERSHIP_ACCEPTED");
    cluster.wait_for_membership(leader, NodeId(2), false);
    let retired_path = ReplicaIdentity::path(cluster.root(), NodeId(2));
    retire_replica(&retired_path, GROUP_ID).expect("removal retires the old identity");
    assert!(matches!(
        load_active_replica(cluster.root(), &retired_path, GROUP_ID),
        Err(IdentityError::Retired { node_id: NodeId(2) })
    ));

    let barrier = wait_until("replacement learner to remain caught up", || {
        let observation = cluster.observe(leader);
        let matched = observation.replication_match(NodeId(4))?;
        (matched.0 >= observation.number("commit_index")).then_some(matched)
    });
    assert_eq!(
        cluster.ask_leader(&format!("PROMOTE_LEARNER 4 {}", barrier.0)),
        "OK MEMBERSHIP_ACCEPTED"
    );
    wait_until("promotion joint phase to commit", || {
        (cluster.observe(leader).string("committed_membership_phase") == "joint").then_some(())
    });
    assert_eq!(cluster.ask_leader("LEAVE_JOINT"), "OK MEMBERSHIP_ACCEPTED");
    wait_until("replacement to become a voter", || {
        cluster
            .observe(leader)
            .contains_member("voters", NodeId(4))
            .then_some(())
    });

    let before = cluster.observe(NodeId(4));
    assert_eq!(before.optional_number("committed_id_high_water"), Some(4));
    cluster.stop(NodeId(4));
    cluster.restart(NodeId(4), &[NodeId(1), NodeId(3), NodeId(4)]);
    let after = cluster.observe(NodeId(4));
    assert!(after.contains_member("voters", NodeId(4)));
    assert_eq!(after.optional_number("committed_id_high_water"), Some(4));
    cluster.shutdown();
}

#[test]
#[ignore = "spawns authenticated real processes; run with --ignored"]
fn client_connection_overflow_is_counted_and_accepted_work_is_answered() {
    let mut cluster = ProductionCluster::start("production-client-bound", process_config());
    let target = cluster.wait_for_leader();
    let addr = cluster.node_mut(target).client_addr();
    let mut accepted = Vec::new();
    for expected in 2..=16 {
        accepted.push(QueuedRequest::connect(addr));
        wait_until("accepted client connection count", || {
            (cluster.observe(target).number("client_active") >= expected).then_some(())
        });
    }

    let _overflow = TcpStream::connect(addr).expect("the listener accepts before enforcing bound");
    wait_counter(&mut cluster, target, "client_connection_full", 1);
    for request in &mut accepted {
        request.send("STATUS");
    }
    for request in &mut accepted {
        assert!(request
            .recv()
            .expect("accepted work receives a terminal answer")
            .starts_with("STATUS "));
    }
    cluster.shutdown();
}

fn wait_counter(cluster: &mut ProductionCluster, node: NodeId, name: &str, minimum: u64) {
    wait_until(&format!("{name} counter to reach {minimum}"), || {
        let observation = cluster.observe(node);
        (observation.number(name) >= minimum).then_some(())
    });
}
