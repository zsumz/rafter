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
#[path = "support/scratch.rs"]
mod scratch;
#[allow(
    dead_code,
    reason = "the production suite uses a subset of shared command builders"
)]
mod support;

use std::{io::Write, net::TcpStream, sync::Arc};

use rafter::{LogIndex, Message, NodeId, PreVote, Term};
use rafter_codec::encode_message;
use rafter_reference_fenced_lock::{
    check_evidence,
    production::{
        allocate_replica, load_active_replica, retire_replica, AllocationCrashPoint, IdentityError,
        ReplicaIdentity,
    },
    ApplyDisposition, FencingToken, GuardedRejection, GuardedWrite, LockConfig, LockResponse,
    OperationResult, RecordingGuardedResource,
};
use rustls::{
    pki_types::{CertificateDer, PrivateKeyDer, ServerName},
    ClientConfig, ClientConnection, RootCertStore, StreamOwned,
};

use process::{QueuedRequest, SubmitOutcome};
use production_process::{fixture_dir, wait_until, ProductionCluster, ProductionNode, NODE_IDS};
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
        assert_eq!(observation.number("replay_window"), 64);
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
    let root = cluster.root().to_path_buf();
    let peer_addr = cluster.node_mut(target).peer_addr(&root);
    let before = cluster.observe(target).number("applied_index");

    let mut unauthenticated = TcpStream::connect(peer_addr).expect("peer listener accepts TCP");
    unauthenticated
        .write_all(b"not a TLS handshake")
        .expect("bad handshake bytes reach the listener");
    drop(unauthenticated);
    wait_counter(&mut cluster, target, "authentication_failed", 1);

    open_authenticated(peer_addr, NodeId(9));
    wait_counter(&mut cluster, target, "unknown_certificate", 1);

    send_frame(peer_addr, NodeId(2), NodeId(3), NodeId(3), 1, 1);
    wait_counter(&mut cluster, target, "identity_mismatch", 1);
    assert_eq!(
        cluster.observe(target).number("applied_index"),
        before,
        "authentication refusals never reached the application"
    );

    cluster.stop(NodeId(2));
    let replay = cluster.replay_store(NodeId(2));
    let first_session = replay.allocate_session().expect("fresh durable session");
    let second_session = replay.allocate_session().expect("next durable session");

    send_frame(peer_addr, NodeId(2), NodeId(2), NodeId(2), first_session, 1);
    send_frame(peer_addr, NodeId(2), NodeId(2), NodeId(2), first_session, 1);
    wait_counter(&mut cluster, target, "replay_duplicate", 1);
    send_frame(
        peer_addr,
        NodeId(2),
        NodeId(2),
        NodeId(2),
        first_session,
        65,
    );
    send_frame(peer_addr, NodeId(2), NodeId(2), NodeId(2), first_session, 1);
    wait_counter(&mut cluster, target, "replay_outside_window", 1);
    send_frame(
        peer_addr,
        NodeId(2),
        NodeId(2),
        NodeId(2),
        second_session,
        1,
    );
    send_frame(
        peer_addr,
        NodeId(2),
        NodeId(2),
        NodeId(2),
        first_session,
        66,
    );
    wait_counter(&mut cluster, target, "replay_stale_session", 1);

    cluster.stop(target);
    cluster.restart(target, &NODE_IDS);
    let restarted_addr = cluster.node_mut(target).peer_addr(&root);
    send_frame(
        restarted_addr,
        NodeId(2),
        NodeId(2),
        NodeId(2),
        second_session,
        1,
    );
    wait_counter(&mut cluster, target, "replay_duplicate", 1);

    assert_eq!(
        cluster.ask_leader("REMOVE_NODE 2"),
        "OK MEMBERSHIP_ACCEPTED"
    );
    let leader = cluster.wait_for_leader();
    wait_until("removal joint phase to commit", || {
        (cluster.observe(leader).string("committed_membership_phase") == "joint").then_some(())
    });
    assert_eq!(cluster.ask_leader("LEAVE_JOINT"), "OK MEMBERSHIP_ACCEPTED");
    cluster.wait_for_membership(leader, NodeId(2), false);
    retire_replica(&ReplicaIdentity::path(cluster.root(), NodeId(2)), GROUP_ID)
        .expect("committed removal permanently retires identity 2");

    cluster.stop(target);
    cluster.restart(target, &[NodeId(1), NodeId(3)]);
    let removed_addr = cluster.node_mut(target).peer_addr(&root);
    send_frame(
        removed_addr,
        NodeId(2),
        NodeId(2),
        NodeId(2),
        second_session + 1,
        1,
    );
    wait_counter(&mut cluster, target, "unauthorized_peer", 1);
    let restored = cluster.observe(target);
    assert!(!restored.contains_member("committed_members", NodeId(2)));
    assert!(restored.number("control_plane_epoch") > 0);
    cluster.shutdown();
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

    std::fs::write(&checkpoint, original).expect("the fixture restores the checkpoint again");
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

fn open_authenticated(addr: std::net::SocketAddr, certificate_node: NodeId) {
    let config = tls_client_config(certificate_node);
    let connection = ClientConnection::new(
        config,
        ServerName::try_from("rafter-peer").expect("fixture server name is valid"),
    )
    .expect("TLS client builds");
    let socket = TcpStream::connect(addr).expect("authenticated peer TCP connects");
    let mut stream = StreamOwned::new(connection, socket);
    while stream.conn.is_handshaking() {
        stream
            .conn
            .complete_io(&mut stream.sock)
            .expect("the test TLS handshake completes");
    }
}

fn send_frame(
    addr: std::net::SocketAddr,
    certificate_node: NodeId,
    outer_from: NodeId,
    embedded_from: NodeId,
    session: u64,
    sequence: u64,
) {
    let config = tls_client_config(certificate_node);
    let connection = ClientConnection::new(
        config,
        ServerName::try_from("rafter-peer").expect("fixture server name is valid"),
    )
    .expect("TLS client builds");
    let socket = TcpStream::connect(addr).expect("authenticated peer TCP connects");
    let mut stream = StreamOwned::new(connection, socket);
    let message = Message::PreVote(PreVote {
        term: Term(0),
        candidate_id: embedded_from,
        last_log_index: LogIndex::ZERO,
        last_log_term: Term(0),
    });
    let payload = encode_message(&message).expect("test peer message encodes");
    let body_len = 49 + payload.len();
    let mut frame = Vec::with_capacity(body_len + 4);
    frame.extend_from_slice(
        &u32::try_from(body_len)
            .expect("test frame length fits")
            .to_be_bytes(),
    );
    frame.extend_from_slice(b"RFTP");
    frame.push(1);
    frame.extend_from_slice(&GROUP_ID.to_be_bytes());
    frame.extend_from_slice(&outer_from.0.to_be_bytes());
    frame.extend_from_slice(&NodeId(1).0.to_be_bytes());
    frame.extend_from_slice(&session.to_be_bytes());
    frame.extend_from_slice(&sequence.to_be_bytes());
    frame.extend_from_slice(
        &u32::try_from(payload.len())
            .expect("test payload length fits")
            .to_be_bytes(),
    );
    frame.extend_from_slice(&payload);
    stream.write_all(&frame).expect("test frame writes");
    stream.flush().expect("test frame flushes");
}

fn tls_client_config(node_id: NodeId) -> Arc<ClientConfig> {
    let fixtures = fixture_dir();
    let mut roots = RootCertStore::empty();
    let mut ca = std::io::BufReader::new(
        std::fs::File::open(fixtures.join("ca.pem")).expect("test CA opens"),
    );
    let certificates = rustls_pemfile::certs(&mut ca)
        .collect::<Result<Vec<_>, _>>()
        .expect("test CA parses");
    roots.add_parsable_certificates(certificates);

    let mut certificate = std::io::BufReader::new(
        std::fs::File::open(fixtures.join(format!("node-{}.pem", node_id.0)))
            .expect("test leaf opens"),
    );
    let certificate: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut certificate)
        .collect::<Result<_, _>>()
        .expect("test leaf parses");
    let mut key = std::io::BufReader::new(
        std::fs::File::open(fixtures.join(format!("node-{}-key.pem", node_id.0)))
            .expect("test key opens"),
    );
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key)
        .expect("test key parses")
        .expect("test key exists");
    Arc::new(
        ClientConfig::builder()
            .with_root_certificates(roots)
            .with_client_auth_cert(certificate, key)
            .expect("test client certificate and key agree"),
    )
}
