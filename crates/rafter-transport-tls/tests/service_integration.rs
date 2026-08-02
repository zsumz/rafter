mod support;

use std::{error::Error, fmt, time::Duration};

use rafter::{LogIndex, Message, NodeConfig, NodeId};
use rafter_app::{
    group::RaftGroup,
    state_machine::{
        ApplyBatch, ApplyResult, ReadBarrier, ReplicatedStateMachine, SnapshotSupport,
    },
};
use rafter_runtime::DurableRaftNode;
use rafter_service::{PeerPolicy, RaftTransport, TransportDriverOptions, TransportRaftDriver};
use rafter_storage::InMemoryRaftHardStateStore;
use rafter_transport_tls::{
    PeerAuthorization, PeerEndpoint, PeerId, RuntimeLimits, TlsTransportError,
};
use support::runtime::{wait_until, RuntimeFixture, DEFAULT_ROUTE, GROUP_ID, NODE_A, NODE_B};
use support::tls::server_name;

#[test]
fn tls_directory_and_sender_compose_with_one_managed_transport_driver() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let placeholder = "127.0.0.1:9".parse().expect("placeholder address");
    let sender_endpoints = fixture.endpoints_to_b(placeholder);
    let sender = fixture.start_a(sender_endpoints.clone());

    let directory = fixture.bound_directory(&[DEFAULT_ROUTE]);
    let receiver = fixture.start_b_with_directory(
        fixture.endpoints_to_a(sender.local_addr()),
        directory.clone(),
    );
    let driver = TransportRaftDriver::new(
        test_group(),
        Vec::new(),
        receiver.sender(),
        directory.clone(),
        TransportDriverOptions::default(),
    )
    .expect("the concrete transport and validator compose with the service driver");

    sender_endpoints
        .replace(
            fixture.peer_b().clone(),
            vec![PeerEndpoint::new(receiver.local_addr(), server_name())],
        )
        .expect("publish the live receiver endpoint");
    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("outbound vote is admitted");

    let mut authenticated = None;
    assert!(wait_until(Duration::from_secs(3), || {
        let mut drained = receiver.inbound().drain(1).expect("inbound queue");
        authenticated = drained.pop();
        authenticated.is_some()
    }));
    driver
        .deliver(authenticated.expect("one authenticated envelope"))
        .expect("the service validator admits the authenticated member");

    let mut response = None;
    assert!(wait_until(Duration::from_secs(3), || {
        let mut drained = sender.inbound().drain(1).expect("inbound queue");
        response = drained.pop();
        response.is_some()
    }));
    let response = response.expect("the concrete driver routes its vote response");
    assert_eq!(response.group_id, GROUP_ID.to_owned());
    assert_eq!(response.authenticated_peer, fixture.peer_b().clone());
    assert_eq!(response.raft_from, NODE_B);
    assert_eq!(response.raft_to, NODE_A);
    let Message::RequestVoteResponse(response) = response.message else {
        panic!("the managed driver must emit a vote response through TlsSender");
    };
    assert_eq!(response.voter_id, NODE_B);

    let policy = directory
        .policy(&GROUP_ID.to_owned())
        .expect("directory remains readable")
        .expect("driver published its initial policy");
    assert_eq!(policy.authorized_peers(), &[fixture.peer_a().clone()]);
    assert_eq!(
        directory
            .authorization(&GROUP_ID.to_owned(), NODE_A)
            .expect("authorization is readable"),
        PeerAuthorization::Authorized
    );

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[test]
fn peer_policy_update_is_atomic_when_a_principal_has_no_sender_worker() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let directory = fixture.bound_directory(&[DEFAULT_ROUTE]);
    let unavailable = "127.0.0.1:9"
        .parse()
        .expect("unavailable loopback endpoint");
    let transport =
        fixture.start_b_with_directory(fixture.endpoints_to_a(unavailable), directory.clone());
    let sender = transport.sender();
    let group_id = GROUP_ID.to_owned();
    let initial = PeerPolicy::new(vec![fixture.peer_a().clone()], Some(NODE_B));
    sender
        .update_peers(&group_id, initial)
        .expect("initial complete policy is installed");

    let peer_c = PeerId::new("peer-c").expect("valid unconfigured principal");
    assert!(matches!(
        sender.update_peers(
            &group_id,
            PeerPolicy::new(vec![peer_c.clone()], Some(NodeId(3))),
        ),
        Err(TlsTransportError::EndpointUnavailable { peer }) if peer == peer_c
    ));

    let policy = directory
        .policy(&group_id)
        .expect("directory remains readable")
        .expect("the prior policy remains installed");
    assert_eq!(policy.authorized_peers(), &[fixture.peer_a().clone()]);
    assert_eq!(policy.retirement_floor(), Some(NODE_B));

    transport.join().expect("transport joins");
}

#[test]
fn committed_retirement_revokes_replication_queued_while_paused() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b();
    let sender = fixture.bind_paused_a_with_store(
        fixture.endpoints_to_b(receiver.local_addr()),
        support::session_store::MemorySessionStore::new(),
    );

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("replication is queued while workers are paused");
    sender
        .sender()
        .update_peers(
            &GROUP_ID.to_owned(),
            PeerPolicy::new(Vec::new(), Some(NODE_B)),
        )
        .expect("destination is retired before activation");
    sender.start().expect("activate sender");

    assert!(wait_until(Duration::from_secs(3), || {
        sender.diagnostics().retired_queued_frames == 1
    }));
    assert!(receiver
        .inbound()
        .drain(1)
        .expect("inbound queue")
        .is_empty());

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

#[test]
fn live_connection_rechecks_policy_and_refuses_a_retired_group_identity() {
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let placeholder = "127.0.0.1:9".parse().expect("placeholder address");
    let sender_endpoints = fixture.endpoints_to_b(placeholder);
    let sender = fixture.start_a(sender_endpoints.clone());

    let directory = fixture.bound_directory(&[DEFAULT_ROUTE]);
    directory
        .replace_policy(
            &GROUP_ID.to_owned(),
            PeerPolicy::new(vec![fixture.peer_a().clone()], Some(NODE_B)),
        )
        .expect("authorize peer A initially");
    let receiver = fixture.start_b_with_directory(
        fixture.endpoints_to_a(sender.local_addr()),
        directory.clone(),
    );
    sender_endpoints
        .replace(
            fixture.peer_b().clone(),
            vec![PeerEndpoint::new(receiver.local_addr(), server_name())],
        )
        .expect("publish the live receiver endpoint");

    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("first vote is admitted");
    assert!(wait_until(Duration::from_secs(3), || {
        receiver
            .inbound()
            .depth()
            .is_ok_and(|(frames, _)| frames == 1)
    }));
    receiver.inbound().drain(1).expect("drain first vote");

    receiver
        .sender()
        .update_peers(
            &GROUP_ID.to_owned(),
            PeerPolicy::new(Vec::new(), Some(NODE_B)),
        )
        .expect("install the complete retired policy");
    sender
        .sender()
        .send(RuntimeFixture::vote())
        .expect("sender still has its independent outbound policy");

    assert!(wait_until(Duration::from_secs(3), || {
        receiver.diagnostics().retired_peer_frames == 1
    }));
    assert!(receiver
        .inbound()
        .drain(1)
        .expect("inbound queue")
        .is_empty());
    assert_eq!(
        directory
            .authorization(&GROUP_ID.to_owned(), NODE_A)
            .expect("authorization is readable"),
        PeerAuthorization::Retired
    );
    assert!(directory
        .replace_policy(
            &GROUP_ID.to_owned(),
            PeerPolicy::new(vec![fixture.peer_a().clone()], Some(NODE_B)),
        )
        .is_err());

    sender.join().expect("sender joins");
    receiver.join().expect("receiver joins");
}

fn test_group() -> RaftGroup<String, TestStateMachine, DurableRaftNode> {
    let config = NodeConfig::new(NODE_B, vec![NODE_A], 3).expect("valid two-node config");
    let runtime = DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
        .expect("in-memory durable runtime opens");
    RaftGroup::new(
        GROUP_ID.to_owned(),
        NODE_B,
        runtime,
        TestStateMachine::default(),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct TestStateMachineError;

impl fmt::Display for TestStateMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("test state-machine error")
    }
}

impl Error for TestStateMachineError {}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct TestStateMachine {
    applied_index: LogIndex,
}

impl ReplicatedStateMachine for TestStateMachine {
    type Command = Vec<u8>;
    type CommandResult = ();
    type Query = ();
    type QueryResult = ();
    type Error = TestStateMachineError;

    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Unsupported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        Ok(command.clone())
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        Ok(payload.to_vec())
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        let mut results = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            self.applied_index = entry.index;
            results.push(ApplyResult {
                index: entry.index,
                term: entry.term,
                result: (),
                local_proposal_id: entry.local_proposal_id,
            });
        }
        Ok(results)
    }

    fn read(
        &self,
        _query: Self::Query,
        _barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        Ok(())
    }
}
