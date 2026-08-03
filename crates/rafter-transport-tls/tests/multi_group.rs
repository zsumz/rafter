mod support;

use std::time::Duration;

use rafter::NodeId;
use rafter_service::RaftTransport;
use rafter_transport_tls::{RuntimeLimits, TransportHealth, DEFAULT_MAX_GROUP_ID_BYTES};

use support::runtime::{wait_until, GroupRoute, RuntimeFixture};

#[test]
fn one_authenticated_connection_multiplexes_independently_numbered_groups() {
    let routes = [
        GroupRoute::new("group-a", NodeId(3), NodeId(17)),
        GroupRoute::new("group-b", NodeId(8), NodeId(4)),
    ];
    let fixture = RuntimeFixture::new(RuntimeLimits::default());
    let receiver = fixture.start_b_routes(&routes);
    let sender = fixture.start_a_routes(fixture.endpoints_to_b(receiver.local_addr()), &routes);

    assert!(wait_until(Duration::from_secs(3), || {
        sender.health() == TransportHealth::Ready
    }));
    for route in routes {
        sender
            .sender()
            .send(RuntimeFixture::vote_for(route))
            .expect("admit multiplexed frame");
    }

    let mut delivered = Vec::new();
    assert!(wait_until(Duration::from_secs(3), || {
        delivered.extend(receiver.inbound().drain(2).expect("drain groups"));
        delivered.len() == 2
    }));
    delivered.sort_by(|left, right| left.group_id.cmp(&right.group_id));
    assert_eq!(delivered[0].group_id, "group-a");
    assert_eq!(delivered[0].raft_from, NodeId(3));
    assert_eq!(delivered[0].raft_to, NodeId(17));
    assert_eq!(delivered[1].group_id, "group-b");
    assert_eq!(delivered[1].raft_from, NodeId(8));
    assert_eq!(delivered[1].raft_to, NodeId(4));
    assert_eq!(sender.diagnostics().active_outbound_connections, 1);
    assert_eq!(receiver.diagnostics().active_inbound_connections, 1);
    assert!(wait_until(Duration::from_secs(1), || {
        receiver
            .queue_depths()
            .is_ok_and(|depths| depths.inbound_memory_bytes == DEFAULT_MAX_GROUP_ID_BYTES)
    }));

    sender.join().expect("join sender runtime");
    receiver.join().expect("join receiver runtime");
}
