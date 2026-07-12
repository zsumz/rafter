//! Leader heartbeat cadence after successful election.

use super::*;

#[test]
fn leader_emits_heartbeats_on_tick() {
    let mut node = node(1, &[2, 3]);
    let _ = elect_leader(&mut node);

    let outputs = node.step(Input::Tick);

    assert_eq!(outputs.len(), 2);
    assert_append_entries(&outputs[0], NodeId(2), 0);
    assert_append_entries(&outputs[1], NodeId(3), 0);
}
#[test]
fn leader_coalesces_heartbeats_until_interval() {
    let mut node = Node::new(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
            .expect("test Raft node config is valid")
            .with_heartbeat_interval_ticks(2),
    );
    let _ = elect_leader(&mut node);

    assert!(node.step(Input::Tick).is_empty());
    let outputs = node.step(Input::Tick);

    assert_eq!(node.role(), Role::Leader);
    assert_eq!(outputs.len(), 2);
    assert_append_entries(&outputs[0], NodeId(2), 0);
    assert_append_entries(&outputs[1], NodeId(3), 0);
}
