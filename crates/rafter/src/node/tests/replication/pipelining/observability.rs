//! Public projection of probe, replicate, and snapshot progress modes.

use super::support::*;
use super::*;

#[test]
fn leader_replication_progress_reports_the_state_of_every_mode() {
    let snapshot = test_snapshot(3, 4, 5, b"observability snapshot");
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3), NodeId(4)], 3)
            .expect("test Raft node config is valid"),
        BootstrapState {
            current_term: Term(5),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot),
            log: vec![bootstrap_entry(4, 5, &payload(4))],
        },
    )
    .expect("leader hydrates from snapshot");
    leader.become_leader();

    // Follower 2 keeps the fresh-leadership probe; follower 3 has confirmed
    // its position; follower 4 is mid-snapshot.
    seed_replicating(&mut leader, NodeId(3), LogIndex(4));
    let behind = leader
        .try_follower_progress_mut(NodeId(4))
        .expect("active follower");
    behind.next_index = LogIndex(3);
    behind.mode = ProgressMode::Snapshot { next_offset: 7 };

    assert_eq!(
        leader.leader_replication_progress(),
        vec![
            ReplicationProgress {
                follower_id: NodeId(2),
                match_index: LogIndex::ZERO,
                next_index: LogIndex(5),
                state: ReplicationState::Probing,
            },
            ReplicationProgress {
                follower_id: NodeId(3),
                match_index: LogIndex(4),
                next_index: LogIndex(5),
                state: ReplicationState::Replicating,
            },
            ReplicationProgress {
                follower_id: NodeId(4),
                match_index: LogIndex::ZERO,
                next_index: LogIndex(3),
                state: ReplicationState::Snapshotting { next_offset: 7 },
            },
        ]
    );
}
