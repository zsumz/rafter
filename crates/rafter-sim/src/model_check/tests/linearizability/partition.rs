//! A new leader can expose a committed write while its old caller still waits.
use super::{
    check_client_history_linearizable, deliver_all_in_state, elect_node_one_in_state,
    three_node_configs, ClientReadOutcome, ClientWriteStatus, Cluster, ExplorationState, LogIndex,
    NodeId, Operation, ProposalId,
};
use crate::model_check::{
    helpers::config,
    invariants::run_replay_check,
    run_raft_random_soak,
    scheduling::SoakOperation,
    state::{apply_to_state, try_apply_soak_action},
    ReplayCheck, SoakConfig,
};
use rafter::{Message, Role};

#[test]
fn partitioned_proposer_can_remain_pending_while_new_leader_serves_its_value() {
    let mut state = ExplorationState::new(Cluster::new(three_node_configs()));
    elect_node_one_in_state(&mut state);
    deliver_all_in_state(&mut state);
    apply_to_state(
        &mut state,
        Operation::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
            stale_leader: false,
        },
    );
    // Replicate to voter 2, but withhold its response from the proposing leader.
    let position = state
        .cluster()
        .network
        .iter()
        .position(|queued| {
            queued.envelope.from == NodeId(1)
                && queued.envelope.to == NodeId(2)
                && matches!(&queued.envelope.message, Message::AppendEntries(append) if
            append.prev_log_index == LogIndex(1) && !append.entries.is_empty())
        })
        .expect("actual command append to voter 2");
    apply_to_state(&mut state, Operation::DeliverReadyAt(position));
    assert_eq!(state.cluster().commit_index(NodeId(1)), LogIndex(1));
    state.drop_all_messages();
    for b in [NodeId(2), NodeId(3)] {
        try_apply_soak_action(&mut state, SoakOperation::Partition { a: NodeId(1), b }).unwrap();
    }
    for _ in 0..32 {
        apply_to_state(&mut state, Operation::Tick(NodeId(2)));
        deliver_all_in_state(&mut state);
        if state.cluster().role(NodeId(2)) == Role::Leader {
            break;
        }
    }
    assert_eq!(state.cluster().role(NodeId(2)), Role::Leader);
    assert!(state.cluster().commit_index(NodeId(2)) >= LogIndex(3));
    apply_to_state(
        &mut state,
        Operation::ReadIndex {
            to: NodeId(2),
            request_id: 1,
        },
    );
    deliver_all_in_state(&mut state);
    let read = state.client_history().reads.values().next().unwrap();
    assert!(
        matches!(&read.outcome, ClientReadOutcome::Completed { result: Some(value), .. }
        if value.as_ref() == b"model-proposal-1")
    );
    assert!(matches!(
        state.client_history().writes[&ProposalId(1)].status,
        ClientWriteStatus::Accepted { .. }
    ));
    check_client_history_linearizable(state.client_history())
        .expect("read may observe a write still awaiting reply");
    run_replay_check(&state, ReplayCheck::CommitSafety, &[]).unwrap();
}

#[test]
fn nightly_seed_preserves_incomplete_write_in_read_history() {
    let nodes = vec![
        config(1, &[2, 3], 2),
        config(2, &[1, 3], 2),
        config(3, &[1, 2], 2),
    ];
    let config = SoakConfig::new(crate::SimSeed(0x5f20_b7ca_1233_fddd), 1024)
        .with_max_proposals(64)
        .with_max_restarts(32)
        .with_max_read_indexes(4)
        .with_max_membership_changes(16)
        .with_max_transfers(2)
        .with_max_partitions(2)
        .with_max_lossy_restarts(2)
        .with_snapshot_catchup_probe()
        .with_tick_skew(NodeId(1), 3);
    run_raft_random_soak(nodes, config)
        .expect("nightly RD-06 seed must complete safety and liveness");
}
