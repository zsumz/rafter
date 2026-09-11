//! Application-loss restart remains visible in the replay timeline.

use super::*;

#[test]
fn replay_preserves_application_loss_epoch_transition() {
    let configs = vec![config(1, &[], 1)];
    let mut state = ExplorationState::new(Cluster::new(configs.clone()));
    let mut trace = Vec::new();
    for _ in 0..4 {
        if state.cluster().role(NodeId(1)) == Role::Leader {
            break;
        }
        apply_and_record(
            &mut state,
            &mut trace,
            Action::Tick(NodeId(1)),
            Operation::Tick(NodeId(1)),
        );
    }
    assert_eq!(state.cluster().role(NodeId(1)), Role::Leader);
    apply_and_record(
        &mut state,
        &mut trace,
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
        },
        Operation::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
            stale_leader: false,
        },
    );
    trace.push(Action::ApplicationLossRestart(NodeId(1)));
    crate::model_check::state::restart_node_losing_application_state(&mut state, NodeId(1), &trace)
        .expect("application-loss transition replays the committed one-node log");
    assert_eq!(state.cluster().application_epoch(NodeId(1)), 1);
    assert!(state
        .observation_set()
        .contains(Observation::CrossEpochExecutionWitnessPairs));
    let expected = summarize(state.cluster());

    let report = replay_raft_trace(
        configs,
        &trace,
        ReplayCheck::CommitSafety,
        ReplayExpectation::FinalState(&expected),
    )
    .expect("application-loss action must replay through the transition engine");

    assert_eq!(report.state(), &expected);
    assert!(report.failure().is_none());
}
