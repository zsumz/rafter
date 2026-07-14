use super::*;
use crate::{
    model_check::{
        helpers::{config, deliver_all_in_state, elect_node_one_in_state},
        state::ExplorationState,
    },
    Cluster, SimSeed,
};
use rafter_invariant_test::oracle_assert;

fn three_node_fast_configs() -> Vec<rafter::NodeConfig> {
    vec![
        config(1, &[2, 3], 2),
        config(2, &[1, 3], 2),
        config(3, &[1, 2], 2),
    ]
}

#[test]
fn quiescent_leader_monitor_survives_heartbeat_between_observations() {
    let config = SoakConfig::new(SimSeed(0x1ead), 0);
    let mut state = ExplorationState::new(Cluster::new_with_seed(
        three_node_fast_configs(),
        config.seed,
    ));
    elect_node_one_in_state(&mut state);
    deliver_all_in_state(&mut state);
    assert_eq!(quiescent_leader(&state), Some(NodeId(1)));

    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();
    let leader =
        drive_until_quiescent_leader(&mut state, config, &mut trace, &mut observed_actions, 12)
            .expect("leader convergence monitor should preserve same-leader observations");

    assert_eq!(leader, Some(NodeId(1)));
    assert!(
        trace
            .iter()
            .any(|action| matches!(action, SoakAction::Tick(NodeId(1)))),
        "regression should exercise a leader tick between quiescent observations"
    );
}

#[test]
fn bounded_fairness_detector_rejects_positive_bound_delivery_starvation() {
    let mut monitor = BoundedFairnessMonitor::new(3, 2);
    monitor
        .observe_round(&[NodeId(1)], &[NodeId(1)], 1, 0)
        .expect("one missed round remains inside the positive bound");
    let error = monitor
        .observe_round(&[NodeId(1)], &[NodeId(1)], 1, 0)
        .expect_err("the second missed delivery must exhaust the positive bound");

    oracle_assert!(error.contains("delivery starvation"));
}

#[test]
fn fair_round_schedule_is_replayable_and_seed_varied() {
    fn tick_order(seed: SimSeed) -> Vec<NodeId> {
        let config = SoakConfig::new(seed, 0);
        let mut state = ExplorationState::new(Cluster::new_with_seed(
            three_node_fast_configs(),
            config.seed,
        ));
        let mut trace = Vec::new();
        let mut observed_actions = BTreeSet::new();
        let mut driver = FairRoundDriver::new(seed);
        drive_soak_liveness_round(
            &mut driver,
            &mut state,
            config,
            &mut trace,
            &mut observed_actions,
            0,
        )
        .expect("one fair round should complete");
        trace
            .into_iter()
            .filter_map(|action| match action {
                SoakAction::Tick(node_id) => Some(node_id),
                _ => None,
            })
            .collect()
    }

    let first = tick_order(SimSeed(0x9103));
    let replay = tick_order(SimSeed(0x9103));
    let varied = tick_order(SimSeed(0x9104));

    assert_eq!(first, replay, "a seed must replay the same fair schedule");
    assert_ne!(
        first, varied,
        "different reviewed seeds must vary the schedule"
    );
    assert_eq!(first.iter().copied().collect::<BTreeSet<_>>().len(), 3);
    assert_eq!(varied.iter().copied().collect::<BTreeSet<_>>().len(), 3);
}

#[test]
fn fair_schedule_offset_continues_instead_of_restarting_round_zero() {
    fn tick_order_from_offset(seed: SimSeed, schedule_round_offset: usize) -> Vec<NodeId> {
        let config = SoakConfig::new(seed, 0);
        let mut state = ExplorationState::new(Cluster::new_with_seed(
            three_node_fast_configs(),
            config.seed,
        ));
        let mut trace = Vec::new();
        let mut observed_actions = BTreeSet::new();
        drive_liveness_rounds_until_observed_from_round(
            &mut state,
            config,
            &mut trace,
            &mut observed_actions,
            LivenessScheduleWindow::new(schedule_round_offset, 1),
            |_| false,
            |_| true,
        )
        .expect("one offset fair round should complete");
        trace
            .into_iter()
            .filter_map(|action| match action {
                SoakAction::Tick(node_id) => Some(node_id),
                _ => None,
            })
            .collect()
    }

    let round_zero = tick_order_from_offset(SimSeed(0x9103), 0);
    let next_round = tick_order_from_offset(SimSeed(0x9103), 1);
    let replayed_next_round = tick_order_from_offset(SimSeed(0x9103), 1);

    assert_ne!(round_zero, next_round);
    assert_eq!(next_round, replayed_next_round);
}

#[test]
fn stable_leader_guard_rejects_replacement_inside_positive_window() {
    let mut guard = StableLeaderGuard::new(NodeId(1), 4);
    guard
        .observe(Some(NodeId(1)))
        .expect("the original leader remains stable initially");
    guard
        .observe(Some(NodeId(1)))
        .expect("the original leader remains stable in round two");
    let error = guard
        .observe(Some(NodeId(2)))
        .expect_err("a replacement inside the positive window must fail");

    assert!(error.contains("replaced"));
    assert!(error.contains("observation 3 of 4"));
}

#[test]
fn delivery_frontier_detector_rejects_cap_exhaustion() {
    let error = ensure_delivery_frontier_drained(1)
        .expect_err("ready messages after the final wave must fail closed");

    assert!(error.contains("cap exhausted"));
    ensure_delivery_frontier_drained(0).expect("a drained frontier satisfies the bound");
}
