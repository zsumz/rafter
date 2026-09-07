//! One bounded-fair round: every node ticks, then the ready frontier drains.
//!
//! Tick order and delivery order come only from the schedule seed and round
//! ordinal, so a seed replays the same interleaving exactly; a round that
//! cannot drain its frontier inside the wave cap fails closed.

use std::collections::BTreeSet;

use crate::model_check::{
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind},
    state::apply_to_state,
    state::ExplorationState,
    MessageKind,
};
use crate::SimSeed;

use super::schedule::{ready_position, rotate_tick_order, schedule_index};
use super::{
    ensure_delivery_frontier_drained, observe_bounded_fairness_round, BoundedFairnessMonitor,
    FairRoundExecution, FAIR_MAX_DELIVERY_WAVES_PER_TICK,
};

pub(super) fn drive_soak_liveness_round_observed(
    state: &mut ExplorationState,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    round: usize,
    observe: &mut dyn FnMut(&ExplorationState) -> bool,
    fairness: &mut BoundedFairnessMonitor,
    schedule_seed: SimSeed,
) -> Result<FairRoundExecution, &'static str> {
    let mut node_ids = state.cluster().nodes.keys().copied().collect::<Vec<_>>();
    rotate_tick_order(&mut node_ids, schedule_seed, round);
    let mut executed_ticks = Vec::with_capacity(node_ids.len());
    let mut observer_held = true;
    let mut ready_at_boundary = 0usize;
    let mut boundary_deliveries = 0usize;
    for (tick_ordinal, node_id) in node_ids.iter().copied().enumerate() {
        apply_to_state(state, Operation::Tick(node_id));
        trace.push(SoakAction::Tick(node_id));
        observed_actions.insert(SoakActionKind::Tick);
        executed_ticks.push(node_id);
        observer_held &= observe(state);
        for wave in 0..FAIR_MAX_DELIVERY_WAVES_PER_TICK {
            let wave_size = ready_message_count(state);
            if wave_size == 0 {
                break;
            }
            ready_at_boundary = ready_at_boundary.saturating_add(wave_size);
            for delivery_ordinal in 0..wave_size {
                let remaining_at_boundary = wave_size - delivery_ordinal;
                let ready_ordinal = schedule_index(
                    schedule_seed,
                    round,
                    tick_ordinal,
                    wave,
                    delivery_ordinal,
                    remaining_at_boundary,
                );
                let Some(position) = ready_position(state, ready_ordinal) else {
                    break;
                };
                let Some(envelope) = state.cluster().pending_envelope_at(position).cloned() else {
                    break;
                };
                let identity =
                    super::super::super::scheduling::envelope_identity(state.cluster(), position)
                        .map_err(|_| "scheduler envelope identity")?;
                apply_to_state(state, Operation::DeliverReadyAt(position));
                trace.push(SoakAction::Deliver {
                    from: envelope.from,
                    to: envelope.to,
                    message: MessageKind::from(&envelope.message),
                    identity,
                });
                observed_actions.insert(SoakActionKind::Deliver);
                boundary_deliveries = boundary_deliveries.saturating_add(1);
                observer_held &= observe(state);
            }
        }
        ensure_delivery_frontier_drained(ready_message_count(state))?;
    }

    observe_bounded_fairness_round(
        fairness,
        &node_ids,
        &executed_ticks,
        ready_at_boundary,
        boundary_deliveries,
    )?;

    Ok(FairRoundExecution {
        expected_ticks: node_ids.len(),
        ticks_executed: executed_ticks.len(),
        ready_at_boundary,
        boundary_deliveries,
        observer_held,
    })
}

fn ready_message_count(state: &ExplorationState) -> usize {
    state
        .cluster()
        .network
        .iter()
        .filter(|queued| queued.ready_at <= state.cluster().clock.now())
        .count()
}
