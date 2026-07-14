use rafter::NodeId;

use crate::{model_check::state::ExplorationState, SimSeed};

pub(super) fn ready_position(state: &ExplorationState, ready_ordinal: usize) -> Option<usize> {
    state
        .cluster()
        .network
        .iter()
        .enumerate()
        .filter(|(_, queued)| queued.ready_at <= state.cluster().clock.now())
        .nth(ready_ordinal)
        .map(|(position, _)| position)
}

pub(super) fn rotate_tick_order(node_ids: &mut [NodeId], schedule_seed: SimSeed, round: usize) {
    if node_ids.len() < 2 {
        return;
    }
    let rotation = schedule_index(schedule_seed, round, 0, 0, 0, node_ids.len());
    node_ids.rotate_left(rotation);
    if schedule_word(schedule_seed, round, 0, 0, 1) & 1 == 1 {
        node_ids.reverse();
    }
}

pub(super) fn schedule_index(
    schedule_seed: SimSeed,
    round: usize,
    tick_ordinal: usize,
    wave: usize,
    delivery_ordinal: usize,
    upper_bound: usize,
) -> usize {
    if upper_bound == 0 {
        return 0;
    }
    let scheduler_bound = u64::try_from(upper_bound).unwrap_or(u64::MAX);
    let bounded =
        schedule_word(schedule_seed, round, tick_ordinal, wave, delivery_ordinal) % scheduler_bound;
    usize::try_from(bounded).unwrap_or_else(|_| upper_bound.saturating_sub(1))
}

fn schedule_word(
    schedule_seed: SimSeed,
    round: usize,
    tick_ordinal: usize,
    wave: usize,
    delivery_ordinal: usize,
) -> u64 {
    let mut value = schedule_seed.0
        ^ (round as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ (tick_ordinal as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9)
        ^ (wave as u64).wrapping_mul(0x94d0_49bb_1331_11eb)
        ^ delivery_ordinal as u64;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}
