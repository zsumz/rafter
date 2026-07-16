use std::collections::{BTreeMap, BTreeSet};

use rafter::NodeId;

use crate::{Cluster, SimSeed};

use super::MIN_SOAK_LIVENESS_ROUNDS;
use crate::model_check::{
    helpers::{proposal_payload, summarize},
    invariants::run_replay_check,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_to_state,
    state::{ClientWriteStatus, ExplorationState},
    Failure, MessageKind, ProposalId, ReplayCheck,
};

mod schedule;

use schedule::{ready_position, rotate_tick_order, schedule_index};

pub(in crate::model_check::liveness) const FAIR_SCHEDULER_POLICY_ID: &str =
    "seeded-rotating-all-node-ticks-ready-wave-permutations-v1";
pub(in crate::model_check::liveness) const FAIR_TICK_BOUND_ROUNDS: usize = 1;
pub(in crate::model_check::liveness) const FAIR_DELIVERY_BOUND_ROUNDS: usize = 1;
pub(in crate::model_check::liveness) const STABLE_LEADER_WINDOW_ROUNDS: usize = 2;
pub(in crate::model_check::liveness) const FAIR_MAX_DELIVERY_WAVES_PER_TICK: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) struct LivenessRoundBudget {
    pub(in crate::model_check::liveness) minimum_rounds: usize,
    pub(in crate::model_check::liveness) node_count: usize,
    pub(in crate::model_check::liveness) queued_messages: usize,
    pub(in crate::model_check::liveness) max_proposals: usize,
    pub(in crate::model_check::liveness) max_membership_changes: usize,
    pub(in crate::model_check::liveness) max_partitions: usize,
    pub(in crate::model_check::liveness) snapshot_catchup_probe: bool,
    pub(in crate::model_check::liveness) base_rounds: usize,
    pub(in crate::model_check::liveness) phase_count: usize,
    pub(in crate::model_check::liveness) fixed_rounds: usize,
}

impl LivenessRoundBudget {
    pub(in crate::model_check::liveness) fn capture(
        state: &ExplorationState,
        config: SoakConfig,
        phase_count: usize,
    ) -> Self {
        let node_count = state.cluster().nodes.len();
        let queued_messages = state.cluster().network.len();
        let base_rounds = calculate_liveness_round_budget(
            node_count,
            queued_messages,
            config.max_proposals,
            config.max_membership_changes,
            config.max_partitions,
            config.snapshot_catchup_probe,
        );
        Self {
            minimum_rounds: MIN_SOAK_LIVENESS_ROUNDS,
            node_count,
            queued_messages,
            max_proposals: config.max_proposals,
            max_membership_changes: config.max_membership_changes,
            max_partitions: config.max_partitions,
            snapshot_catchup_probe: config.snapshot_catchup_probe,
            base_rounds,
            phase_count,
            fixed_rounds: 0,
        }
    }

    pub(in crate::model_check::liveness) const fn with_fixed_rounds(
        mut self,
        fixed_rounds: usize,
    ) -> Self {
        self.fixed_rounds = fixed_rounds;
        self
    }

    pub(in crate::model_check::liveness) const fn round_limit(self) -> usize {
        self.base_rounds
            .saturating_mul(self.phase_count)
            .saturating_add(self.fixed_rounds)
    }

    pub(in crate::model_check::liveness) fn validate(self) -> Result<(), &'static str> {
        if self.phase_count == 0 {
            return Err("phase_count");
        }
        let expected = calculate_liveness_round_budget(
            self.node_count,
            self.queued_messages,
            self.max_proposals,
            self.max_membership_changes,
            self.max_partitions,
            self.snapshot_catchup_probe,
        );
        if self.minimum_rounds != MIN_SOAK_LIVENESS_ROUNDS || self.base_rounds != expected {
            return Err("base_rounds");
        }
        Ok(())
    }
}

fn calculate_liveness_round_budget(
    node_count: usize,
    queued_messages: usize,
    max_proposals: usize,
    max_membership_changes: usize,
    max_partitions: usize,
    snapshot_catchup_probe: bool,
) -> usize {
    MIN_SOAK_LIVENESS_ROUNDS
        .saturating_add(node_count.saturating_mul(16))
        .saturating_add(queued_messages.saturating_mul(4))
        .saturating_add(max_proposals.saturating_mul(8))
        .saturating_add(max_membership_changes.saturating_mul(16))
        .saturating_add(max_partitions.saturating_mul(16))
        .saturating_add(usize::from(snapshot_catchup_probe).saturating_mul(64))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) struct BoundedRun {
    pub(in crate::model_check::liveness) completed: bool,
    pub(in crate::model_check::liveness) rounds_used: usize,
    pub(in crate::model_check::liveness) observer_held: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) struct LeaderConvergence {
    pub(in crate::model_check::liveness) leader: NodeId,
    pub(in crate::model_check::liveness) rounds_used: usize,
    pub(in crate::model_check::liveness) stable_rounds: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) enum ProposalTerminalOutcome {
    Committed,
    Rejected,
    Unknown,
}

impl ProposalTerminalOutcome {
    pub(in crate::model_check::liveness) const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Rejected => "rejected",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FairRoundExecution {
    expected_ticks: usize,
    ticks_executed: usize,
    ready_at_boundary: usize,
    boundary_deliveries: usize,
    observer_held: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct BoundedFairnessMonitor {
    tick_bound: usize,
    delivery_bound: usize,
    unticked_rounds: BTreeMap<NodeId, usize>,
    undelivered_rounds: usize,
}

impl BoundedFairnessMonitor {
    fn new(tick_bound: usize, delivery_bound: usize) -> Self {
        assert!(tick_bound > 0, "tick fairness bound must be positive");
        assert!(
            delivery_bound > 0,
            "delivery fairness bound must be positive"
        );
        Self {
            tick_bound,
            delivery_bound,
            unticked_rounds: BTreeMap::new(),
            undelivered_rounds: 0,
        }
    }

    fn observe_round(
        &mut self,
        expected_ticks: &[NodeId],
        executed_ticks: &[NodeId],
        ready_at_boundary: usize,
        boundary_deliveries: usize,
    ) -> Result<(), &'static str> {
        let executed = executed_ticks.iter().copied().collect::<BTreeSet<_>>();
        for node_id in expected_ticks {
            let age = self.unticked_rounds.entry(*node_id).or_default();
            if executed.contains(node_id) {
                *age = 0;
            } else {
                *age = age.saturating_add(1);
                if *age >= self.tick_bound {
                    return Err("tick starvation exceeded the bounded-fair round limit");
                }
            }
        }

        if boundary_deliveries >= ready_at_boundary {
            self.undelivered_rounds = 0;
        } else {
            self.undelivered_rounds = self.undelivered_rounds.saturating_add(1);
            if self.undelivered_rounds >= self.delivery_bound {
                return Err("delivery starvation exceeded the bounded-fair round limit");
            }
        }
        Ok(())
    }
}

fn observe_bounded_fairness_round(
    monitor: &mut BoundedFairnessMonitor,
    expected_ticks: &[NodeId],
    executed_ticks: &[NodeId],
    ready_at_boundary: usize,
    boundary_deliveries: usize,
) -> Result<(), &'static str> {
    monitor.observe_round(
        expected_ticks,
        executed_ticks,
        ready_at_boundary,
        boundary_deliveries,
    )
}

pub(in crate::model_check::liveness) struct FairRoundDriver {
    fairness: BoundedFairnessMonitor,
    schedule_seed: SimSeed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) struct LivenessScheduleWindow {
    pub(in crate::model_check::liveness) round_offset: usize,
    pub(in crate::model_check::liveness) budget: usize,
}

impl LivenessScheduleWindow {
    pub(in crate::model_check::liveness) const fn new(round_offset: usize, budget: usize) -> Self {
        Self {
            round_offset,
            budget,
        }
    }
}

impl FairRoundDriver {
    pub(in crate::model_check::liveness) fn new(schedule_seed: SimSeed) -> Self {
        Self {
            fairness: BoundedFairnessMonitor::new(
                FAIR_TICK_BOUND_ROUNDS,
                FAIR_DELIVERY_BOUND_ROUNDS,
            ),
            schedule_seed,
        }
    }

    fn drive(
        &mut self,
        state: &mut ExplorationState,
        trace: &mut Vec<SoakAction>,
        observed_actions: &mut BTreeSet<SoakActionKind>,
        round: usize,
        observe: &mut dyn FnMut(&ExplorationState) -> bool,
    ) -> Result<FairRoundExecution, &'static str> {
        drive_soak_liveness_round_observed(
            state,
            trace,
            observed_actions,
            round,
            observe,
            &mut self.fairness,
            self.schedule_seed,
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::model_check::liveness) struct StableLeaderGuard {
    expected: NodeId,
    round_limit: usize,
    observations: usize,
}

impl StableLeaderGuard {
    pub(in crate::model_check::liveness) const fn new(
        expected: NodeId,
        round_limit: usize,
    ) -> Self {
        Self {
            expected,
            round_limit,
            observations: 0,
        }
    }

    pub(in crate::model_check::liveness) fn observe(
        &mut self,
        observed: Option<NodeId>,
    ) -> Result<(), String> {
        self.observations = self.observations.saturating_add(1);
        if observed == Some(self.expected) {
            return Ok(());
        }
        Err(format!(
            "stable leader {} was replaced by {:?} during observation {} of {}",
            self.expected, observed, self.observations, self.round_limit
        ))
    }
}

#[allow(dead_code)]
pub(in crate::model_check::liveness) fn drive_liveness_rounds_until(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
    complete: impl FnMut(&ExplorationState) -> bool,
) -> Result<bool, SoakFailure> {
    Ok(drive_liveness_rounds_until_observed(
        state,
        config,
        trace,
        observed_actions,
        budget,
        complete,
        |_| true,
    )?
    .completed)
}

pub(in crate::model_check::liveness) fn drive_liveness_rounds_until_observed(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
    mut complete: impl FnMut(&ExplorationState) -> bool,
    mut observe: impl FnMut(&ExplorationState) -> bool,
) -> Result<BoundedRun, SoakFailure> {
    drive_liveness_rounds_until_observed_from_round(
        state,
        config,
        trace,
        observed_actions,
        LivenessScheduleWindow::new(0, budget),
        &mut complete,
        &mut observe,
    )
}

pub(in crate::model_check::liveness) fn drive_liveness_rounds_until_observed_from_round(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    schedule: LivenessScheduleWindow,
    mut complete: impl FnMut(&ExplorationState) -> bool,
    mut observe: impl FnMut(&ExplorationState) -> bool,
) -> Result<BoundedRun, SoakFailure> {
    if complete(state) {
        return Ok(BoundedRun {
            completed: true,
            rounds_used: 0,
            observer_held: true,
        });
    }
    let mut fair_rounds = FairRoundDriver::new(config.seed);
    for elapsed_round in 0..schedule.budget {
        let schedule_round = schedule.round_offset.saturating_add(elapsed_round);
        let mut completion_latched = false;
        let mut premise_held = true;
        let mut observe_until_terminal = |state: &ExplorationState| {
            if completion_latched {
                return true;
            }
            if complete(state) {
                completion_latched = true;
                return true;
            }
            let held = observe(state);
            premise_held &= held;
            held
        };
        let execution = fair_rounds
            .drive(
                state,
                trace,
                observed_actions,
                schedule_round,
                &mut observe_until_terminal,
            )
            .map_err(|message| soak_liveness_harness_error(state, config, trace, message))?;
        check_soak_safety(state, config, trace)?;
        if !premise_held || !execution.observer_held {
            return Ok(BoundedRun {
                completed: false,
                rounds_used: elapsed_round + 1,
                observer_held: false,
            });
        }
        if completion_latched || complete(state) {
            return Ok(BoundedRun {
                completed: true,
                rounds_used: elapsed_round + 1,
                observer_held: true,
            });
        }
    }
    Ok(BoundedRun {
        completed: false,
        rounds_used: schedule.budget,
        observer_held: true,
    })
}

#[cfg(test)]
pub(in crate::model_check::liveness) fn drive_until_quiescent_leader(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
) -> Result<Option<NodeId>, SoakFailure> {
    Ok(
        drive_until_stable_leader(state, config, trace, observed_actions, budget)?
            .map(|evidence| evidence.leader),
    )
}

pub(in crate::model_check::liveness) fn drive_until_stable_leader(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
) -> Result<Option<LeaderConvergence>, SoakFailure> {
    drive_until_stable_leader_from_round(state, config, trace, observed_actions, 0, budget)
}

pub(in crate::model_check::liveness) fn drive_until_stable_leader_from_round(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    schedule_round_offset: usize,
    budget: usize,
) -> Result<Option<LeaderConvergence>, SoakFailure> {
    let mut stable_leader = None;
    let mut stable_rounds = 0usize;
    let mut fair_rounds = FairRoundDriver::new(config.seed);
    for elapsed_round in 0..budget {
        let schedule_round = schedule_round_offset.saturating_add(elapsed_round);
        let round_candidate = single_leader(state);
        let mut candidate_held = round_candidate.is_some();
        let mut observe_candidate = |state: &ExplorationState| {
            let held = round_candidate.is_some() && single_leader(state) == round_candidate;
            candidate_held &= held;
            held
        };
        let execution = fair_rounds
            .drive(
                state,
                trace,
                observed_actions,
                schedule_round,
                &mut observe_candidate,
            )
            .map_err(|message| soak_liveness_harness_error(state, config, trace, message))?;
        check_soak_safety(state, config, trace)?;

        if candidate_held && execution.observer_held {
            if stable_leader == round_candidate {
                stable_rounds = stable_rounds.saturating_add(1);
            } else {
                stable_leader = round_candidate;
                stable_rounds = 1;
            }
        } else {
            stable_leader = single_leader(state);
            stable_rounds = 0;
        }

        if stable_rounds >= STABLE_LEADER_WINDOW_ROUNDS {
            return Ok(stable_leader.map(|leader| LeaderConvergence {
                leader,
                rounds_used: elapsed_round + 1,
                stable_rounds,
            }));
        }
    }
    Ok(None)
}

pub(in crate::model_check::liveness) fn drive_soak_liveness_round(
    fair_rounds: &mut FairRoundDriver,
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    round: usize,
) -> Result<(), SoakFailure> {
    fair_rounds
        .drive(state, trace, observed_actions, round, &mut always_observe)
        .map(|_| ())
        .map_err(|message| soak_liveness_harness_error(state, config, trace, message))
}

pub(in crate::model_check::liveness) fn drive_soak_liveness_round_until_terminal(
    fair_rounds: &mut FairRoundDriver,
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    round: usize,
    mut terminal: impl FnMut(&ExplorationState) -> bool,
) -> Result<bool, SoakFailure> {
    let mut terminal_latched = terminal(state);
    let mut latch_terminal = |state: &ExplorationState| {
        terminal_latched |= terminal(state);
        true
    };
    fair_rounds
        .drive(state, trace, observed_actions, round, &mut latch_terminal)
        .map_err(|message| soak_liveness_harness_error(state, config, trace, message))?;
    Ok(terminal_latched)
}

fn drive_soak_liveness_round_observed(
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
                    super::super::scheduling::envelope_identity(state.cluster(), position)
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

fn ensure_delivery_frontier_drained(ready_after_final_wave: usize) -> Result<(), &'static str> {
    if ready_after_final_wave == 0 {
        Ok(())
    } else {
        Err("delivery-wave cap exhausted with ready messages still queued")
    }
}

fn always_observe(_: &ExplorationState) -> bool {
    true
}

fn ready_message_count(state: &ExplorationState) -> usize {
    state
        .cluster()
        .network
        .iter()
        .filter(|queued| queued.ready_at <= state.cluster().clock.now())
        .count()
}

pub(in crate::model_check::liveness) fn check_soak_safety(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
) -> Result<(), SoakFailure> {
    if let Err(failure) = run_replay_check(state, ReplayCheck::CommitSafety, &[]) {
        return Err(SoakFailure {
            seed: config.seed,
            step: trace.len(),
            trace: trace.to_vec(),
            failure: Box::new(failure),
        });
    }
    Ok(())
}

pub(in crate::model_check::liveness) fn issue_liveness_proposal(
    state: &mut ExplorationState,
    leader: NodeId,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Option<ProposalId> {
    let proposal_id = ProposalId(state.proposals_issued() + 1);
    let payload = proposal_payload(proposal_id);
    apply_to_state(
        state,
        Operation::Propose {
            to: leader,
            proposal_id,
            stale_leader: false,
        },
    );
    trace.push(SoakAction::Propose {
        to: leader,
        proposal_id,
    });
    observed_actions.insert(SoakActionKind::Propose);

    if !state
        .client_history()
        .writes
        .get(&proposal_id)
        .is_some_and(|write| {
            matches!(
                write.status,
                ClientWriteStatus::Accepted { .. } | ClientWriteStatus::Completed { .. }
            )
        })
        || !liveness_payload_visible(state, &payload)
    {
        return None;
    }

    Some(proposal_id)
}

pub(in crate::model_check::liveness) fn soak_liveness_round_budget(
    state: &ExplorationState,
    config: SoakConfig,
) -> usize {
    LivenessRoundBudget::capture(state, config, 1).base_rounds
}

pub(in crate::model_check::liveness) fn quiescent_leader(
    state: &ExplorationState,
) -> Option<NodeId> {
    state
        .cluster()
        .network
        .is_empty()
        .then(|| single_leader(state))?
}

pub(in crate::model_check::liveness) fn single_leader(state: &ExplorationState) -> Option<NodeId> {
    let leaders = state.cluster().leaders();
    (leaders.len() == 1).then(|| leaders[0])
}

pub(in crate::model_check::liveness) fn has_partition(cluster: &Cluster) -> bool {
    cluster
        .nodes
        .keys()
        .any(|a| cluster.nodes.keys().any(|b| cluster.partitioned(*a, *b)))
}

pub(in crate::model_check::liveness) fn liveness_proposal_completed(
    state: &ExplorationState,
    proposal_id: ProposalId,
) -> bool {
    state
        .client_history()
        .writes
        .get(&proposal_id)
        .is_some_and(|write| matches!(write.status, ClientWriteStatus::Completed { .. }))
}

pub(in crate::model_check::liveness) fn liveness_proposal_accepted(
    state: &ExplorationState,
    proposal_id: ProposalId,
) -> bool {
    state
        .client_history()
        .writes
        .get(&proposal_id)
        .is_some_and(|write| {
            matches!(
                write.status,
                ClientWriteStatus::Accepted { .. } | ClientWriteStatus::Completed { .. }
            )
        })
}

pub(in crate::model_check::liveness) fn liveness_proposal_terminal_outcome(
    state: &ExplorationState,
    proposal_id: ProposalId,
) -> Option<ProposalTerminalOutcome> {
    let write = state.client_history().writes.get(&proposal_id)?;
    match write.status {
        ClientWriteStatus::Completed { .. } => Some(ProposalTerminalOutcome::Committed),
        ClientWriteStatus::Rejected => Some(ProposalTerminalOutcome::Rejected),
        ClientWriteStatus::Unknown { .. } => Some(ProposalTerminalOutcome::Unknown),
        ClientWriteStatus::Pending | ClientWriteStatus::Accepted { .. } => None,
    }
}

pub(in crate::model_check::liveness) fn soak_liveness_coverage_failure(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    invariant: &'static str,
    message: String,
) -> SoakFailure {
    classified_liveness_failure(
        state,
        config,
        trace,
        invariant,
        crate::model_check::FailureKind::CoverageNotReached,
        message,
    )
}

pub(in crate::model_check::liveness) fn soak_liveness_invariant_failure(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    invariant: &'static str,
    message: String,
) -> SoakFailure {
    classified_liveness_failure(
        state,
        config,
        trace,
        invariant,
        crate::model_check::FailureKind::InvariantViolation,
        message,
    )
}

fn classified_liveness_failure(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    invariant: &'static str,
    kind: crate::model_check::FailureKind,
    message: String,
) -> SoakFailure {
    SoakFailure {
        seed: config.seed,
        step: trace.len(),
        trace: trace.to_vec(),
        failure: Box::new(Failure {
            kind,
            invariant,
            message,
            trace: Vec::new(),
            state: summarize(state.cluster()),
        }),
    }
}

pub(in crate::model_check::liveness) fn soak_transition_failure(
    config: SoakConfig,
    trace: &[SoakAction],
    failure: Failure,
) -> SoakFailure {
    SoakFailure {
        seed: config.seed,
        step: trace.len(),
        trace: trace.to_vec(),
        failure: Box::new(failure),
    }
}

pub(in crate::model_check::liveness) fn soak_liveness_harness_error(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    message: &'static str,
) -> SoakFailure {
    SoakFailure {
        seed: config.seed,
        step: trace.len(),
        trace: trace.to_vec(),
        failure: Box::new(Failure {
            kind: crate::model_check::FailureKind::HarnessError,
            invariant: "bounded-fair liveness scheduler",
            message: message.to_owned(),
            trace: Vec::new(),
            state: summarize(state.cluster()),
        }),
    }
}

fn liveness_payload_visible(state: &ExplorationState, payload: &[u8]) -> bool {
    state
        .cluster()
        .applied()
        .iter()
        .any(|applied| applied.payload.as_slice() == payload)
        || state.cluster().nodes.keys().any(|node_id| {
            state
                .cluster()
                .bootstrap_state(*node_id)
                .log
                .iter()
                .any(|entry| entry.kind.application_payload() == Some(payload))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rafter_invariant_test::{oracle_assert, oracle_expect_err};

    #[rafter_invariant_test::detector_test]
    fn bounded_fairness_detector_rejects_positive_bound_tick_starvation() {
        let mut monitor = BoundedFairnessMonitor::new(2, 3);
        observe_bounded_fairness_round(&mut monitor, &[NodeId(1), NodeId(2)], &[NodeId(1)], 0, 0)
            .expect("one missed round remains inside the positive bound");
        let error = oracle_expect_err!(
            observe_bounded_fairness_round(
                &mut monitor,
                &[NodeId(1), NodeId(2)],
                &[NodeId(1)],
                0,
                0,
            ),
            "the second missed tick must exhaust the positive bound"
        );

        oracle_assert!(error.contains("tick starvation"));
    }
}

#[cfg(test)]
#[path = "driver/tests.rs"]
mod unit_tests;
