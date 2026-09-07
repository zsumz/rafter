//! Driving one remove-voter transition to an explicit terminal outcome.
//!
//! Every round must latch the first terminal outcome it sees and re-check
//! safety, so exhausting the budget means the transition really did not
//! terminate rather than that the observation was missed. What counts as
//! terminal is the monitor's judgment, not this module's.

use std::collections::BTreeSet;

use rafter::NodeId;

use crate::model_check::liveness::driver::{
    check_soak_safety, drive_soak_liveness_round_until_terminal, soak_liveness_harness_error,
    soak_liveness_invariant_failure, FairRoundDriver,
};
use crate::model_check::liveness::features::{LivenessFeatureReport, TerminalEvidenceRecorder};
use crate::model_check::{
    catalog,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_to_state,
    state::ExplorationState,
};

use super::monitor::{membership_transition_ready_to_leave, MembershipMonitor};

pub(super) fn run_membership_operation(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    monitor: &MembershipMonitor,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let mut terminal_recorder = TerminalEvidenceRecorder::new(
        format!(
            "remove-voter:{}:{}",
            monitor.leader.0, monitor.removed_voter.0
        ),
        monitor.recorder_mode,
    );
    apply_to_state(
        state,
        Operation::RemoveVoter {
            to: monitor.leader,
            voter_id: monitor.removed_voter,
        },
    );
    trace.push(SoakAction::RemoveVoter {
        to: monitor.leader,
        voter_id: monitor.removed_voter,
    });
    observed_actions.insert(SoakActionKind::RemoveVoter);
    check_soak_safety(state, config, trace)?;
    if terminal_recorder.observe(monitor.outcome(state, monitor.leader)) {
        return report_recorded_membership_operation(
            state,
            config,
            trace,
            monitor,
            &terminal_recorder,
            0,
        );
    }

    let mut leave_issued = false;
    let mut fair_rounds = FairRoundDriver::new(config.seed);
    for round in 0..monitor.operation_budget {
        if terminal_recorder.observe(monitor.outcome(state, monitor.leader)) {
            return report_recorded_membership_operation(
                state,
                config,
                trace,
                monitor,
                &terminal_recorder,
                operation_rounds(round, false),
            );
        }

        if !leave_issued {
            if let Some(leave_leader) =
                issue_leave_joint_if_ready(state, config, trace, observed_actions, monitor)?
            {
                leave_issued = true;
                if terminal_recorder.observe(monitor.outcome(state, leave_leader)) {
                    return report_recorded_membership_operation(
                        state,
                        config,
                        trace,
                        monitor,
                        &terminal_recorder,
                        operation_rounds(round, false),
                    );
                }
            }
        }

        let terminal_latched = drive_soak_liveness_round_until_terminal(
            &mut fair_rounds,
            state,
            config,
            trace,
            observed_actions,
            round,
            |state| terminal_recorder.observe(monitor.outcome(state, monitor.leader)),
        )?;
        check_soak_safety(state, config, trace)?;
        if terminal_latched {
            return report_recorded_membership_operation(
                state,
                config,
                trace,
                monitor,
                &terminal_recorder,
                operation_rounds(round, true),
            );
        }
    }

    Err(soak_liveness_invariant_failure(
        state,
        config,
        trace,
        catalog::LV_03_FEATURE_OPERATION_PROGRESS,
        format!(
            "membership transition removing {} did not reach stable target {:?} within {} post-heal rounds",
            monitor.removed_voter, monitor.target, monitor.operation_budget
        ),
    ))
}

fn report_recorded_membership_operation(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    monitor: &MembershipMonitor,
    recorder: &TerminalEvidenceRecorder,
    operation_rounds: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let Some(operation) = recorder.evidence() else {
        return Err(soak_liveness_harness_error(
            state,
            config,
            trace,
            "membership terminal recorder completed without evidence",
        ));
    };
    Ok(monitor.report(operation_rounds, operation))
}

fn issue_leave_joint_if_ready(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    monitor: &MembershipMonitor,
) -> Result<Option<NodeId>, SoakFailure> {
    let Some(leader) = membership_transition_ready_to_leave(state, &monitor.target) else {
        return Ok(None);
    };
    apply_to_state(state, Operation::LeaveJoint { to: leader });
    trace.push(SoakAction::LeaveJoint { to: leader });
    observed_actions.insert(SoakActionKind::LeaveJoint);
    check_soak_safety(state, config, trace)?;
    Ok(Some(leader))
}

pub(super) const fn operation_rounds(round: usize, drove_round: bool) -> usize {
    round.saturating_add(drove_round as usize)
}
