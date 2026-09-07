//! Feature-specific validation of report evidence.
//!
//! Each liveness feature admits exactly one set of clause IDs, one stable
//! leader window shape, and one set of terminal outcomes, so a report that
//! names a feature cannot carry another feature's evidence.

use super::super::driver::{ProposalTerminalOutcome, STABLE_LEADER_WINDOW_ROUNDS};
use super::{
    OperationTerminalOutcome, StableLeaderEvidence, LV_01_CONVERGENCE_CLAUSE_IDS,
    LV_01_USABILITY_CLAUSE_IDS, LV_02_PROGRESS_CLAUSE_IDS, LV_02_TERMINATION_CLAUSE_IDS,
    LV_03_MEMBERSHIP_CLAUSE_IDS, LV_03_READ_CLAUSE_IDS, LV_03_SNAPSHOT_CLAUSE_IDS,
    LV_03_TRANSFER_CLAUSE_IDS,
};

pub(super) fn expected_clause_ids(feature_id: &str) -> Option<&'static [&'static str]> {
    match feature_id {
        "leader-convergence" | "quorum-only-leader-convergence" => {
            Some(LV_01_CONVERGENCE_CLAUSE_IDS)
        }
        "leader-usability" | "quorum-only-leader-usability" => Some(LV_01_USABILITY_CLAUSE_IDS),
        "proposal-progress" => Some(LV_02_PROGRESS_CLAUSE_IDS),
        "proposal-termination" => Some(LV_02_TERMINATION_CLAUSE_IDS),
        "read-barrier" => Some(LV_03_READ_CLAUSE_IDS),
        "snapshot-catch-up" => Some(LV_03_SNAPSHOT_CLAUSE_IDS),
        "membership-transition" => Some(LV_03_MEMBERSHIP_CLAUSE_IDS),
        "leadership-transfer" => Some(LV_03_TRANSFER_CLAUSE_IDS),
        _ => None,
    }
}

pub(super) fn validate_stable_window(
    feature_id: &str,
    evidence: StableLeaderEvidence,
    rounds_used: usize,
) -> Result<(), String> {
    let valid = match feature_id {
        "leader-convergence"
        | "leader-usability"
        | "quorum-only-leader-convergence"
        | "quorum-only-leader-usability"
        | "read-barrier" => {
            evidence.stable_rounds == STABLE_LEADER_WINDOW_ROUNDS
                && evidence.remained_leader_through_probe
        }
        "proposal-progress" => {
            evidence.stable_rounds == rounds_used.max(1) && evidence.remained_leader_through_probe
        }
        "proposal-termination" => {
            evidence.stable_rounds == 1 && !evidence.remained_leader_through_probe
        }
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err("stable-leader window does not match monitor semantics".to_owned())
    }
}

pub(super) fn validate_proposal_outcome(
    feature_id: &str,
    outcome: ProposalTerminalOutcome,
) -> Result<(), String> {
    let valid = match feature_id {
        "leader-usability" | "quorum-only-leader-usability" | "proposal-progress" => {
            outcome == ProposalTerminalOutcome::Committed
        }
        "proposal-termination" => matches!(
            outcome,
            ProposalTerminalOutcome::Committed
                | ProposalTerminalOutcome::Rejected
                | ProposalTerminalOutcome::Unknown
        ),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err("proposal terminal outcome does not match monitor semantics".to_owned())
    }
}

pub(super) fn validate_operation_outcome(
    feature_id: &str,
    outcome: OperationTerminalOutcome,
) -> Result<(), String> {
    let valid = match feature_id {
        "read-barrier" => matches!(
            outcome,
            OperationTerminalOutcome::Completed
                | OperationTerminalOutcome::Rejected
                | OperationTerminalOutcome::Canceled
        ),
        "snapshot-catch-up" => outcome == OperationTerminalOutcome::Installed,
        "membership-transition" => matches!(
            outcome,
            OperationTerminalOutcome::Committed | OperationTerminalOutcome::Rejected
        ),
        "leadership-transfer" => matches!(
            outcome,
            OperationTerminalOutcome::Completed | OperationTerminalOutcome::Rejected
        ),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err("operation terminal outcome does not match monitor semantics".to_owned())
    }
}
