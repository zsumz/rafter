//! Terminal-evidence recording for one monitored operation.
//!
//! The recorder latches the first terminal outcome it observes and never
//! revises it, so a monitor that keeps running past completion cannot
//! overwrite the evidence its report is built from.

use super::{
    OperationEvidence, OperationTerminalOutcome, TerminalEvidenceRecorder, TerminalRecorderMode,
};

impl OperationTerminalOutcome {
    pub(super) const fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Rejected => "rejected",
            Self::Canceled => "canceled",
            Self::Committed => "committed",
            Self::Installed => "installed",
            Self::Unknown => "unknown",
        }
    }
}

impl OperationEvidence {
    pub(super) fn new(operation_id: String, outcome: OperationTerminalOutcome) -> Self {
        Self {
            operation_id,
            outcome,
        }
    }
}

impl TerminalEvidenceRecorder {
    pub(super) fn new(operation_id: String, mode: TerminalRecorderMode) -> Self {
        Self {
            operation_id,
            mode,
            evidence: None,
        }
    }

    pub(super) fn observe(&mut self, outcome: Option<OperationTerminalOutcome>) -> bool {
        if self.evidence.is_some() {
            return true;
        }
        let Some(outcome) = outcome else {
            return false;
        };
        match self.mode {
            TerminalRecorderMode::Production => {
                self.evidence = Some(OperationEvidence::new(self.operation_id.clone(), outcome));
            }
            #[cfg(test)]
            TerminalRecorderMode::DropTerminalRecord => {}
        }
        self.evidence.is_some()
    }

    pub(super) fn evidence(&self) -> Option<OperationEvidence> {
        self.evidence.clone()
    }
}
