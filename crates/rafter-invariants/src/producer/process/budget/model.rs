//! Producer layer-budget state, process kinds, and policy adaptation.

use std::time::{Duration, Instant};

use super::super::{FinalizationPolicy, TerminationPolicy};

const DEFAULT_KILL_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_PROCESS_TERMINATION_GRACE: Duration = Duration::from_secs(30);
const DEFAULT_PROCESS_FINALIZATION_ALLOWANCE: Duration = Duration::from_secs(5);

#[derive(Clone, Debug)]
pub(in crate::producer::process) struct ActiveLayerBudget {
    pub(in crate::producer::process) profile: String,
    pub(in crate::producer::process) layer: String,
    pub(in crate::producer::process) finalization_deadline: Instant,
    pub(in crate::producer::process) total_deadline: Instant,
    pub(in crate::producer::process) finalization_reserve: Duration,
    pub(in crate::producer::process) compile_timeout: Option<Duration>,
    pub(in crate::producer::process) discovery_timeout: Option<Duration>,
    pub(in crate::producer::process) execution_timeout: Option<Duration>,
    pub(in crate::producer::process) policy: ProcessPolicy,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::producer::process) struct ProcessPolicy {
    pub(in crate::producer::process) termination_grace: Duration,
    pub(in crate::producer::process) kill_confirmation_timeout: Duration,
    pub(in crate::producer::process) receipt_finalization_allowance: Duration,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::producer::process) struct ProcessSchedule {
    pub(in crate::producer::process) execution_timeout: Duration,
    pub(in crate::producer::process) execution_window_deadline: Instant,
    pub(in crate::producer::process) cleanup_start_deadline: Instant,
    pub(in crate::producer::process) finalization_start_deadline: Instant,
    pub(in crate::producer::process) lifecycle_deadline: Instant,
    pub(in crate::producer::process) policy: ProcessPolicy,
}

impl ProcessSchedule {
    pub(in crate::producer::process) fn standalone(
        execution_timeout: Duration,
        policy: ProcessPolicy,
    ) -> Result<Self, &'static str> {
        let execution_allowance = policy
            .kill_confirmation_timeout
            .checked_add(execution_timeout)
            .ok_or("standalone process execution allowance overflow")?;
        let termination_allowance = policy
            .termination_grace
            .checked_add(policy.kill_confirmation_timeout)
            .ok_or("standalone process termination allowance overflow")?;
        let cleanup_allowance = policy.kill_confirmation_timeout;
        let reserved_allowance = termination_allowance
            .checked_add(cleanup_allowance)
            .and_then(|value| value.checked_add(policy.receipt_finalization_allowance))
            .ok_or("standalone process reserved allowance overflow")?;
        let lifecycle_allowance = execution_allowance
            .checked_add(reserved_allowance)
            .ok_or("standalone process lifecycle allowance overflow")?;
        let now = Instant::now();
        let execution_window_deadline = now
            .checked_add(execution_allowance)
            .ok_or("standalone process execution deadline overflow")?;
        let lifecycle_deadline = now
            .checked_add(lifecycle_allowance)
            .ok_or("standalone process lifecycle deadline overflow")?;
        let finalization_start_deadline = lifecycle_deadline
            .checked_sub(policy.receipt_finalization_allowance)
            .ok_or("standalone process finalization boundary underflow")?;
        let cleanup_start_deadline = finalization_start_deadline
            .checked_sub(cleanup_allowance)
            .ok_or("standalone process cleanup boundary underflow")?;
        Ok(Self {
            execution_timeout,
            execution_window_deadline,
            cleanup_start_deadline,
            finalization_start_deadline,
            lifecycle_deadline,
            policy,
        })
    }
}

impl Default for ProcessPolicy {
    fn default() -> Self {
        Self {
            termination_grace: DEFAULT_PROCESS_TERMINATION_GRACE,
            kill_confirmation_timeout: DEFAULT_KILL_CONFIRMATION_TIMEOUT,
            receipt_finalization_allowance: DEFAULT_PROCESS_FINALIZATION_ALLOWANCE,
        }
    }
}

impl ProcessPolicy {
    pub(in crate::producer::process) fn termination(self) -> TerminationPolicy {
        TerminationPolicy {
            grace: self.termination_grace,
            publication_timeout: self.kill_confirmation_timeout,
            kill_confirmation_timeout: self.kill_confirmation_timeout,
        }
    }

    pub(in crate::producer::process) fn finalization(self) -> FinalizationPolicy {
        FinalizationPolicy::bounded(self.receipt_finalization_allowance)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::producer) enum ProcessKind {
    Compile,
    Identity,
    TestDiscovery,
    TestExecution,
    SimulatorExecution,
    TlaExecution,
    MaelstromTrial,
}

impl ProcessKind {
    pub(super) fn timeout_cap(self, budget: &ActiveLayerBudget) -> Option<Duration> {
        match self {
            Self::Compile => budget.compile_timeout,
            Self::TestDiscovery => budget.discovery_timeout,
            Self::TestExecution => budget.execution_timeout,
            Self::Identity
            | Self::SimulatorExecution
            | Self::TlaExecution
            | Self::MaelstromTrial => None,
        }
    }
}

#[derive(Debug)]
pub(in crate::producer) struct LayerBudgetGuard {
    pub(super) installed: bool,
}
