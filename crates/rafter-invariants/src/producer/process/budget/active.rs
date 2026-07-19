//! Thread-scoped layer-budget installation, deadline checks, and timeout allocation.

use std::{
    cell::RefCell,
    error::Error,
    time::{Duration, Instant},
};

use crate::contract::profile::RunnerContract;

use super::super::duration_ms;
use super::{layer_budget, ActiveLayerBudget, LayerBudgetGuard, ProcessKind, ProcessSchedule};

thread_local! {
    static ACTIVE_LAYER_BUDGET: RefCell<Option<ActiveLayerBudget>> = const { RefCell::new(None) };
}

impl LayerBudgetGuard {
    pub(in crate::producer) fn enter(
        profile: &str,
        layer: &str,
        runner: &RunnerContract,
    ) -> Result<Self, Box<dyn Error>> {
        let Some(budget) = layer_budget(profile, layer, runner)? else {
            return Ok(Self { installed: false });
        };
        ACTIVE_LAYER_BUDGET.with(|active| -> Result<(), Box<dyn Error>> {
            let mut active = active.borrow_mut();
            if active.is_some() {
                return Err("nested invariant producer subprocess budgets are forbidden".into());
            }
            *active = Some(budget);
            Ok(())
        })?;
        Ok(Self { installed: true })
    }
}

impl Drop for LayerBudgetGuard {
    fn drop(&mut self) {
        if self.installed {
            ACTIVE_LAYER_BUDGET.with(|active| {
                active.borrow_mut().take();
            });
        }
    }
}

pub(in crate::producer) fn active_layer_deadlines(
    profile: &str,
    layer: &str,
) -> Result<(Instant, Instant), Box<dyn Error>> {
    ACTIVE_LAYER_BUDGET.with(|active| {
        let active = active.borrow();
        let budget = active
            .as_ref()
            .ok_or("invariant producer has no active layer budget")?;
        require_matching_scope(budget, profile, layer)?;
        Ok((budget.finalization_deadline, budget.total_deadline))
    })
}

pub(in crate::producer) fn ensure_execution_deadline(
    profile: &str,
    layer: &str,
    operation: &str,
) -> Result<(), Box<dyn Error>> {
    ACTIVE_LAYER_BUDGET.with(|active| {
        let active = active.borrow();
        let budget = active
            .as_ref()
            .ok_or("invariant producer has no active layer budget")?;
        require_matching_scope(budget, profile, layer)?;
        if Instant::now() >= budget.finalization_deadline {
            return Err(format!(
                "invariant profile {profile} layer {layer} exhausted its execution budget before {operation}"
            )
            .into());
        }
        Ok(())
    })
}

pub(in crate::producer) fn ensure_total_deadline(
    profile: &str,
    layer: &str,
    operation: &str,
    require_receipt_allowance: bool,
) -> Result<(), Box<dyn Error>> {
    ACTIVE_LAYER_BUDGET.with(|active| {
        let active = active.borrow();
        let budget = active
            .as_ref()
            .ok_or("invariant producer has no active layer budget")?;
        require_matching_scope(budget, profile, layer)?;
        let remaining = budget
            .total_deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        let required = if require_receipt_allowance {
            budget.policy.receipt_finalization_allowance
        } else {
            Duration::ZERO
        };
        if remaining <= required {
            return Err(format!(
                "invariant profile {profile} layer {layer} exhausted its total budget before {operation}"
            )
            .into());
        }
        Ok(())
    })
}

fn require_matching_scope(
    budget: &ActiveLayerBudget,
    profile: &str,
    layer: &str,
) -> Result<(), Box<dyn Error>> {
    if budget.profile != profile || budget.layer != layer {
        return Err(format!(
            "active invariant producer budget is {}/{}, expected {profile}/{layer}",
            budget.profile, budget.layer
        )
        .into());
    }
    Ok(())
}

pub(in crate::producer::process) fn active_process_timeout(
    kind: ProcessKind,
    requested_cap: Option<Duration>,
) -> Result<ProcessSchedule, Box<dyn Error>> {
    active_process_timeout_for(kind, requested_cap, false)
}

pub(in crate::producer::process) fn active_total_process_timeout(
    kind: ProcessKind,
    requested_cap: Option<Duration>,
) -> Result<ProcessSchedule, Box<dyn Error>> {
    active_process_timeout_for(kind, requested_cap, true)
}

pub(in crate::producer::process) fn has_active_layer_budget() -> bool {
    ACTIVE_LAYER_BUDGET.with(|active| active.borrow().is_some())
}

fn active_process_timeout_for(
    kind: ProcessKind,
    requested_cap: Option<Duration>,
    use_total_deadline: bool,
) -> Result<ProcessSchedule, Box<dyn Error>> {
    ACTIVE_LAYER_BUDGET.with(|active| {
        let active = active.borrow();
        let budget = active.as_ref().ok_or_else(|| {
            "bounded producer subprocess invoked outside an active layer budget".to_owned()
        })?;
        let deadline = if use_total_deadline {
            budget.total_deadline
        } else {
            budget.finalization_deadline
        };
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        let cleanup_allowance = budget
            .policy
            .termination_grace
            .saturating_add(budget.policy.kill_confirmation_timeout)
            .saturating_add(budget.policy.kill_confirmation_timeout)
            .saturating_add(budget.policy.receipt_finalization_allowance);
        let deadline_scope = if use_total_deadline { "total" } else { "execution" };
        let preserved_allowance = if use_total_deadline {
            "subprocess termination and receipt cleanup allowance".to_owned()
        } else {
            format!(
                "{}ms layer finalization reserve plus subprocess termination and receipt cleanup allowance",
                duration_ms(budget.finalization_reserve)
            )
        };
        let available = remaining.checked_sub(cleanup_allowance).ok_or_else(|| {
            format!(
                "invariant profile {} layer {} exhausted its {deadline_scope} subprocess window before {kind:?}; preserving {preserved_allowance}",
                budget.profile, budget.layer
            )
        })?;
        let timeout = [kind.timeout_cap(budget), requested_cap]
            .into_iter()
            .flatten()
            .fold(available, std::cmp::min);
        if timeout.is_zero() {
            return Err(format!(
                "invariant profile {} layer {} has no {deadline_scope} time left for {kind:?}; preserving {preserved_allowance}",
                budget.profile, budget.layer
            ));
        }
        Ok(ProcessSchedule {
            execution_timeout: timeout,
            execution_window_deadline: deadline
                .checked_sub(cleanup_allowance)
                .ok_or("process execution window deadline underflow")?,
            cleanup_start_deadline: deadline
                .checked_sub(budget.policy.kill_confirmation_timeout)
                .and_then(|value| {
                    value.checked_sub(budget.policy.receipt_finalization_allowance)
                })
                .ok_or("process cleanup boundary underflow")?,
            finalization_start_deadline: deadline
                .checked_sub(budget.policy.receipt_finalization_allowance)
                .ok_or("process finalization boundary underflow")?,
            lifecycle_deadline: deadline,
            policy: budget.policy,
        })
    })
    .map_err(Into::into)
}
