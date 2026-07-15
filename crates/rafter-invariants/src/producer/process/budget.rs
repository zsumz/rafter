use std::{
    cell::RefCell,
    collections::BTreeMap,
    error::Error,
    time::{Duration, Instant},
};

use crate::RunnerContract;

use super::duration_ms;

pub(super) const DEFAULT_KILL_CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_PROCESS_TERMINATION_GRACE: Duration = Duration::from_secs(30);
const DEFAULT_PROCESS_FINALIZATION_ALLOWANCE: Duration = Duration::from_secs(5);

thread_local! {
    static ACTIVE_LAYER_BUDGET: RefCell<Option<ActiveLayerBudget>> = const { RefCell::new(None) };
}

#[derive(Clone, Debug)]
pub(super) struct ActiveLayerBudget {
    pub(super) profile: String,
    pub(super) layer: String,
    pub(super) finalization_deadline: Instant,
    pub(super) total_deadline: Instant,
    pub(super) finalization_reserve: Duration,
    pub(super) compile_timeout: Option<Duration>,
    pub(super) discovery_timeout: Option<Duration>,
    pub(super) execution_timeout: Option<Duration>,
    pub(super) policy: ProcessPolicy,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct ProcessPolicy {
    pub(super) termination_grace: Duration,
    pub(super) kill_confirmation_timeout: Duration,
    pub(super) receipt_finalization_allowance: Duration,
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
    fn timeout_cap(self, budget: &ActiveLayerBudget) -> Option<Duration> {
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
    installed: bool,
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

pub(super) fn layer_budget(
    profile: &str,
    layer: &str,
    runner: &RunnerContract,
) -> Result<Option<ActiveLayerBudget>, Box<dyn Error>> {
    if !matches!(layer, "tests" | "simulator" | "tla" | "maelstrom") {
        return Err(format!("unsupported producer layer {layer}").into());
    }
    let total = configured_duration(
        runner,
        if layer == "tla" {
            "total_timeout"
        } else {
            "layer_timeout"
        },
    )?;
    let finalization_reserve = configured_duration(runner, "finalization_reserve")?;
    let execution_window = total
        .checked_sub(finalization_reserve)
        .filter(|window| !window.is_zero())
        .ok_or("producer finalization reserve must be smaller than its layer budget")?;
    let optional = |name| {
        runner
            .configuration
            .get(name)
            .map(|value| parse_contract_duration(name, value))
            .transpose()
    };
    let started = Instant::now();
    Ok(Some(ActiveLayerBudget {
        profile: profile.to_owned(),
        layer: layer.to_owned(),
        finalization_deadline: started
            .checked_add(execution_window)
            .ok_or("producer execution deadline overflow")?,
        total_deadline: started
            .checked_add(total)
            .ok_or("producer total deadline overflow")?,
        finalization_reserve,
        compile_timeout: optional("compile_timeout")?,
        discovery_timeout: optional("discovery_timeout")?,
        execution_timeout: optional("execution_timeout")?,
        policy: process_policy(runner)?,
    }))
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
        if budget.profile != profile || budget.layer != layer {
            return Err(format!(
                "active invariant producer budget is {}/{}, expected {profile}/{layer}",
                budget.profile, budget.layer
            )
            .into());
        }
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
        if budget.profile != profile || budget.layer != layer {
            return Err(format!(
                "active invariant producer budget is {}/{}, expected {profile}/{layer}",
                budget.profile, budget.layer
            )
            .into());
        }
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
        if budget.profile != profile || budget.layer != layer {
            return Err(format!(
                "active invariant producer budget is {}/{}, expected {profile}/{layer}",
                budget.profile, budget.layer
            )
            .into());
        }
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

fn process_policy(runner: &RunnerContract) -> Result<ProcessPolicy, Box<dyn Error>> {
    process_policy_from_configuration(&runner.configuration)
}

fn process_policy_from_configuration(
    configuration: &BTreeMap<String, String>,
) -> Result<ProcessPolicy, Box<dyn Error>> {
    Ok(ProcessPolicy {
        termination_grace: configured_map_duration(configuration, "termination_grace")?,
        kill_confirmation_timeout: configured_map_duration(
            configuration,
            "kill_confirmation_timeout",
        )?,
        receipt_finalization_allowance: configured_map_duration(
            configuration,
            "receipt_finalization_allowance",
        )?,
    })
}

fn configured_duration(runner: &RunnerContract, name: &str) -> Result<Duration, Box<dyn Error>> {
    let value = runner
        .configuration
        .get(name)
        .ok_or_else(|| format!("runner configuration omitted {name}"))?;
    parse_contract_duration(name, value)
}

fn configured_map_duration(
    configuration: &BTreeMap<String, String>,
    name: &str,
) -> Result<Duration, Box<dyn Error>> {
    let value = configuration
        .get(name)
        .ok_or_else(|| format!("runner configuration omitted {name}"))?;
    parse_contract_duration(name, value)
}

fn parse_contract_duration(name: &str, value: &str) -> Result<Duration, Box<dyn Error>> {
    let (amount, multiplier) = if let Some(value) = value.strip_suffix('s') {
        (value, 1)
    } else if let Some(value) = value.strip_suffix('m') {
        (value, 60)
    } else if let Some(value) = value.strip_suffix('h') {
        (value, 60 * 60)
    } else {
        return Err(
            format!("runner duration {name} must use whole seconds, minutes, or hours").into(),
        );
    };
    let amount = amount.parse::<u64>()?;
    let seconds = amount
        .checked_mul(multiplier)
        .ok_or_else(|| format!("runner duration {name} overflows"))?;
    if seconds == 0 {
        return Err(format!("runner duration {name} must be positive").into());
    }
    Ok(Duration::from_secs(seconds))
}

pub(super) fn active_process_timeout(
    kind: ProcessKind,
    requested_cap: Option<Duration>,
) -> Result<(Duration, ProcessPolicy), Box<dyn Error>> {
    active_process_timeout_for(kind, requested_cap, false)
}

pub(super) fn active_total_process_timeout(
    kind: ProcessKind,
    requested_cap: Option<Duration>,
) -> Result<(Duration, ProcessPolicy), Box<dyn Error>> {
    active_process_timeout_for(kind, requested_cap, true)
}

pub(super) fn has_active_layer_budget() -> bool {
    ACTIVE_LAYER_BUDGET.with(|active| active.borrow().is_some())
}

fn active_process_timeout_for(
    kind: ProcessKind,
    requested_cap: Option<Duration>,
    use_total_deadline: bool,
) -> Result<(Duration, ProcessPolicy), Box<dyn Error>> {
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
            .saturating_add(budget.policy.receipt_finalization_allowance);
        let deadline_scope = if use_total_deadline {
            "total"
        } else {
            "execution"
        };
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
        Ok((timeout, budget.policy))
    })
    .map_err(Into::into)
}
