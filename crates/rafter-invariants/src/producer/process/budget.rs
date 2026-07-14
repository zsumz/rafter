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
    TestDiscovery,
    TestExecution,
    SimulatorExecution,
    MaelstromTrial,
}

impl ProcessKind {
    fn timeout_cap(self, budget: &ActiveLayerBudget) -> Option<Duration> {
        match self {
            Self::Compile => budget.compile_timeout,
            Self::TestDiscovery => budget.discovery_timeout,
            Self::TestExecution => budget.execution_timeout,
            Self::SimulatorExecution | Self::MaelstromTrial => None,
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
    if layer == "tla" {
        return Ok(None);
    }
    let total = configured_duration(runner, "layer_timeout")?;
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
    Ok(Some(ActiveLayerBudget {
        profile: profile.to_owned(),
        layer: layer.to_owned(),
        finalization_deadline: Instant::now() + execution_window,
        finalization_reserve,
        compile_timeout: optional("compile_timeout")?,
        discovery_timeout: optional("discovery_timeout")?,
        execution_timeout: optional("execution_timeout")?,
        policy: process_policy(runner)?,
    }))
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
    ACTIVE_LAYER_BUDGET.with(|active| {
        let active = active.borrow();
        let budget = active.as_ref().ok_or_else(|| {
            "bounded producer subprocess invoked outside an active layer budget".to_owned()
        })?;
        let remaining = budget
            .finalization_deadline
            .checked_duration_since(Instant::now())
            .unwrap_or(Duration::ZERO);
        let cleanup_allowance = budget
            .policy
            .termination_grace
            .saturating_add(budget.policy.kill_confirmation_timeout)
            .saturating_add(budget.policy.receipt_finalization_allowance);
        let available = remaining.checked_sub(cleanup_allowance).ok_or_else(|| {
            format!(
                "invariant profile {} layer {} exhausted its subprocess window before {kind:?}; preserving {}ms finalization reserve",
                budget.profile,
                budget.layer,
                duration_ms(budget.finalization_reserve)
            )
        })?;
        let timeout = [kind.timeout_cap(budget), requested_cap]
            .into_iter()
            .flatten()
            .fold(available, std::cmp::min);
        if timeout.is_zero() {
            return Err(format!(
                "invariant profile {} layer {} has no execution time left for {kind:?}; preserving {}ms finalization reserve",
                budget.profile,
                budget.layer,
                duration_ms(budget.finalization_reserve)
            ));
        }
        Ok((timeout, budget.policy))
    })
    .map_err(Into::into)
}
