//! Producer policy and evidence adaptation for neutral process execution.
//!
//! This domain allocates profile and layer budgets, freezes invocation
//! provenance, delegates mechanics to `execution::process`, and binds the raw
//! result into serialized evidence. It does not own launch or cleanup logic.

mod adapter;
mod api;
mod budget;
mod evidence;
mod model;
mod output;
mod runtime;
#[cfg(test)]
mod tests;

pub(crate) use crate::execution::process::base_environment;
pub(super) use crate::execution::process::duration_ms;
use crate::execution::process::{FinalizationPolicy, TerminationPolicy};

pub(super) use adapter::timed_with_timeout;
#[cfg(test)]
use adapter::timed_with_timeout_and_policy_and_descriptors;
#[cfg(test)]
use adapter::{
    process_execution_policy, timed_with_timeout_after_bind, timed_with_timeout_after_run,
};
use adapter::{timed_with_schedule_and_descriptors, timed_with_timeout_and_policy};

#[cfg(test)]
use api::identity_command_with_timeout;
#[cfg(target_os = "linux")]
pub(super) use api::timed_for_with_cap_and_descriptors;
pub(crate) use api::{identity_command, identity_command_in, identity_command_in_total_budget};
pub(super) use api::{timed_for, timed_for_with_cap, timed_with_optional_layer_budget};

#[cfg(test)]
use budget::layer_budget;
pub(super) use budget::{
    active_layer_deadlines, ensure_execution_deadline, ensure_total_deadline, LayerBudgetGuard,
    ProcessKind,
};
use budget::{
    active_process_timeout, active_total_process_timeout, has_active_layer_budget, ProcessPolicy,
    ProcessSchedule,
};

use evidence::bind_invocation;
#[cfg(test)]
pub(crate) use evidence::expected_invocation;
pub(super) use evidence::{combined_detector_log, combined_log, json_log, tla_json_log};

pub(crate) use model::IdentityOutput;
pub(super) use model::ProcessOutput;
use output::bind_process_output;
pub(crate) use runtime::capture_runtime_receipts;
