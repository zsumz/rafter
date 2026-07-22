//! Bounded TLC execution, probe, budget, and process facade.

mod budget;
mod command;
mod model;
mod outcome;
mod probes;
mod runner;

pub(in crate::producer) use model::{MainStatus, ProbeStatus, TlaExecution};
#[cfg(test)]
pub(in crate::producer) use outcome::parse_main_summary;
#[cfg(test)]
pub(in crate::producer) use probes::detector_qualified;
pub(super) use runner::execute;

#[cfg(test)]
pub(super) use super::process;
#[cfg(test)]
use budget::{
    configured_budget_duration, maximum_qualification_time, mutation_suite_timeout, probe_timeout,
    ExecutionBudget, FINALIZATION_RESERVE_KEY, QUALIFICATION_PHASE_COUNT, TOTAL_TIMEOUT_KEY,
};
#[cfg(all(test, not(target_os = "linux")))]
use command::require_sound_tlc_state_binding;
#[cfg(test)]
use model::{DetectorProbes, TlcRun};
#[cfg(test)]
use outcome::{complete_main_execution, MainCompletion};
#[cfg(test)]
use probes::{mutation_suite_qualified, REQUIRED_MUTATION_TESTS};

#[cfg(test)]
#[path = "../tla_exec_budget_tests.rs"]
mod budget_tests;

#[cfg(test)]
#[path = "../tla_exec_policy_tests.rs"]
mod policy_tests;
