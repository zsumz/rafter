//! Simulator model compilation, execution, fixtures, and event-collection facade.

mod build;
#[cfg(all(test, unix))]
mod fixtures;
mod runner;
mod types;

#[cfg(test)]
pub(in crate::producer) use build::reset_simulator_build_scratch;
#[cfg(all(test, unix))]
pub(in crate::producer) use fixtures::{later_launch_error_fixture, timed_out_zero_exit_fixture};
#[cfg(all(test, unix))]
pub(crate) use fixtures::{
    later_launch_error_fixture_at, timed_out_zero_exit_fixture_at, SimulatorFixtureInvocation,
};
pub(in crate::producer) use runner::{canonical_check_id, execute};
pub(in crate::producer) use types::SimulatorExecution;

#[cfg(test)]
use runner::execution_plan;

#[cfg(test)]
#[path = "../simulator_model_tests.rs"]
mod tests;
