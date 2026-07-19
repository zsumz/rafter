//! Profile selection, runner policy, and bounded simulator contracts.

mod liveness;
mod load;
mod model;
mod runner_contract;
mod simulator;
mod validate;

pub(crate) use liveness::expected_execution_contract;
pub use liveness::{SimulatorExecutionContract, SimulatorLivenessContract};
pub use model::{ProfileContract, ProfileManifest, RunnerContract, SimulatorCheckContract};
pub(crate) use simulator::{
    per_check_observation_key, per_check_protocol_states_key, per_check_verifier_states_key,
    SimulatorRunnerConfiguration, SimulatorStateFloors,
};

#[cfg(test)]
mod tests;
