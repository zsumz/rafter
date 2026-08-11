//! Profile selection, runner policy, and bounded simulator contracts.

mod liveness;
mod load;
mod model;
mod policy;
mod replay;
mod runner_contract;
mod simulator;
mod validate;

pub(crate) use liveness::expected_execution_contract;
pub use liveness::{SimulatorExecutionContract, SimulatorLivenessContract};
pub use model::{
    ObligationCompletion, ProfileContract, ProfileManifest, ProofObligationContract,
    RunnerContract, SimulatorCheckContract, VerifierContract,
};
pub use policy::{
    ClausePolicy, EvidenceLayer, EvidencePolicy, EvidenceStrength, RequiredClauseStrength,
};
pub use replay::{
    DetectorReplayArtifactPolicy, DetectorReplayBuild, DetectorReplayChallenge,
    DetectorReplayContract, DetectorReplayFixtureInventory, DetectorReplayPolicy,
    DetectorReplaySource, DetectorReplayTargetDirectory,
};
pub(crate) use simulator::{
    canonical_simulator_check_id, per_check_observation_key, per_check_protocol_states_key,
    per_check_verifier_states_key, scheduled_simulator_seeds, SimulatorRunnerConfiguration,
    SimulatorStateFloors,
};

#[cfg(test)]
mod tests;
