//! Stable executable identities shared by registry and catalog models.

use super::profile::SimulatorLivenessContract;

/// Exact Cargo target and libtest identity for tests-layer evidence.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct TestIdentity {
    pub package: String,
    pub target_kind: String,
    pub target: String,
    pub test_name: String,
}

impl TestIdentity {
    /// Returns the stable check identity required in a tests-layer receipt.
    #[must_use]
    pub fn check_id(&self) -> String {
        format!(
            "tests/{}/{}/{}#{}",
            self.package, self.target_kind, self.target, self.test_name
        )
    }
}

/// Exact simulator legs, coverage floors, and detector qualification.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SimulatorIdentity {
    pub checks: Vec<String>,
    pub required_observation: String,
    pub minimum_observation: usize,
    pub minimum_protocol_states: Option<usize>,
    pub minimum_verifier_states: Option<usize>,
    pub minimum_runs_per_check: Option<usize>,
    pub minimum_steps: Option<usize>,
    pub liveness_report: Option<SimulatorLivenessContract>,
    pub negative_test: Option<TestIdentity>,
}
