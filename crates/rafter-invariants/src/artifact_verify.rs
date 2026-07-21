//! Test-only compatibility mounts for reviewed artifact-verifier identities.

mod compile;
mod simulator;
mod simulator_schedule;
mod test_logs;

pub(crate) use crate::verification::artifact::detector_log_verifier;
pub(crate) use crate::verification::artifact::test_support::verify_resource_metrics;
pub(crate) use crate::verification::{
    simulator::{
        event_semantics_test_support::verify_simulator_observations,
        schedule::validate_simulator_schedule, verify_liveness_observations,
    },
    verify_producer_invocation_paths,
};

pub(crate) const EVENT_PREFIX: &str = "RAFTER_EVENT ";

#[path = "artifact_verify/tests.rs"]
mod tests;

#[path = "artifact_verify/compile_tests.rs"]
mod compile_tests;
