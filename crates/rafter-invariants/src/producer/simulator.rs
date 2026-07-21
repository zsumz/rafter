//! Simulator evidence production domains and compatibility exports.

mod check_contract;
mod detector;
mod evaluation;
mod events;
mod issue;
mod liveness;
pub(in crate::producer) mod model;
mod observation;
mod resources;
mod runner;
mod verdict;

#[cfg(test)]
use crate::producer::test_exec;
use issue::{merge_issue, SimulatorIssue};

#[allow(unused_imports)]
pub(crate) use events::passing_simulator_event_contract;
pub(super) use runner::run;

#[cfg(test)]
pub(crate) use evaluation::evaluate_model_fixture;

#[cfg(test)]
use check_contract::liveness_contracts;
#[cfg(test)]
use detector::{unique_detector_identities, DetectorRun};
#[cfg(test)]
use evaluation::{evaluate, evaluate_descriptors};
#[cfg(test)]
use events::simulator_event_issue;
#[cfg(test)]
use observation::{coverage_reached, model_observations};
#[cfg(test)]
use resources::execution_resource_metrics;

#[cfg(test)]
#[path = "simulator_detector_identity_tests.rs"]
mod detector_identity_tests;

#[cfg(test)]
#[path = "simulator_tests.rs"]
mod tests;
