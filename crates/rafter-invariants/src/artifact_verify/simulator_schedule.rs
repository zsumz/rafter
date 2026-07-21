//! Compatibility facade for domain-owned simulator schedule verification.

#[cfg(test)]
mod events;

#[cfg(test)]
pub(super) use events::scan_machine_events;

#[cfg(test)]
pub(super) use crate::verification::{
    simulator::schedule::{
        simulator_compiler_artifact_executable, simulator_program_matches,
        verify_simulator_invocation_outcome, verify_simulator_schedule,
    },
    AggregateError,
};

#[cfg(test)]
#[path = "../verification/simulator/schedule/tests/provenance.rs"]
mod provenance_tests;
