//! Independent verification of simulator execution schedules and provenance.

mod compiler;
mod events;
mod invocation;
mod paths;
mod profile;
mod verify;

pub(crate) use events::ScannedSimulatorLog;
pub(crate) use verify::verify_simulator_schedule_authenticated;

#[cfg(test)]
pub(crate) use compiler::simulator_compiler_artifact_executable;
#[cfg(test)]
pub(crate) use events::{scan_machine_events, MAX_EVENTS_PER_LOG, MAX_EVENT_BYTES};
#[cfg(test)]
pub(crate) use invocation::{simulator_program_matches, verify_simulator_invocation_outcome};
#[cfg(test)]
pub(crate) use profile::validate_simulator_schedule;
#[cfg(test)]
pub(crate) use verify::verify_simulator_schedule;
