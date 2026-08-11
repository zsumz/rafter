//! TLA+ evidence-production facade and stable producer test mount.

pub(in crate::producer) mod checkpoint;
pub(in crate::producer) mod contract;
mod evaluation;
pub(in crate::producer) mod execution;
mod result;
mod runner;

pub(super) use runner::run;

pub(super) use super::{artifact, process, source, tla_output, ProducerContext};

#[cfg(test)]
pub(super) use evaluation::{evaluate, observations, TlaVerdict};
#[cfg(test)]
pub(super) use execution::{MainStatus, ObligationOutcome, ProbeStatus, TlaExecution};
#[cfg(test)]
pub(super) use result::evidence_result;

#[cfg(test)]
#[path = "tla_tests.rs"]
mod tests;
