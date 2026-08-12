//! Canonical contracts for the evidence runners selected by a profile.

mod maelstrom;
mod simulator;
mod tests_runner;
mod tla;
mod tla_obligations;
mod validate;

pub(super) use validate::validate_runner;

#[cfg(test)]
mod tests;
