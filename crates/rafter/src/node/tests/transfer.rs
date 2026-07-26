//! Leadership-transfer validation, catch-up, handoff, and `TimeoutNow` scenarios.

pub(super) use super::helpers::{elect_leader, node};
pub(super) use super::*;

mod handoff;
mod lease_waiver;
mod support;
mod timeout;
mod validation;
