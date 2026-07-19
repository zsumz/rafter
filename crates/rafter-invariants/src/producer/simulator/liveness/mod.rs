//! Producer-side classification of raw simulator liveness events.

mod binding;
mod error;
mod inventory;
mod raw;

pub(super) use binding::derive_liveness_binding;
pub(super) use error::LivenessReportErrorKind;
