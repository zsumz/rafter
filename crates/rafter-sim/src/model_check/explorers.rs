//! Bounded state-space explorers, one per property under check.
//!
//! Each explorer owns which actions are enabled from a state and which
//! invariants run after every transition, so a check's coverage is decided in
//! exactly one place. Explorers are internal; the entry points in `checks` are
//! the only supported way to run one.

mod budget;
mod commit;
mod election;
mod read;
mod restart;

pub(super) use budget::protocol_state_fingerprint;
pub(super) use commit::CommitSafetyExplorer;
pub(super) use election::ElectionSafetyExplorer;
pub(super) use read::ReadIndexSafetyExplorer;
pub(super) use restart::RestartSafetyExplorer;
