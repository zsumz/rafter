//! Internal node state grouped by persistence and protocol ownership.
//!
//! Canonical durable state, process-local state, election rounds, leader-only
//! state, and recomputable indexes remain separate so each transition has an
//! obvious mutation boundary.

mod core;
mod derived;
mod election;
mod leader;
mod membership;
mod progress;
mod proposal;
mod snapshot;

#[cfg(test)]
mod derived_test;
#[cfg(test)]
mod election_test;
#[cfg(test)]
mod proposal_test;

pub(super) use core::{PersistentState, VolatileState};
pub(super) use derived::DerivedState;
pub(super) use election::ElectionState;
pub(super) use leader::{LeaderState, PendingLeadershipTransfer, PendingReadRound};
pub(super) use membership::{AcknowledgementSet, MembershipIndex, ProgressSet, SlotSet};
#[cfg(test)]
pub(super) use progress::Inflights;
pub(super) use progress::{Progress, ProgressMode};
pub(super) use proposal::{LocalProposal, LocalProposalTracker};
pub(super) use snapshot::IncomingSnapshotTransfer;
