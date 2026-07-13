//! Membership-to-slot indexing for quorum evidence and replication progress.
//!
//! Node IDs remain the protocol vocabulary. These private indexes map them to
//! compact slots so stable and joint quorum operations share one deterministic
//! representation.

mod acknowledgements;
mod index;
mod progress;
mod slots;

#[cfg(test)]
mod tests;

pub(in crate::node) use acknowledgements::AcknowledgementSet;
pub(in crate::node) use index::MembershipIndex;
pub(in crate::node) use progress::ProgressSet;
pub(in crate::node) use slots::SlotSet;
