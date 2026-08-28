//! Message routing types for the simulated network.
//!
//! An envelope carries sender, recipient, and payload; queueing adds only the
//! tick at which it becomes deliverable. Ordering, faults, and the effects of
//! delivery belong to the submodules, not to these records.

mod delivery;
pub(crate) use delivery::ExecutionInstrumentationError;
mod faults;

use rafter::NodeId;

/// A queued or delivered Raft message with simulator routing metadata.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Envelope {
    pub from: NodeId,
    pub to: NodeId,
    pub message: rafter::Message,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(super) struct QueuedEnvelope {
    pub(super) ready_at: crate::SimTick,
    pub(super) envelope: Envelope,
}
