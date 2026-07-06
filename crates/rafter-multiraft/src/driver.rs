//! Group-driver abstraction for many-group hosts.

use std::fmt::Debug;

use rafter_app::{group::GroupInput, group::GroupStepReport, metrics::RaftGroupMetrics};

/// Object-safe driver surface used by [`crate::host::MultiRaftHost`].
pub trait GroupDriver<G>: Debug {
    /// Steps one group input and returns explicit side effects.
    ///
    /// # Errors
    ///
    /// Returns an implementation-defined message when the group driver cannot
    /// process the input.
    fn step(
        &mut self,
        input: GroupInput<G, Vec<u8>>,
    ) -> Result<GroupStepReport<G, Vec<u8>>, String>;

    fn metrics(&self) -> RaftGroupMetrics<G>;
}
