//! What this driver hands its embedder to make durable.
//!
//! The checkpoint a caller persists, the epoch that says when to, and the live
//! reading of the register the spent test is judged against. Reads of state the
//! reconciliation transaction owns, and one counter it moves; nothing here
//! decides what a record means.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use std::collections::BTreeSet;

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::checkpoint::CurrentCommittedState;
use super::condition::Contradiction;
use super::state::TransportDriverState;
use super::*;

/// Whether a record holding this mark and this current state spends `node_id`.
///
/// The two reads [`TransportDriverState::is_spent`] makes, over the halves of a
/// record rather than over the record — which is what lets the join read a
/// checkpoint's spent-ness after moving its fields out.
pub(super) fn spends_under(
    mark: Option<NodeId>,
    current: Option<&CurrentCommittedState>,
    node_id: NodeId,
) -> bool {
    mark.is_some_and(|mark| node_id <= mark)
        && !current.is_some_and(|current| current.membership.contains(&node_id))
}

impl<G, A, R, T, V> TransportDriverState<G, A, R, T, V>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine + Send + 'static,
    A::Command: Send + 'static,
    A::CommandResult: Clone + Send + 'static,
    A::Query: Clone + Send + 'static,
    A::QueryResult: Send + 'static,
    R: PersistedRaftRuntime + Send + 'static,
    T: RaftTransport<G>,
    V: AuthenticatedPeerValidator<G, T::PeerPrincipal> + Send + Sync + 'static,
{
    /// Records that the checkpointable control-plane state moved.
    ///
    /// Called wherever one of the checkpoint fields changes and nowhere else, so
    /// an embedder that persists on every epoch move persists exactly the
    /// changes it must not lose. Saturating rather than wrapping: an epoch that
    /// wrapped past a caller's last-persisted value would report "no change" for
    /// a state that had changed, and a driver that reached `u64::MAX`
    /// configuration changes has an embedder that should persist unconditionally
    /// from then on.
    pub(super) fn advance_checkpoint_epoch(&mut self) {
        self.checkpoint_epoch = self.checkpoint_epoch.saturating_add(1);
    }

    /// Returns the peer-control-plane state this driver's embedder must make
    /// durable.
    pub(super) fn control_plane_checkpoint(&self) -> PeerControlPlaneCheckpoint<G> {
        PeerControlPlaneCheckpoint {
            group: self.group_id.clone(),
            committed_id_high_water: self.committed_id_high_water,
            current_committed: self.current_committed.clone(),
            contradicted_at: self.contradiction.map(Contradiction::through),
        }
    }

    /// The membership this driver's current state names, or the empty set.
    pub(super) fn live_committed_members(&self) -> &BTreeSet<NodeId> {
        static NONE: BTreeSet<NodeId> = BTreeSet::new();
        self.current_committed
            .as_ref()
            .map_or(&NONE, |current| &current.membership)
    }
}
