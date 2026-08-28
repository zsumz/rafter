//! Moving a surviving candidate onto the driver.
//!
//! The one place the staged fields become the driver's own, and therefore the
//! one place the epoch an embedder persists against moves. A contradicted driver
//! takes the live half and keeps the record it froze, and that rule lives here
//! so every installer inherits it rather than remembering it.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::candidate::MembershipCandidate;
use super::state::TransportDriverState;
use super::*;

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
    /// Stages this driver's membership fields.
    pub(super) fn membership_candidate(&self) -> MembershipCandidate {
        MembershipCandidate {
            effective_members: self.effective_members.clone(),
            committed_members: self.committed_members.clone(),
            current_committed: self.current_committed.clone(),
            committed_id_high_water: self.committed_id_high_water,
        }
    }

    /// Installs a candidate that survived its whole batch, and states it once.
    ///
    /// The flush is the single publication the transaction promises — after every
    /// field is in place, so the policy it derives describes the whole batch
    /// rather than a prefix of it.
    pub(super) fn install_membership(&mut self, candidate: MembershipCandidate) {
        self.assign_membership(candidate);
        self.flush_peer_policy();
    }

    /// Installs a candidate a *record* produced, and states nothing yet.
    ///
    /// **A restored record is not on its own a publishable statement**, and both
    /// entry points that restore one reach the runtime's endpoint immediately
    /// afterwards. A record says what this driver has spent; what the link layer
    /// is owed is derived once the record and the runtime have met, and that
    /// meeting can still refuse. Publishing between the two would state a floor
    /// licensed by half the inputs — which is the same defect the transaction
    /// exists for, one level up.
    pub(super) fn install_restored_membership(&mut self, candidate: MembershipCandidate) {
        self.assign_membership(candidate);
    }

    /// Moves the staged fields onto the driver, advancing the epoch if the
    /// checkpointable half moved.
    ///
    /// The epoch moves only if the *checkpointable* half actually changed, which
    /// is the contract an embedder persists against: a batch whose facts the
    /// driver had already absorbed asks nobody to write a file.
    ///
    /// **A contradicted driver takes the live half and keeps the record it
    /// froze**, and this is the one place that rule lives so every installer
    /// inherits it. `contradicted_at` used to stop the flush and nothing else, so
    /// ordinary reconciliation went on folding later batches into the mark and
    /// the register and went on advancing the epoch — the embedder persisted a
    /// *newer* record carrying no trace of the fork, and a restart from it
    /// started clean. The unresolved same-position fork disappeared across a
    /// restart, which is the one thing a terminal state must not permit.
    ///
    /// **The live half is deliberately not frozen with it**, and the asymmetry is
    /// the same one [`super::policy`] already draws. The two runtime facts answer
    /// "who is this replica's own stream saying may speak", which is what the
    /// inbound admission check reads — and a driver in this state is still
    /// supposed to be a useful follower, still stepping and still catching up.
    /// Freezing them would refuse frames from every replica the cluster admits
    /// afterwards, which stops the catch-up the terminal state explicitly allows.
    /// Nothing is derived from them either way: the flush is guarded, so the two
    /// halves cannot come apart in anything this driver *states*.
    fn assign_membership(&mut self, candidate: MembershipCandidate) {
        self.effective_members = candidate.effective_members;
        self.committed_members = candidate.committed_members;
        if self.contradiction.is_some() {
            return;
        }
        let before = self.control_plane_checkpoint();
        self.current_committed = candidate.current_committed;
        self.committed_id_high_water = candidate.committed_id_high_water;
        if before != self.control_plane_checkpoint() {
            self.advance_checkpoint_epoch();
        }
    }
}
