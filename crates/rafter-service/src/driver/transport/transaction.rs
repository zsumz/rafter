//! The membership transaction a construction holds open across calls.
//!
//! A live report is one batch folded and installed inside one call, so its
//! candidate is a local. Construction's inputs arrive in three steps with the
//! group's own stepping machinery in between, so the candidate lives on the
//! driver until every one of them has been read — and the single installation
//! and single publication behind it happen here or not at all.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::checkpoint::{restore_checkpoint, RecordJoin};
use super::condition::Contradiction;
use super::observation::MembershipFact;
use super::reconciliation::StagedMembership;
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
    /// Opens the construction-wide membership transaction over a recovered
    /// record.
    ///
    /// **Construction ran two membership transactions and needed one.** The
    /// record was restored and installed; the replay's crossings were then folded
    /// and *published* as an ordinary batch; and only afterwards did the runtime's
    /// final endpoint get compared against the result. So a record whose position
    /// no crossing ties with — every crossing folding cleanly beneath it — got a
    /// peer set and a retirement floor onto the caller's link layer, and the
    /// endpoint then refused the construction. A `PeerPolicy` is the external
    /// installation of the whole admission policy rather than scratch state:
    /// nothing takes one back, and the process that stated it never started.
    ///
    /// So the record, every recovery membership event, and the runtime's endpoint
    /// fold into one candidate, and
    /// [`TransportDriverState::commit_membership_transaction`] is the single
    /// installation and the single publication behind it. Everything loss-tolerant
    /// the replay produces — peer messages, snapshot directives, proposal and read
    /// resolutions — routes normally throughout, for the reason the module header
    /// gives.
    ///
    /// A restored contradiction marker is recorded here rather than at the commit,
    /// and the order is what makes the freeze cover the replay: the guard in
    /// [`TransportDriverState::route_membership_events`] reads the driver's own
    /// state, so a marker set only at the end would let every replayed crossing
    /// move the register first.
    ///
    /// # Errors
    ///
    /// As [`TransportDriverState::adoption_candidate`], less the runtime probe.
    pub(super) fn open_membership_transaction(
        &mut self,
        checkpoint: PeerControlPlaneCheckpoint<G>,
    ) -> Result<(), ControlPlaneCheckpointError> {
        let mut candidate = self.membership_candidate();
        let restored = restore_checkpoint(
            &mut candidate,
            checkpoint,
            &self.group_id,
            RecordJoin::Resume,
        )?;
        if let Some(through) = restored {
            // **The record goes on before the marker does**, and that order is
            // load-bearing: [`TransportDriverState::assign_membership`] freezes
            // the durable half once the marker is set, so recording it first
            // would leave this driver holding an *empty* record and reporting it
            // to an embedder that would then persist the fork away. Nothing is
            // stated to the link layer — `install_restored_membership` does not
            // flush — and from here nothing ever will be.
            self.install_restored_membership(candidate);
            self.record_contradiction(Contradiction::restored(through).refusal());
            candidate = self.membership_candidate();
        }
        self.staged_membership = Some(StagedMembership {
            candidate,
            refused: None,
        });
        Ok(())
    }

    /// Closes it: folds the runtime's endpoint, installs once, and states the
    /// result once.
    ///
    /// The one publication the construction makes. Nothing reaches the link layer
    /// before this and nothing reaches it at all when any input refused, which is
    /// the whole of the transaction's promise one level up.
    ///
    /// **A driver whose record arrived already contradicted reads the endpoint
    /// for its live half alone.** The fold exists to derive a policy worth
    /// publishing and this driver will publish nothing ever again, so what is
    /// left of the endpoint is the two runtime facts — which the inbound
    /// admission check reads, and which a replica that must not serve still needs
    /// in order to catch up. Its record was installed when the transaction opened
    /// and is frozen where the marker says, so nothing here can move it.
    ///
    /// # Errors
    ///
    /// Returns the first refusal any input of the transaction produced. Nothing
    /// is installed and nothing is published on that path.
    pub(super) fn commit_membership_transaction(
        &mut self,
    ) -> Result<(), ControlPlaneCheckpointError> {
        let Some(staged) = self.staged_membership.take() else {
            // Unreachable behind the constructor, which opens one before this
            // runs. Publishing the endpoint the ordinary way is the honest
            // fallback rather than a panic: it is exactly what a driver with no
            // transaction open owes its link layer.
            return self.publish_adopted_membership();
        };
        let StagedMembership {
            mut candidate,
            refused,
        } = staged;
        if let Some(reason) = refused {
            self.record_contradiction(reason);
            return Err(reason);
        }
        if self.contradiction.is_some() {
            self.assign_runtime_membership();
            return Ok(());
        }
        if let Some(group) = self.group.as_ref() {
            let (committed, effective) = Self::adopted_observation(group.runtime());
            if let Err(reason) = candidate.apply(MembershipFact::Committed {
                committed,
                effective,
            }) {
                self.record_contradiction(reason);
                return Err(reason);
            }
        }
        self.install_membership(candidate);
        Ok(())
    }

    /// Assigns the two runtime facts straight from the group, touching no
    /// durable field.
    ///
    /// The one thing a frozen driver still takes from its runtime. Both are
    /// *observations* rather than conclusions — "what does this replica's own
    /// stream say the cluster has committed, and what is in effect here" — so
    /// neither is licensed by the record the marker froze, and the inbound
    /// admission check needs both to let a replica the cluster admitted afterward
    /// speak. A driver holding no group assigns nothing rather than clearing what
    /// it has: an absent runtime is not a narrowing.
    fn assign_runtime_membership(&mut self) {
        let Some(group) = self.group.as_ref() else {
            return;
        };
        let (committed, effective) = Self::adopted_observation(group.runtime());
        self.committed_members = committed.membership;
        self.effective_members = effective;
    }
}
