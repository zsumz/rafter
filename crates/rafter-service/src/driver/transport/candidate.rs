//! The membership fields of one driver, staged.
//!
//! A plain value with no transport, no group, and no epoch, so it can refuse and
//! refusing costs a drop. Every fact of a batch folds into one of these, and the
//! driver installs it whole or not at all. What one *committed* fact does to it
//! is [`committed`], because that is the only place identity is consumed.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use std::collections::BTreeSet;

use super::checkpoint::{merge_current_state, CurrentCommittedState, IncomingObservation};
use super::observation::{CommittedObservation, MembershipFact};
use super::*;

mod committed;

/// The membership fields of one driver, staged.
///
/// Every field here is one the driver installs as a unit or not at all. The
/// candidate is a plain value with no transport, no group, and no epoch: it can
/// refuse, and refusing costs a drop.
pub(super) struct MembershipCandidate {
    /// The configuration this replica is operating under, as last reported.
    pub(super) effective_members: BTreeSet<NodeId>,
    /// The configuration the cluster has committed, raw as reported.
    pub(super) committed_members: BTreeSet<NodeId>,
    /// The committed membership this candidate believes is current, positioned.
    pub(super) current_committed: Option<CurrentCommittedState>,
    /// The greatest `NodeId` any committed configuration has named.
    pub(super) committed_id_high_water: Option<NodeId>,
}

impl MembershipCandidate {
    /// The membership this candidate's register names, or the empty set.
    fn live_members(&self) -> &BTreeSet<NodeId> {
        static NONE: BTreeSet<NodeId> = BTreeSet::new();
        self.current_committed
            .as_ref()
            .map_or(&NONE, |current| &current.membership)
    }

    /// Whether a committed removal has consumed `node_id`, by this candidate's
    /// own record.
    ///
    /// The same two reads
    /// [`TransportDriverState::is_spent`](super::state::TransportDriverState::is_spent)
    /// makes, against the
    /// staged fields rather than the installed ones — which is what lets the
    /// adoption gate ask about an offered node ID before anything is installed.
    pub(super) fn is_spent(&self, node_id: NodeId) -> bool {
        self.committed_id_high_water
            .is_some_and(|mark| node_id <= mark)
            && !self.live_members().contains(&node_id)
    }

    /// The spent test as it stood *before* the fact being folded.
    ///
    /// Read against a mark the incoming fact had already raised, an identity this
    /// driver has simply not observed yet — every identity at all, on the very
    /// first fact — would test as spent, and the membership the fact names would
    /// filter down to nothing. So the closure captures the mark and the live set
    /// first, and the fold reads it.
    fn spent_before(&self) -> impl Fn(NodeId) -> bool {
        let mark = self.committed_id_high_water;
        let live = self.live_members().clone();
        move |node_id: NodeId| mark.is_some_and(|mark| node_id <= mark) && !live.contains(&node_id)
    }

    /// Folds one membership fact into this candidate.
    ///
    /// The committed half goes first, because it is the one that can refuse: an
    /// effective membership assigned ahead of a refusal would be half a fact
    /// applied, and the candidate is dropped whole on a refusal precisely so that
    /// cannot matter to the driver.
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when
    /// the fact and this candidate stand at one position and disagree about the
    /// membership there.
    pub(super) fn apply(
        &mut self,
        fact: MembershipFact,
    ) -> Result<(), ControlPlaneCheckpointError> {
        match fact {
            // Assigned, not merged. The effective configuration moves in both
            // directions — a new leader can truncate an uncommitted one back off
            // the log — and it still cannot narrow what this driver authorizes,
            // because every derivation takes it in union with the committed floor
            // and the register.
            MembershipFact::Effective(effective) => self.effective_members = effective,
            MembershipFact::Committed {
                committed,
                effective,
            } => {
                self.observe_committed(committed)?;
                self.effective_members = effective;
            }
        }
        Ok(())
    }

    /// Asks whether one committed fact would contradict this candidate, without
    /// folding it.
    ///
    /// The adoption precheck. A runtime whose committed membership disagrees with
    /// the record at a position both observed is a supervisor handing over a
    /// replica that must not open, not a replica that opens and then reports
    /// itself sick — so the merge is run and its answer discarded, and the whole
    /// candidate is installed only if it agrees.
    ///
    /// # Errors
    ///
    /// As [`MembershipCandidate::observe_committed`].
    pub(super) fn probe(
        &self,
        fact: &CommittedObservation,
    ) -> Result<(), ControlPlaneCheckpointError> {
        self.check_ancestry(fact)?;
        let was_spent = self.spent_before();
        merge_current_state(
            self.current_committed.as_ref(),
            &IncomingObservation {
                through: fact.through,
                membership: &fact.membership,
                proven_removed: &fact.removed,
            },
            &was_spent,
        )
        .map(|_| ())
    }
}
