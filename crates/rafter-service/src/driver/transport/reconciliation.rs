#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! One membership statement per batch of facts, or none at all.
//!
//! **The staged transaction, and it exists because a retirement floor is
//! permanent.** The facts that license a publication do not arrive one at a time.
//! An adoption offers a durable record *and* a runtime; one step's report can
//! carry an effective change, a crossing, and a committed endpoint together; a
//! recovery replay carries every crossing it produced in one report. Each of
//! those used to be merged into live state and flushed to the link layer the
//! moment it was read — so a contradiction in the second fact arrived after the
//! first had already been written into the checkpoint an embedder persists and
//! stated to the transport, and neither of those is retractable.
//!
//! So every site here does the same three things. Clone the membership fields
//! into a [`MembershipCandidate`]; fold every fact of the batch into it, keeping
//! the first refusal; and install the candidate — with exactly one
//! [`TransportDriverState::flush_peer_policy`] behind it — only if the whole
//! batch survived. A batch that refuses leaves the driver holding the last
//! consistent state it had, not the prefix that happened to parse, and tells the
//! link layer nothing.
//!
//! **What is deliberately outside the transaction is everything loss-tolerant.**
//! A report's peer messages, snapshot directives, and proposal and read
//! resolutions route normally, before and after a refusal alike: Raft re-sends a
//! dropped frame, a waiter resolved is a waiter that stays resolved, and neither
//! is a permanent statement about who may speak. Only the control-plane statement
//! is transactional, because only it cannot be taken back.
//!
//! The candidate holds the same four fields the driver does, and no more. The two
//! checkpointable ones — the mark and the register — are what an embedder
//! persists; the two live ones are what admission and the local service state are
//! read from. Splitting them across two transactions would let a refusal leave a
//! published peer set describing a membership the durable record does not agree
//! with, which is the same defect one layer down.

use std::collections::BTreeSet;

use crate::transport::{AuthenticatedPeerValidator, RaftTransport};

use super::super::*;
use super::condition::Contradiction;
use super::observation::{observed_membership, MembershipFact, ObservedMembership};
use super::state::TransportDriverState;

pub(super) use super::candidate::MembershipCandidate;

/// One membership transaction that outlives a single batch.
///
/// **Construction's transaction, and it is the only one that has to be held
/// across calls.** A live report is one batch folded and installed inside one
/// call, so its candidate is a local. Construction's inputs arrive in three
/// separate steps — the recovered record, then every crossing the replay
/// produces, then the runtime's own endpoint — with the group's stepping
/// machinery in between, so the candidate has to live on the driver for the
/// routing path to reach it.
///
/// The refusal travels with it for the same reason the candidate does. Recovery
/// routing is reached from `route_report`, which returns nothing, so a batch that
/// refuses records here and the constructor asks once every input has been read.
pub(super) struct StagedMembership {
    /// The candidate every input of the construction folds into.
    pub(super) candidate: MembershipCandidate,
    /// The first refusal any of them produced, if one did.
    ///
    /// First rather than last, matching the batch rule: once a candidate has
    /// refused it is dropped whole, so nothing after the first refusal is folded
    /// and a later one would be a refusal of a fact this transaction never
    /// reached.
    pub(super) refused: Option<ControlPlaneCheckpointError>,
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
    /// Folds every membership event of one report into one candidate and
    /// installs it, or installs none of them.
    ///
    /// **A contradiction discovered here is recorded rather than returned**, and
    /// there is nowhere else for it to go: this runs from `route_report` and
    /// `reconcile_membership`, which are reached from every step outcome
    /// including a failing one. What it must never do is publish anyway —
    /// [`TransportDriverState::install_membership`] is not reached on that path,
    /// so the driver keeps the state and the policy it had, and
    /// [`DriverServiceState::ContradictoryCurrentState`] is how a supervisor
    /// hears about it.
    pub(super) fn route_membership_events(&mut self, events: &[MembershipEvent<G>]) {
        // **A batch of nothing installs nothing and states nothing.** Most steps
        // move no membership at all, and the error-path reconciliation is empty
        // after every successful one — so without this the commonest path in the
        // driver would re-derive and re-attempt a policy the link layer has
        // already been offered, turning a refused publication's retry into
        // something that happens per step rather than per entry point. The
        // entry points flush on their own; this is a reconciliation.
        if events.is_empty() {
            return;
        }
        // Construction's transaction is open, so this batch is one input of a
        // larger one: it folds into the candidate that is being staged and
        // installs nothing. See [`StagedMembership`].
        //
        // A construction that has *already* recorded a contradiction — its
        // recovered record arrived carrying the durable marker — folds nothing at
        // all. There is no conclusion left for the transaction to reach: it will
        // publish nothing and its durable record is frozen where the marker says,
        // so staging a fact would only be work whose result is discarded.
        if let Some(mut staged) = self.staged_membership.take() {
            if staged.refused.is_none() && self.contradiction.is_none() {
                staged.refused = self
                    .fold_membership_events(&mut staged.candidate, events)
                    .err();
            }
            self.staged_membership = Some(staged);
            return;
        }
        // The refusal is already recorded on the state by the time this returns;
        // there is no caller here that could act on a second copy of it.
        let _ = self.absorb_membership_events(events);
    }

    fn absorb_membership_events(
        &mut self,
        events: &[MembershipEvent<G>],
    ) -> Result<(), ControlPlaneCheckpointError> {
        let mut candidate = self.membership_candidate();
        if let Err(reason) = self.fold_membership_events(&mut candidate, events) {
            self.record_contradiction(reason);
            return Err(reason);
        }
        self.install_membership(candidate);
        Ok(())
    }

    /// Folds one batch of membership events into a candidate, keeping the first
    /// refusal.
    ///
    /// Shared by the live transaction and construction's, so a batch asserts the
    /// same facts whichever one is open. It installs nothing and publishes
    /// nothing: what a surviving candidate is *for* is the caller's decision, and
    /// that is the only difference between the two.
    ///
    /// # Errors
    ///
    /// As [`MembershipCandidate::apply`]. The candidate is left holding the
    /// prefix that folded, which is why every caller drops it on a refusal rather
    /// than installing it.
    fn fold_membership_events(
        &self,
        candidate: &mut MembershipCandidate,
        events: &[MembershipEvent<G>],
    ) -> Result<(), ControlPlaneCheckpointError> {
        for event in events {
            let fact = match observed_membership(event) {
                ObservedMembership::Effective(effective) => MembershipFact::Effective(effective),
                // The runtime is the authority on what is in effect, and it
                // agrees with the effective event that preceded this one in the
                // same report. A driver holding no group keeps what the candidate
                // had rather than assigning an empty set: an absent effective
                // membership must not turn a retirement into a silence, and must
                // not narrow anything either.
                ObservedMembership::Committed(committed) => MembershipFact::Committed {
                    committed,
                    effective: self
                        .runtime_effective_members()
                        .unwrap_or_else(|| candidate.effective_members.clone()),
                },
                ObservedMembership::Nothing => continue,
            };
            candidate.apply(fact)?;
        }
        Ok(())
    }

    /// Records a contradiction so a supervisor polling
    /// [`TransportDriverState::service_state`] sees it.
    ///
    /// Only the two terminal shapes are recorded, and
    /// [`Contradiction::of`] is where that line is drawn. The rest are refusals
    /// of an *input* — a damaged record, a foreign group, a record older than
    /// what this driver holds — and every one of them is raised where a caller
    /// can still be told, so recording them here would report a driver as sick
    /// for a file it declined to open.
    ///
    /// **The first one wins**, because the state is terminal and there is nothing
    /// a second could add: the driver is already frozen and already publishing
    /// nothing, and overwriting the position would move the marker off the
    /// disagreement an operator is being pointed at.
    ///
    /// **Setting it moves the checkpoint epoch**, because the marker is a
    /// checkpointable field — see
    /// [`PeerControlPlaneCheckpoint::contradicted_at`]. It is also the *only*
    /// checkpointable change a contradiction makes: the batch that produced it
    /// installed nothing, and every later batch is frozen out, so an embedder
    /// that persists on the epoch would otherwise never write the one record that
    /// says this replica must not serve again.
    pub(super) fn record_contradiction(&mut self, reason: ControlPlaneCheckpointError) {
        let Some(contradiction) = Contradiction::of(reason) else {
            return;
        };
        if self.contradiction.is_some() {
            return;
        }
        self.contradiction = Some(contradiction);
        self.advance_checkpoint_epoch();
    }

    /// The contradiction this driver recorded while routing, if it recorded one.
    ///
    /// **The only way a refusal escapes the routing path.**
    /// [`TransportDriverState::route_membership_events`] has nowhere to return
    /// one — it runs from every step outcome, including a failing one — so it
    /// records instead, and the two entry points that *can* refuse ask here once
    /// their recovery outputs have been routed. A driver that opened over a
    /// durable record its own replayed history contradicts would be serving from
    /// inputs it has already declared untrustworthy.
    pub(super) fn recorded_contradiction(&self) -> Option<ControlPlaneCheckpointError> {
        self.contradiction.map(Contradiction::refusal)
    }

    /// Reads the group's effective membership, or `None` if it holds no group.
    fn runtime_effective_members(&self) -> Option<BTreeSet<NodeId>> {
        self.group.as_ref().map(|group| {
            group
                .runtime()
                .membership()
                .replica_ids()
                .into_iter()
                .collect()
        })
    }
}
