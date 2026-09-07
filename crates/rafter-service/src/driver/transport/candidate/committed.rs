//! Taking one committed membership fact into a candidate.
//!
//! The only place identity is *consumed*: which IDs a removal spends, which fact
//! is refused for contradicting the register, and how far allocation has got.
//! Every operation here is monotone evidence, so re-folding a fact a restart
//! replays changes nothing and there is no cursor left to keep.

#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

use std::collections::BTreeSet;

use super::super::checkpoint::{live, merge_current_state, IncomingObservation};
use super::super::observation::CommittedObservation;
use super::super::*;
use super::MembershipCandidate;

impl MembershipCandidate {
    /// Takes one committed membership fact: the removals it proves, the
    /// high-water mark, and the current state the spent test reads.
    ///
    /// The only place identity is *consumed*, and everything about consumption is
    /// here: which IDs a removal spends, which a violating fact is refused for,
    /// and how far allocation has got.
    ///
    /// # Why this needs no cursor
    ///
    /// A restart hands this the same facts twice: the runtime replays every
    /// configuration entry above the application's applied floor, oldest first,
    /// beneath a current state that has already moved past them. What made that
    /// dangerous was deriving removals by subtraction from the driver's own
    /// membership, which is right only when that membership stands exactly where
    /// the fact does. Two cursors were kept so a fact standing anywhere else
    /// could be skipped.
    ///
    /// Every operation below is monotone evidence instead, so re-folding one
    /// changes nothing and there is nothing left to skip:
    ///
    /// * **Removals come from the fact.** A crossing carries its own transition,
    ///   computed by the kernel where the chronology is known, so
    ///   `previous − configuration` is the same set at every replay. An endpoint
    ///   carries none and asserts none.
    /// * **The mark is a maximum**, taken over both ends of the fact.
    /// * **The current state is a versioned register.** An older observation
    ///   never displaces a later one; it only contributes what the pair proves,
    ///   which is the identities it named that the later one does not.
    ///
    /// # The removal a later register still names
    ///
    /// A crossing beneath the register proves a removal whose ID the register
    /// may still name, and `spent(id) = id ≤ mark ∧ id ∉ membership` cannot see
    /// it while the membership does. That gap is closed here without a holding
    /// set, and the argument is short:
    ///
    /// 1. A fact that proves `id` removed named `id`, so it raised the mark to
    ///    at least `id`.
    /// 2. `id` is subtracted from the register's membership *whatever the fact's
    ///    position* — a removal is not an observation of the present, it is a
    ///    permanent fact about an identity, so the later-wins rule does not
    ///    apply to it.
    /// 3. So `id ≤ mark ∧ id ∉ membership` holds the moment the fold returns.
    /// 4. Every later assignment to the register filters its incoming membership
    ///    through the spent test, so `id` can never re-enter.
    ///
    /// A contract-violating configuration that names `id` again is therefore
    /// refused at step 4 rather than parked in a set that has to be bounded, and
    /// the violation stays countable through the raw floor beside the register —
    /// see
    /// [`TransportDriverState::readmitted_retired_peers`](super::super::state::TransportDriverState::readmitted_retired_peers).
    ///
    /// # Errors
    ///
    /// Returns [`ControlPlaneCheckpointError::ContradictoryTransitionPredecessor`]
    /// when this fact is a transition standing immediately above the register and
    /// declares a predecessor the register is not, and
    /// [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when this fact
    /// and the register stand at one position and still disagree about the
    /// membership there after normalization. **Nothing is mutated on either
    /// path**: the ancestry check runs first and every value below it is computed
    /// into a local and assigned only once the merge has answered.
    pub(super) fn observe_committed(
        &mut self,
        fact: CommittedObservation,
    ) -> Result<(), ControlPlaneCheckpointError> {
        self.check_ancestry(&fact)?;
        let was_spent = self.spent_before();
        let current = merge_current_state(
            self.current_committed.as_ref(),
            &IncomingObservation {
                through: fact.through,
                membership: &fact.membership,
                proven_removed: &fact.removed,
            },
            &was_spent,
        )?;

        // Over every identity the fact named rather than the survivors: an ID
        // the cluster committed is allocated whether or not it survived the
        // transition, and a mark that ignored a removed one would leave it
        // allocatable again.
        if let Some(highest) = fact.named.iter().copied().max() {
            self.committed_id_high_water = Some(
                self.committed_id_high_water
                    .map_or(highest, |mark| mark.max(highest)),
            );
        }
        self.current_committed = Some(current);
        // **The raw floor is not part of the register, and assigning it always
        // is round 8's rule kept rather than re-litigated.** It answers "what
        // does this replica's own stream say the cluster has committed", which
        // no position has an opinion about, and tying it to a gate left it stale
        // in two lifecycle cells — a second recovery from one checkpoint, and a
        // supervisor handing a record to a runtime that rebuilt behind it. The
        // only facts that can leave it historical are recovery outputs, and both
        // entry points publish the runtime's endpoint after them.
        //
        // Raw rather than filtered, which is what makes a readmission countable.
        self.committed_members = fact.membership;
        Ok(())
    }

    /// Refuses a transition whose declared predecessor this candidate is not.
    ///
    /// **The one-chain contract, made executable.** Every crossing carries the
    /// membership the kernel computed as standing immediately before its own
    /// entry — see [`rafter::Output::ConfigurationCommitted`] — and a register
    /// standing exactly one position below it is a claim about that same
    /// committed configuration. Two claims about one committed membership that
    /// still differ are not two readings to reconcile; they are proof that the
    /// record and the log are not one chain, which is the strongest evidence of a
    /// fork this driver can hold and was previously discarded on the way in.
    ///
    /// Discarding it was not merely a lost diagnosis. Folded anyway, the merge
    /// reads the register-minus-transition difference as a committed removal *and*
    /// absorbs the transition's own removal set, so a single contradictory pair
    /// retires two identities at once — one of them named by neither side's
    /// removal — and a retirement floor never falls.
    ///
    /// **Adjacency is required and its absence is not a weaker check, it is no
    /// check at all.** See
    /// [`CommittedObservation::membership_claimed_at`]: a transition that does
    /// not stand immediately above the register makes no claim about where the
    /// register stands, because the entries between them may be application
    /// entries — across which the committed membership does not move — or
    /// configuration entries this driver never saw. Comparing across a gap would
    /// manufacture the very contradiction the check exists to detect.
    ///
    /// Both sides are normalized by what this candidate has already proven spent,
    /// for the reason [`merge_current_state`] gives: a cluster that names a
    /// retired identity again has broken the single-use contract, which is a
    /// counted violation with an answer of its own rather than a damaged record.
    /// The register's own membership is already the live reading, so the
    /// normalization only ever moves the incoming side.
    ///
    /// # Errors
    ///
    /// Returns
    /// [`ControlPlaneCheckpointError::ContradictoryTransitionPredecessor`], naming
    /// the position whose committed membership the two disagree about — the
    /// register's own, not the transition's.
    pub(super) fn check_ancestry(
        &self,
        fact: &CommittedObservation,
    ) -> Result<(), ControlPlaneCheckpointError> {
        let Some(held) = self.current_committed.as_ref() else {
            return Ok(());
        };
        let Some(claimed) = fact.membership_claimed_at(held.through) else {
            return Ok(());
        };
        let was_spent = self.spent_before();
        let nothing = BTreeSet::new();
        if live(&held.membership, &nothing, &was_spent) != live(claimed, &nothing, &was_spent) {
            return Err(
                ControlPlaneCheckpointError::ContradictoryTransitionPredecessor {
                    through: held.through,
                },
            );
        }
        Ok(())
    }
}
