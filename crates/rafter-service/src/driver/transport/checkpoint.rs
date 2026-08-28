#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! The peer-control-plane record a restarted process reads back, and the one
//! merge every pair of observations goes through.
//!
//! Split from [`super::control_plane`] along the line between a *record* and a
//! *derivation*. That file answers "who is allowed to send a step" — the
//! membership facts, the retirement floor, the policy the link layer is handed.
//! This one answers "what does a process that crashed get back, and what may it
//! conclude from it": the type, what makes one valid, and how two observations of
//! the committed membership combine.
//!
//! **There is no consumer offset here, and there used to be two.** A record kept
//! a position in the committed configuration stream so a replayed history could
//! be skipped as already-folded, because folding a historical membership against
//! a present one reads as a removal of everything the present added. That is a
//! true hazard and a cursor is the wrong answer to it: a position is a claim
//! about a *prefix*, and nothing a driver observes is a prefix — a
//! snapshot-recovered process that then folds a crossing at index 8 has consumed
//! neither 6 nor 7, and a cursor reading 8 says otherwise. The facts are
//! monotone evidence now, so re-folding one changes nothing and there is nothing
//! to skip. See [`super::control_plane`] for the algebra that makes that true.
//!
//! **There is no obligation ledger here either, and there used to be one.** A
//! record carried the committed removals whose per-principal fence the link layer
//! had not accepted, because `fence_peer` was an operation that could be refused
//! and no later event re-derived it. Retirement is published as a *floor* now —
//! see [`crate::transport::PeerPolicy`] — so every statement this driver makes to
//! its link layer is a function of state it still holds, and a refused
//! publication is retried rather than remembered. That deleted the one output of
//! the join that was not monotone, which is what restored order-freedom by
//! construction rather than by proof.
//!
//! The state these rules read still lives on
//! [`super::state::TransportDriverState`], like every other field behind the one
//! lock. What lives here is the record's own algebra.

use std::collections::BTreeSet;

use super::super::*;
use super::reconciliation::MembershipCandidate;

mod record;
mod validation;

pub use record::{CurrentCommittedState, PeerControlPlaneCheckpoint};

pub(super) use super::durable::spends_under;
pub(super) use super::merge::{live, merge_current_state, IncomingObservation, RecordJoin};

/// Joins a recovered checkpoint into a staged candidate.
///
/// **One authoritative chain per replica, and that is the narrowed contract.** A
/// driver's records form a chain: each incarnation is handed the previous one's
/// record before it observes anything, so every record it goes on to write
/// already carries everything the earlier ones spent. Records from *other*
/// sources — two peers' files, or another process's record joined into a driver
/// that has been running — are not one chain, and merging them arbitrarily is
/// withdrawn from the contract. [`super::reconciliation`] names the two callers.
///
/// The rule that enforces it is positional and is the first thing the body does:
/// **a record standing before the candidate's own observation is refused**, with
/// [`ControlPlaneCheckpointError::StaleCurrentState`]. Equal positions keep the
/// contradiction arm; later ones merge.
///
/// # Why the position, and what refusing one costs
///
/// The register keeps *one* observation, so the equal-position contradiction
/// check can only fire while the register still stands where the incoming record
/// does. Once any later record moves it forward, two records that directly
/// contradict each other are never compared — and the eleventh reviewer's
/// counterexample is exactly that. `A {through 1, mark 2, {1,2}}` and
/// `B {through 1, mark 3, {1,3}}` collide at position 1; `C {through 2, mark 3,
/// {1,2,3}}` moves the register past both. `(A∨B)∨C` refuses at the collision,
/// while `A∨(B∨C)` never compares A against B at all and settles on a record that
/// permanently retires a replica the latest record calls live.
///
/// Detecting that after the fact would need per-position history, which is the
/// unbounded structure this whole design exists without. Refusing the input
/// instead costs one real capability, and it is worth naming rather than
/// glossing: an older record can carry a mark a *newer* one lacks — a
/// snapshot-derived record observes the boundary configuration and nothing
/// beneath it — so a stale record joined mid-life used to contribute retirement
/// the driver had no other source for. That capability is not deleted, it moves.
/// [`TransportRaftDriver::with_control_plane_checkpoint`] restores into empty
/// held state, which is the documented crash-recovery path and the one a
/// supervisor holding another process's record must use.
///
/// # The lattice join, and where that matters
///
/// **A lattice join, not three independent merges, and the current state is
/// where that matters.** Taking the union of the two memberships was wrong twice
/// over, in the two directions this mechanism exists to prevent.
///
/// It un-spent a witnessed removal: a stale-but-valid record holding
/// `{mark 5, live {1,2,5}}` joined into a driver holding `{mark 5, live {1,2}}`
/// produced live `{1,2,5}`, and the identity the cluster consumed became
/// adoptable again. And it dropped a removal *neither* record witnessed but the
/// two jointly prove: an older `{through 7, mark 5, live {1,2,3,5}}` beside a
/// later snapshot-derived `{through 10, mark 3, live {1,2,3}}` says node 5 was in
/// the committed membership at position 7 and is not in it at position 10, which
/// is a committed removal — while the union kept 5 live, unspent, and published
/// to the link layer. The missing fact was not in either record; it was *between*
/// them, and only a positioned current state can see it.
///
/// Write `spent_x(n) = n ≤ mark_x ∧ n ∉ live_x`, and let `held` be the
/// candidate's state and `incoming` the record's, with `incoming` at or after
/// `held` by the rule above. Then:
///
/// ```text
/// mark     = max(mark_held, mark_incoming)
/// spent    = S_held ∪ S_incoming
/// inferred = held.membership \ incoming.membership
/// current  = { through: incoming.through,
///              membership: incoming.membership \ inferred \ spent }
/// ```
///
/// An identity *above* one side's mark is judged only by the side whose mark
/// covers it, which is what lets a record that never saw an identity avoid
/// overruling one that did. The inferred set is the exception and earns it: it is
/// judged by both, because being named at one position and absent at a later one
/// is a two-record fact.
///
/// **Every output is a lattice operation.** There used to be a fifth line — the
/// fence obligations, which were the union of both sides *plus the inferred
/// removals neither side had already spent*. That last clause read the spent set
/// to decide a side effect, which made the effect depend on how much spent-ness
/// had accumulated by the time the inference fired, which made it depend on the
/// order. Publishing retirement as a floor deletes the line rather than repairing
/// it — see [`crate::transport::PeerPolicy`] — and what is left is `max`, `∪`,
/// `\` and "the later of the two", every one of which is order-free.
///
/// # What order-freedom now claims, and what it does not
///
/// **Records of one chain, and nothing wider.** Along a chain every operation
/// above is symmetric in the pair, associative and idempotent, so which prefix of
/// its own chain a supervisor happens to have persisted does not change where a
/// driver settles — and that is the property an embedder actually depends on,
/// because a crash can lose any suffix of its own writes.
///
/// Across a *fork* it is false, and the counterexample above is the proof. The
/// claim used to be stated over arbitrary records and was only ever checked
/// against mutually reconcilable ones. The chain rule is what makes the wider
/// claim unnecessary rather than merely unproven: a fork refuses instead of
/// settling, and the supervisor owns chain identity.
///
/// **Monotone in spent-ness, and that survives the narrowing.** Every identity
/// either side had witnessed spent is spent in the join and in every later join:
/// `n ≤ mark_x ≤ mark_join`, and `n` is filtered out of the joined membership by
/// construction. A witnessed removal cannot be undone by any accepted join, and
/// an inferred one cannot either — it leaves the joined membership, and the
/// filter keeps a later observation from putting it back.
///
/// **The joined candidate is validated too, and that is deliberate redundancy.**
/// The argument above says it cannot fail: the coupling biconditional is
/// preserved because the mark is a `max` and the current state is a choice
/// between two, so the join has a mark exactly when a side did and a current
/// state exactly when a side did; and every live identity stays at or below the
/// joined mark because the mark only rose. The properties are nonetheless
/// *executed* rather than only argued, because the cost is one pass over a
/// cluster-sized set and the thing being protected is a retirement floor that
/// never falls. A proof that stops holding because someone edited the join is a
/// proof that fails silently; this one fails loudly.
///
/// # The marker a record can carry, and which caller may take it
///
/// A record whose `contradicted_at` is set says its chain observed a fork nothing
/// resolves. `RecordJoin::Resume` returns the position so the constructor can
/// re-enter the terminal state; `RecordJoin::Merge` refuses the record outright,
/// for the reason [`RecordJoin`] gives. Refusing is not a loss of the marker:
/// the record is still on the embedder's disk, and the supported way to read one
/// back is the constructor.
///
/// # Errors
///
/// Returns [`ControlPlaneCheckpointError`] when the checkpoint names another
/// group, contradicts the invariants a driver maintains for one, carries a
/// contradiction marker into an adoption, stands before the candidate's own
/// observation, contradicts it at a shared position, or when the joined result
/// would contradict those invariants. **Nothing is mutated on any of those
/// paths** — the join is computed into locals and validated before the first
/// field moves — so a caller that refuses is left with a candidate it drops and a
/// driver in exactly the state it was.
pub(super) fn restore_checkpoint<G>(
    candidate: &mut MembershipCandidate,
    checkpoint: PeerControlPlaneCheckpoint<G>,
    group_id: &G,
    join: RecordJoin,
) -> Result<Option<LogIndex>, ControlPlaneCheckpointError>
where
    G: Ord,
{
    checkpoint.validate(group_id)?;
    if let (Some(through), RecordJoin::Merge) = (checkpoint.contradicted_at, join) {
        return Err(ControlPlaneCheckpointError::ContradictedRecordMerged { through });
    }

    // Taken apart rather than read through, so the restored observation is moved
    // into the candidate rather than cloned into it. The two halves are what
    // spent-ness is computed from either way.
    let PeerControlPlaneCheckpoint {
        committed_id_high_water: restored_mark,
        current_committed: restored_state,
        contradicted_at: restored_contradiction,
        ..
    } = checkpoint;

    // **The chain rule, before either side is read for anything else.** A record
    // that observed the committed membership before this candidate did is not a
    // later record of this candidate's chain, and merging one is the laundering
    // vector above. A record with no observation at all carries no position and
    // is not judged here — `adopt_group` passes exactly that, and it has to stay
    // a no-op.
    if let (Some(held), Some(incoming)) = (
        candidate.current_committed.as_ref(),
        restored_state.as_ref(),
    ) {
        if incoming.through < held.through {
            return Err(ControlPlaneCheckpointError::StaleCurrentState {
                held: held.through,
                incoming: incoming.through,
            });
        }
    }

    let held_mark = candidate.committed_id_high_water;
    let held_state = candidate.current_committed.clone();
    let committed_id_high_water = match (held_mark, restored_mark) {
        (Some(held), Some(restored)) => Some(held.max(restored)),
        (held, None) => held,
        (None, restored) => restored,
    };
    let spent = |node_id: NodeId| {
        spends_under(held_mark, held_state.as_ref(), node_id)
            || spends_under(restored_mark, restored_state.as_ref(), node_id)
    };
    // A record carries no transition, so it proves no removal on its own. What
    // the *pair* proves — an identity named at one position and absent at a later
    // one — is the merge's own inference.
    let proves_nothing = BTreeSet::new();
    let current_committed = match restored_state.as_ref() {
        // The incoming record observed nothing, so it can raise no mark either
        // (the biconditional above), and there is nothing to merge.
        None => held_state.clone(),
        Some(incoming) => Some(merge_current_state(
            held_state.as_ref(),
            &IncomingObservation {
                through: incoming.through,
                membership: &incoming.membership,
                proven_removed: &proves_nothing,
            },
            &spent,
        )?),
    };

    let joined = PeerControlPlaneCheckpoint {
        group: group_id,
        committed_id_high_water,
        current_committed,
        contradicted_at: restored_contradiction,
    };
    joined.validate(&group_id)?;

    // The raw committed floor is deliberately not restored from here. It answers
    // "what does this replica's own stream say the cluster has committed now", a
    // record says what this driver has *spent*, and both entry points that reach
    // this call publish the runtime's endpoint afterwards — so the floor is
    // assigned from the runtime before the driver serves anything.
    candidate.committed_id_high_water = joined.committed_id_high_water;
    candidate.current_committed = joined.current_committed;
    // The marker travels back to the caller rather than onto the candidate. A
    // candidate is the four membership fields and nothing else — it is dropped
    // whole on a refusal — while the marker is a fact about the *driver*, and it
    // has to be recorded before the replay routes anything or the freeze it
    // licenses starts one batch too late.
    Ok(joined.contradicted_at)
}
