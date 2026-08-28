#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! How two observations of the committed membership combine.
//!
//! One merge reached from four directions — two records joining, a record
//! meeting an adopted runtime's endpoint, a held register meeting a routed
//! endpoint, and one meeting a crossing — because they were four expressions of
//! one rule and only one of them refused a tie. The later observation wins, a
//! proven removal is absorbed wherever it stands, and a tie that survives
//! normalization is refused rather than broken.

use std::cmp::Ordering;
use std::collections::BTreeSet;

use super::checkpoint::CurrentCommittedState;
use super::*;

/// Which of the two entry points is offering a record, and therefore what a
/// contradiction marker on it means.
///
/// **The line is whether the record is this chain's own or somebody else's**, and
/// it falls exactly where the chain rule already put it.
/// [`TransportRaftDriver::with_control_plane_checkpoint`] restores into empty
/// held state and is the documented crash-recovery path, so the record it is
/// handed *is* the chain: a marker on it has to be carried, or the terminal state
/// ends at the next crash and the fork disappears.
/// [`TransportRaftDriver::adopt_group_with_checkpoint`] merges a record into a
/// driver that has been running, and a marked record is a statement that its
/// chain observed a fork nothing can resolve — merging it into a live driver is
/// licensing what it refused, and the mark and register a fork produced are not
/// evidence anyone may join.
pub(super) enum RecordJoin {
    /// A constructor resuming this replica's own chain into empty held state.
    Resume,
    /// An adoption merging a record into a driver that has been running.
    Merge,
}

/// Everything one merge needs to know about the observation arriving.
///
/// A position, the membership observed there **raw**, and what the fact itself
/// proves a committed removal consumed. The third field is what separates a
/// crossing from every other input: only a transition the kernel computed proves
/// a removal on its own, and it proves it wherever the fact is folded.
pub(super) struct IncomingObservation<'a> {
    pub(super) through: LogIndex,
    pub(super) membership: &'a BTreeSet<NodeId>,
    pub(super) proven_removed: &'a BTreeSet<NodeId>,
}

/// Merges one observation of the committed membership into the one held.
///
/// **The single merge, reached from four directions.** Two checkpoints joining;
/// a checkpoint meeting the adopted runtime's own endpoint; a held state meeting
/// a routed `CommittedEndpoint`; a held state meeting a crossing that advances
/// the register. They were four expressions of one rule and only the first
/// refused a tie, so a runtime that disagreed with a durable record at the very
/// position both had observed silently retired a live replica in one direction
/// and silently authorized a never-committed one in the other.
///
/// # The rule
///
/// * **The later observation wins**, and the earlier is not discarded: the
///   identities it named that the later one does not are the committed removals
///   that happened between the two positions. That is the inference neither side
///   can make alone.
/// * **A proven removal is absorbed whatever the fact's position**, because it is
///   not an observation of the present — it is a permanent fact about an
///   identity. A crossing beneath the register still takes its removal out of it.
/// * **A tie is refused rather than broken**, once normalization has had its say.
///   The committed membership at one log position is one set, so two claims about
///   it that still differ are not two readings to reconcile; picking either
///   would be choosing which side to believe with nothing to decide on, and
///   merging them would invent a third neither side ever held.
///
/// # What normalization is for, and what it deliberately is not
///
/// **Spent-ness is normalized away before the tie is judged, and the incoming
/// fact's own removals are not.**
///
/// `spent` is what keeps a **readmission** from reading as corruption. A cluster
/// that names an already-spent identity again has broken the single-use contract,
/// and this driver has an answer for that — refuse the replica, count the
/// violation at `readmitted_retired_peers`. The raw membership a runtime reports
/// contains the readmitted identity and the held register does not, which is a
/// difference with a known cause, so it must not be reported as a damaged file.
/// It is applied to both sides rather than only to the incoming one, which is
/// what keeps the merge symmetric — and symmetry is a property the checkpoint
/// join needs, since a supervisor has no correct order to read two peers' records
/// in.
///
/// `proven_removed` is not, and the reason is that it cannot discriminate. A
/// crossing at position `p` carries the committed configuration *at* `p`, and its
/// removal set is `previous ∖ configuration` — disjoint from that configuration
/// by construction. Any honest observation at `p` reports the same configuration,
/// so the removal set is disjoint from the held membership too, and subtracting
/// it from both sides changes neither. The only pair it can change is one where
/// the held state names an identity the transition at that very index removed —
/// a record disagreeing with the kernel's own account of its own index, which is
/// exactly what this arm exists to refuse. A normalization that fires only on the
/// case the check exists to catch is a hole with a justification attached.
///
/// The removals themselves are still absorbed: they raise the mark through the
/// fact's `named` set, and an identity at or below the mark that the membership
/// does not name is spent. Nothing about a tie loses them.
///
/// # Errors
///
/// [`ControlPlaneCheckpointError::ContradictoryCurrentState`] when the two stand
/// at one position and still disagree about the membership there.
pub(super) fn merge_current_state(
    held: Option<&CurrentCommittedState>,
    incoming: &IncomingObservation<'_>,
    spent: &dyn Fn(NodeId) -> bool,
) -> Result<CurrentCommittedState, ControlPlaneCheckpointError> {
    let Some(held) = held else {
        return Ok(CurrentCommittedState::new(
            incoming.through,
            live(incoming.membership, incoming.proven_removed, spent),
        ));
    };
    let (older, newer) = match held.through.cmp(&incoming.through) {
        Ordering::Less => (&held.membership, incoming.membership),
        Ordering::Greater => (incoming.membership, &held.membership),
        Ordering::Equal => {
            let nothing = BTreeSet::new();
            let held_live = live(&held.membership, &nothing, spent);
            let incoming_live = live(incoming.membership, &nothing, spent);
            if held_live != incoming_live {
                return Err(ControlPlaneCheckpointError::ContradictoryCurrentState {
                    through: held.through,
                });
            }
            return Ok(CurrentCommittedState::new(held.through, held_live));
        }
    };
    let mut removed: BTreeSet<NodeId> = older.difference(newer).copied().collect();
    removed.extend(incoming.proven_removed.iter().copied());
    Ok(CurrentCommittedState::new(
        held.through.max(incoming.through),
        live(newer, &removed, spent),
    ))
}

/// One membership less everything a removal took and everything already spent.
pub(super) fn live(
    membership: &BTreeSet<NodeId>,
    removed: &BTreeSet<NodeId>,
    spent: &dyn Fn(NodeId) -> bool,
) -> BTreeSet<NodeId> {
    membership
        .iter()
        .copied()
        .filter(|node_id| !removed.contains(node_id) && !spent(*node_id))
        .collect()
}
