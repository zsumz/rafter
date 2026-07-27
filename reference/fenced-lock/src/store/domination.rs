//! What a state has to dominate before it may be published or adopted.
//!
//! Three comparisons, all of them refusals rather than repairs, and all of them
//! run before a byte moves: every publication must dominate the fencing
//! high-water marks this store has already acknowledged, every republication at
//! an unchanged applied index must dominate the durable session cache, and
//! every image a recovery discards or sets aside must be dominated by the image
//! adopted in its place. The `# Mark durability` and `# Repairing, as a
//! separate act` sections of the [module documentation](super) are the argument
//! for each.

use std::{collections::BTreeMap, fmt};

use crate::{ClientId, FencingToken, LockService, ResourceName, Sequence, SessionEpoch};

use super::{damage::SlotDamage, error::LockStoreError, format::SlotIndex, image::DecodedImage};

// Imported for the intra-doc links the prose below carries.
#[allow(unused_imports)]
use super::{report::Repair, LockStore};

/// How far one client slot's session had progressed when it was made durable.
///
/// This is the key the session cache is ordered by, and it is deliberately not
/// the applied Raft index: an install may republish the index the store already
/// holds, and at that index the index itself says nothing about which requests
/// have completed.
///
/// Ordering is lexicographic — the epoch first, then the highest completed
/// sequence under it — because opening a newer epoch is exactly what
/// legitimately clears an older epoch's cache. A slot on a later epoch has not
/// lost anything by holding no completion yet.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionProgress {
    /// Session generation the slot was on.
    pub epoch: SessionEpoch,
    /// Highest completed sequence cached under that epoch, if any.
    pub completed: Option<Sequence>,
}

impl fmt::Display for SessionProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.completed {
            Some(sequence) => write!(
                formatter,
                "epoch {} through sequence {}",
                self.epoch.get(),
                sequence.get()
            ),
            None => write!(
                formatter,
                "epoch {} with nothing completed",
                self.epoch.get()
            ),
        }
    }
}

/// Returns every tracked resource's fencing high-water mark.
pub(super) fn marks_of(service: &LockService) -> BTreeMap<ResourceName, FencingToken> {
    service
        .view()
        .resources
        .into_iter()
        .map(|resource| (resource.resource, resource.token_floor))
        .collect()
}

/// Returns every client slot's session progress.
pub(super) fn session_progress_of(service: &LockService) -> BTreeMap<ClientId, SessionProgress> {
    service
        .view()
        .sessions
        .into_iter()
        .map(|session| {
            (
                session.client_id,
                SessionProgress {
                    epoch: session.session_epoch,
                    completed: session.cached.map(|(sequence, _, _)| sequence),
                },
            )
        })
        .collect()
}

/// Refuses a state that would move any client slot's session cache backwards.
///
/// A slot that disappears is the same failure as one whose progress decreases:
/// both let an acknowledged operation execute a second time, and for an
/// acquisition that is a second fencing token for one tenure.
pub(super) fn verify_session_cache_dominates(
    acknowledged: &BTreeMap<ClientId, SessionProgress>,
    offered: &BTreeMap<ClientId, SessionProgress>,
) -> Result<(), LockStoreError> {
    for (client, progress) in acknowledged {
        let found = offered.get(client).copied();
        if found.is_none_or(|offered_progress| offered_progress < *progress) {
            return Err(LockStoreError::SessionCacheRegression {
                client: *client,
                acknowledged: *progress,
                offered: found,
            });
        }
    }
    Ok(())
}

/// Refuses to drop or set aside an image whose marks the adopted one does not
/// dominate, and says whether it was able to check.
///
/// Returns `true` when the comparison ran. `false` means the discarded slot held
/// nothing this build could decode, which is the case the old sentence on
/// [`Repair`] claimed for all of them: reading that slot is exactly what failed,
/// so nobody can say what was in it. That sentence was true of every damage
/// except the one that matters. [`SlotDamage::UnsealedCompleteImage`] is a whole
/// image that verified under the restored mark — [`classify_unsealed`] decoded
/// it far enough to report a generation — so "nobody can say" was a claim about
/// the mechanism's scope made one step wider than the mechanism reached.
///
/// # Is a repair that must discard a higher-marked image ever legitimate?
///
/// No, and this function is that answer.
///
/// [`SlotDamage::UnsealedCompleteImage`] has two readings, and they differ in
/// exactly one observable: whether the discarded image's fencing marks were ever
/// acknowledged to a client. Under the interrupted-publication reading no
/// response was returned for the entry that image holds, because a response
/// follows the commit point, so its marks reached nobody. Under the rotted-mark
/// reading they reached a client, and a guarded resource downstream has accepted
/// a token at least that high.
///
/// When the adopted image **dominates** those marks, the two readings agree on
/// every mark a caller could hold. Adopting is then correct under both readings
/// rather than a choice between them, no decision is required, and the repair is
/// safe. That is the same asymmetry the set-aside branch above already rests on,
/// applied one level up.
///
/// When it does not, the readings disagree about the one fact a fencing lock
/// exists to protect. Nothing in the bytes decides which holds, and neither can
/// the caller: the deciding evidence is not in this store, it is in the guarded
/// downstream, which this store cannot read. An entry point that proceeded on
/// the strength of having been called by name would be taking consent in place
/// of information, and would perform this design's worst failure — two
/// independent tenures under one token — on request, with a report.
///
/// So it refuses, in **both** entry points, and names the resource and both
/// marks rather than the generations, because the marks are the loss and the
/// generations are not. There is deliberately no override, because an override
/// is the boolean this paragraph rejects.
///
/// ## What that refusal costs, and where the store it wedges goes
///
/// The cost is not small and is not hedged here: an ordinary crash during an
/// **acquisition** produces this every time — the interrupted image is the
/// newer one and it is the first to hold the mark it raised, so no partner can
/// dominate it — and acquisition is the operation a fencing lock exists to
/// perform. Both entry points then refuse, neither moves a byte, and the
/// directory has no entry point that reads it.
///
/// That state used to be the end of the road, under a sentence saying a replica
/// which cannot prove its marks "is re-seeded from the group". That was a claim
/// about the cluster with nothing behind it: no call in this crate would open
/// the directory, so the way forward began with deleting files by hand.
/// [`LockStore::discard_and_reseed`] is that way forward as a named entry point
/// with its own argument, its own report, and its own tests, and the reason
/// discarding is sound rather than merely available is on it.
///
/// The asymmetry the whole rule rests on stays what it was. This replica's
/// store is a projection of a committed log and the log can rebuild it. A
/// fencing token that has left the cluster is not in any log, and nothing
/// rebuilds the guarded resource that accepted it.
///
/// ## What this does not check, and why
///
/// The **session cache** is deliberately not required to dominate here, and the
/// asymmetry is not an oversight. Session progress advances on every applied
/// entry, so a discarded image almost always holds more of it than the adopted
/// one; requiring domination would refuse every repair and leave the ordinary
/// crash with no way forward at all, which is the state the repair entry point
/// was added to end. What makes the two different is where the loss can be
/// recovered from: a session-cache regression is bounded by the applied index
/// this store reports, and replaying from there re-executes the same entries
/// through the same deterministic state machine, so a client's retry meets its
/// cached result again. A fencing token has already left the cluster. No replay
/// reaches the guarded resource that accepted it.
///
/// "Replaying from there" is the load-bearing half, and it is a claim about the
/// composition rather than about this file — so it is checked in the
/// composition. `a_reseeded_replica_recovers_its_marks_from_the_group` empties
/// a replica's store outright, which discards strictly more session cache than
/// any repair can, and the three-node driver re-applies it back. What is left
/// unchecked is only what that test also cannot show: that the group still
/// holds the entries. It does whenever they committed, and it does not if a
/// quorum lost them together.
///
/// The two sides of the rule itself are pinned by
/// `a_repair_that_regresses_only_session_progress_is_allowed_and_reported` and
/// `a_repair_that_would_regress_a_mark_is_refused_by_both_entry_points`.
pub(super) fn verify_discard_preserves_marks(
    discarded: SlotIndex,
    damage: SlotDamage,
    discarded_image: Option<&DecodedImage>,
    adopted: SlotIndex,
    adopted_image: Option<&DecodedImage>,
) -> Result<bool, LockStoreError> {
    let (Some(discarded_image), Some(adopted_image)) = (discarded_image, adopted_image) else {
        return Ok(false);
    };
    let acknowledged = marks_of(&discarded_image.service);
    let offered = marks_of(&adopted_image.service);
    for (resource, mark) in &acknowledged {
        let found = offered.get(resource).copied();
        if found.is_none_or(|offered_mark| offered_mark < *mark) {
            return Err(LockStoreError::DiscardWouldRegressMark {
                slot: discarded,
                damage,
                adopted,
                resource: *resource,
                acknowledged: *mark,
                offered: found,
            });
        }
    }
    Ok(true)
}

/// Refuses a state that would lower or drop any acknowledged mark.
///
/// A resource that disappears is the same failure as one whose mark decreases:
/// both let a later acquisition reissue a token a guarded resource has accepted.
pub(super) fn verify_marks_dominate(
    acknowledged: &BTreeMap<ResourceName, FencingToken>,
    offered: &BTreeMap<ResourceName, FencingToken>,
) -> Result<(), LockStoreError> {
    for (resource, mark) in acknowledged {
        let found = offered.get(resource).copied();
        if found.is_none_or(|offered_mark| offered_mark < *mark) {
            return Err(LockStoreError::MarkRegression {
                resource: *resource,
                acknowledged: *mark,
                offered: found,
            });
        }
    }
    Ok(())
}
