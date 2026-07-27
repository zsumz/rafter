//! Picking the slot recovery adopts.
//!
//! One function, and the only branch in the store that reads whether the caller
//! asked to open or to repair. Unreadability is settled before anything is
//! chosen, because the question the generation comparison answers — which image
//! is newer — is exactly what a slot this build cannot read has already made
//! unanswerable.

use super::{
    damage::{SlotDamage, SlotState},
    domination::{marks_of, verify_discard_preserves_marks, verify_marks_dominate},
    error::LockStoreError,
    format::SlotIndex,
    image::DecodedImage,
};

// Imported for the intra-doc links the prose below carries.
#[allow(unused_imports)]
use super::LockStore;

/// The slot recovery adopted.
pub(super) struct AdoptedImage {
    pub(super) slot: SlotIndex,
    pub(super) generation: u64,
    pub(super) image: DecodedImage,
    pub(super) cross_checked_marks: bool,
    /// The slot a repair gave up to reach this one, what it held, and whether
    /// its marks could be and were compared against this image's.
    pub(super) given_up: Option<(SlotIndex, SlotDamage, bool)>,
}

/// Whether a slot this build cannot read refuses the store or is given up.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OnUnreadableSlot {
    /// [`LockStore::open`]: refuse, and let the caller decide.
    Refuse,
    /// [`LockStore::open_and_repair`]: adopt the partner and report the cost.
    Give,
}

/// Picks the slot recovery adopts, refusing any slot it cannot read.
///
/// Unreadability is settled *before* anything is chosen, because the question
/// the generation comparison answers — which image is newer — is exactly the
/// question a slot this build cannot read has already made unanswerable.
pub(super) fn choose_live_slot(
    states: &[SlotState; 2],
    images: [Option<DecodedImage>; 2],
    on_unreadable: OnUnreadableSlot,
) -> Result<Option<AdoptedImage>, LockStoreError> {
    // Both damaged is reported as the stronger fact it is: there is no image at
    // all, whatever each slot's damage happened to be. A lock service that
    // cannot read any image cannot know its high-water marks, and opening empty
    // would reissue token 1 for a resource whose guarded downstream has already
    // accepted far more. This is above the repair branch rather than inside it:
    // a repair chooses between two readings of a store, and there is no second
    // reading here to choose.
    if states.iter().all(|state| state.damage().is_some()) {
        return Err(LockStoreError::NoReadableImage { slots: *states });
    }
    // One damaged slot. Skipping it is safe only when its damage proves it was
    // the slot being written; anything else could be the live image, and
    // adopting the partner would roll an acknowledged mark back one generation.
    let mut given_up = None;
    let mut cross_checked = false;
    for slot in [SlotIndex::Zero, SlotIndex::One] {
        let Some(damage) = states[slot.position()].damage() else {
            continue;
        };
        if damage.is_publication_residue() {
            continue;
        }
        let other = states[slot.other().position()];
        // A whole image whose mark reads unsealed is ambiguous only if it could
        // be the newest one. When the partner holds a *sealed* image of a
        // strictly greater generation it cannot be: generations are strictly
        // increasing, and a sealed image is one some publication finished, so
        // the partner is the newer committed image whichever history left this
        // slot's bytes. Adopting the partner is then correct under both readings
        // rather than a choice between them, and no operator is needed.
        //
        // This is what an ordinary publication interrupted in its first bytes
        // leaves: those bytes are still byte-for-byte the older image's — the
        // magic, the version, and the leading bytes of the generation — so the
        // slot holds the whole older image with its mark overwritten.
        //
        // "Correct under both readings" is a claim about what a caller can
        // observe, and the fencing marks are what a caller observes. So it is
        // checked here rather than argued: the image being set aside was
        // decoded by `open_inner`, and the partner must dominate its marks. In
        // the ordinary case it does — the partner is newer, and marks only
        // ever rise — and running the check anyway is what makes the sentence
        // above a property of this store rather than of this paragraph.
        if let SlotDamage::UnsealedCompleteImage { generation, .. } = damage {
            if let SlotState::Intact {
                generation: sealed_generation,
                ..
            } = other
            {
                if sealed_generation > generation {
                    cross_checked |= verify_discard_preserves_marks(
                        slot,
                        damage,
                        images[slot.position()].as_ref(),
                        slot.other(),
                        images[slot.other().position()].as_ref(),
                    )?;
                    continue;
                }
            }
        }
        // Three ways this stays a refusal even under a repair:
        //
        // - A version this build cannot read is not damage, so it is not
        //   something a repair may clear. Giving it up would delete a newer
        //   build's committed work on the strength of one byte.
        // - The partner holds no image. A repair chooses between two readings of
        //   a store, and an empty partner is not a second reading: adopting it
        //   would open a fresh lock service over a store that has committed, and
        //   hand out token 1 for a resource whose guarded downstream has already
        //   accepted far more. That is the same fail-closed rule as
        //   `NoReadableImage`, reached from the other side.
        // - The caller asked to open rather than to repair.
        if matches!(damage, SlotDamage::UnsupportedFormatVersion { .. })
            || !matches!(other, SlotState::Intact { .. })
            || on_unreadable == OnUnreadableSlot::Refuse
        {
            return Err(LockStoreError::UnreadableSlot {
                slot,
                damage,
                other,
            });
        }
        // A fourth way this stays a refusal, and the only one a repair reaches:
        // the image being given up is readable enough to name its marks, and
        // the partner does not dominate them. See
        // `verify_discard_preserves_marks` for why that is refused rather than
        // repaired-and-reported.
        let checked = verify_discard_preserves_marks(
            slot,
            damage,
            images[slot.position()].as_ref(),
            slot.other(),
            images[slot.other().position()].as_ref(),
        )?;
        cross_checked |= checked;
        given_up = Some((slot, damage, checked));
    }

    let generations = [generation_of(states[0]), generation_of(states[1])];
    let [zero, one] = images;

    match (generations[0], generations[1], zero, one) {
        (Some(left), Some(right), _, _) if left == right => {
            Err(LockStoreError::AmbiguousGeneration { generation: left })
        }
        (Some(left), Some(right), Some(zero), Some(one)) => {
            // Both slots hold a committed image. This is the only shape in
            // which a second committed image exists to compare against, and
            // wherever it exists the comparison runs: recovery re-checks the
            // marks across the commit boundary it is recovering rather than
            // taking the newest image's word for it.
            let (newer_slot, newer, older) = if left > right {
                (SlotIndex::Zero, zero, one)
            } else {
                (SlotIndex::One, one, zero)
            };
            verify_marks_dominate(&marks_of(&older.service), &marks_of(&newer.service))?;
            if newer.applied_index < older.applied_index {
                return Err(LockStoreError::AppliedIndexRegression {
                    previous: older.applied_index,
                    found: newer.applied_index,
                });
            }
            Ok(Some(AdoptedImage {
                slot: newer_slot,
                generation: left.max(right),
                image: newer,
                cross_checked_marks: true,
                given_up,
            }))
        }
        // One committed image beside a slot holding none. Reaching here is a
        // proof rather than a default: the partner is empty, or it holds
        // residue, and residue means the partner was the slot being written —
        // so whatever it last committed was older than what is adopted here.
        //
        // There is no *second committed* image to compare against. There may
        // still be an image: a slot whose only fault is its mark holds a whole
        // one, and it is set aside or given up above, where the comparison does
        // run. `cross_checked` carries that answer here rather than a constant
        // `false`, so this flag keeps meaning "an image was compared against
        // this one" in every shape rather than only in the two-intact one.
        (Some(generation), None, Some(image), _) => Ok(Some(AdoptedImage {
            slot: SlotIndex::Zero,
            generation,
            image,
            cross_checked_marks: cross_checked,
            given_up,
        })),
        (None, Some(generation), _, Some(image)) => Ok(Some(AdoptedImage {
            slot: SlotIndex::One,
            generation,
            image,
            cross_checked_marks: cross_checked,
            given_up,
        })),
        // Neither slot holds a committed image, and neither is unreadable. A
        // publication only ever writes the stale slot, so an empty partner
        // means no publication has ever committed and there was never a mark to
        // lose. This is the one case the fail-closed rules must not catch.
        _ => Ok(None),
    }
}

const fn generation_of(state: SlotState) -> Option<u64> {
    match state {
        SlotState::Intact { generation, .. } => Some(generation),
        SlotState::Empty | SlotState::Damaged(_) => None,
    }
}
