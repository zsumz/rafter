//! What opening the store found, and what the two destructive openings gave up.
//!
//! [`RecoveryReport`] describes one opening and never changes afterwards.
//! [`Repair`] and [`Reseed`] are the two things that can be recorded in it that
//! cost something: a repair chooses between two readings of a store and can say
//! the adopted one dominates the discarded one's marks, and a re-seed keeps
//! neither reading and can say no such thing.

use std::fmt;

use rafter::LogIndex;

use super::{
    damage::{SlotDamage, SlotState},
    format::SlotIndex,
};

// Imported for the intra-doc links the prose below carries.
#[allow(unused_imports)]
use super::{error::LockStoreError, LockStore};

/// What opening the store found and did.
///
/// Recovery observations are not failures, so they stay out of
/// [`LockStoreError`]. A test asserts on this report to show which crash window
/// it actually reproduced.
///
/// The report describes one opening and never changes afterwards. A later
/// publication does not edit the history of how this handle came to exist; a
/// caller that wants to know what a fresh opener would find reopens.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryReport {
    pub(super) created: bool,
    pub(super) slots: [SlotState; 2],
    pub(super) live_slot: Option<SlotIndex>,
    pub(super) cross_checked_marks: bool,
    pub(super) repair: Option<Repair>,
    pub(super) reseed: Option<Reseed>,
}

/// What [`LockStore::discard_and_reseed`] deleted.
///
/// A re-seed is the one thing this store does that discards state it *could*
/// read, so what it found is recorded before it goes. This is the whole of what
/// the deletion can be held to: nothing here is a promise that the log will
/// refill it, which is a fact about the composition and not about this
/// directory.
///
/// It is deliberately not a [`Repair`]. A repair chooses between two readings
/// of a store and can say the adopted one dominates the discarded one's marks;
/// a re-seed keeps neither reading and can say no such thing.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Reseed {
    pub(super) slots: [SlotState; 2],
    pub(super) discarded_bytes: u64,
    pub(super) discarded_applied_index: Option<LogIndex>,
}

impl Reseed {
    /// What each slot held when the re-seed found it, indexed by [`SlotIndex`].
    #[must_use]
    pub const fn slots(&self) -> [SlotState; 2] {
        self.slots
    }

    /// Bytes removed from the two slot files.
    #[must_use]
    pub const fn discarded_bytes(&self) -> u64 {
        self.discarded_bytes
    }

    /// The applied Raft index the deleted store had reached, when a whole image
    /// could name one.
    ///
    /// This is the number that says how far the replay has to go, so it is
    /// reported rather than left to a byte count. It reads a whole image whose
    /// mark is unsealed as well as a sealed one, because that damage is a whole
    /// image and refusing to name its index would leave the wedged store — the
    /// case this entry point exists for — reporting nothing.
    ///
    /// `None` means neither slot held an image this build could decode, which
    /// is where nobody can say how far it had got.
    #[must_use]
    pub const fn discarded_applied_index(&self) -> Option<LogIndex> {
        self.discarded_applied_index
    }
}

impl fmt::Display for Reseed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "discarded {} bytes over {} and {}, which held {} and {}",
            self.discarded_bytes,
            SlotIndex::Zero,
            SlotIndex::One,
            self.slots[0],
            self.slots[1]
        )?;
        match self.discarded_applied_index {
            Some(applied_index) => write!(
                formatter,
                "; the deleted store had applied through {applied_index} and the replicated \
                 log must carry it back"
            ),
            None => formatter.write_str(
                "; no slot held an image this build could decode, so how far it had applied \
                 is unknown",
            ),
        }
    }
}

/// What [`LockStore::open_and_repair`] gave up, when it gave anything up.
///
/// A repair is the one thing this store does that can lose committed state, so
/// it is recorded rather than implied, and it is unreachable through
/// [`LockStore::open`] at all.
///
/// What it can say about the loss depends on which damage it gave up, and it now
/// says which. That distinction used to be missing, and its absence is the shape
/// of the defect this type was part of.
///
/// - When the slot held [`SlotDamage::UnsealedCompleteImage`], the image is
///   whole and verified under the restored mark, so this build *can* read it.
///   [`Repair::discarded_generation`] names its generation and
///   [`Repair::marks_cross_checked`] is `true`, which means the adopted image
///   was proved to carry every fencing high-water mark the discarded one did.
///   Where that proof fails the repair does not happen at all:
///   [`LockStoreError::DiscardWouldRegressMark`].
/// - For every other damage nothing is decodable, and there the old sentence is
///   the true one: reading that slot is exactly what failed, so nobody can say
///   what was in it. Both fields say so — `None` and `false` — instead of the
///   type implying a check that did not run.
///
/// The bound in either case is one publication, because generations are strictly
/// increasing and only two slots exist.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Repair {
    pub(super) slot: SlotIndex,
    pub(super) damage: SlotDamage,
    pub(super) adopted: SlotIndex,
    pub(super) adopted_generation: u64,
    pub(super) discarded_generation: Option<u64>,
    pub(super) marks_cross_checked: bool,
}

impl Repair {
    /// The slot whose contents were given up.
    #[must_use]
    pub const fn slot(&self) -> SlotIndex {
        self.slot
    }

    /// Why that slot could not be read.
    #[must_use]
    pub const fn damage(&self) -> SlotDamage {
        self.damage
    }

    /// The slot adopted in its place.
    #[must_use]
    pub const fn adopted(&self) -> SlotIndex {
        self.adopted
    }

    /// Publication generation of the image adopted in its place.
    ///
    /// At most one publication separates this from
    /// [`Repair::discarded_generation`].
    #[must_use]
    pub const fn adopted_generation(&self) -> u64 {
        self.adopted_generation
    }

    /// Publication generation of the discarded image, when it was decodable.
    ///
    /// `None` means the slot held damage this build could not read past, which
    /// is where "nobody can say what was in it" is the true statement rather
    /// than a stand-in for one.
    #[must_use]
    pub const fn discarded_generation(&self) -> Option<u64> {
        self.discarded_generation
    }

    /// Whether the discarded image's fencing marks were compared against the
    /// adopted image's.
    ///
    /// `true` is a proof, not a note: the repair happened, so the comparison
    /// passed, so the adopted image carries every fencing high-water mark the
    /// discarded one did. A repair where it would have failed is refused with
    /// [`LockStoreError::DiscardWouldRegressMark`] and produces no `Repair` at
    /// all.
    ///
    /// `false` means there was nothing to compare — see
    /// [`Repair::discarded_generation`] — and is the one shape in which this
    /// report cannot bound the loss in the dimension that matters.
    #[must_use]
    pub const fn marks_cross_checked(&self) -> bool {
        self.marks_cross_checked
    }
}

impl fmt::Display for Repair {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "gave up {}, which held {}, and adopted generation {} from {}",
            self.slot, self.damage, self.adopted_generation, self.adopted
        )?;
        match (self.discarded_generation, self.marks_cross_checked) {
            (Some(discarded), true) => write!(
                formatter,
                "; the discarded generation {discarded} was readable and its fencing \
                 high-water marks are all carried by the adopted image"
            ),
            (_, _) => formatter.write_str(
                "; the discarded image was not readable, so no fencing high-water mark it \
                 held could be checked against the one adopted",
            ),
        }
    }
}

impl RecoveryReport {
    /// Whether this open created the store's slot files.
    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }

    /// Returns what each slot held, indexed by [`SlotIndex`].
    #[must_use]
    pub const fn slots(&self) -> [SlotState; 2] {
        self.slots
    }

    /// Returns what one slot held.
    #[must_use]
    pub const fn slot(&self, slot: SlotIndex) -> SlotState {
        self.slots[slot.position()]
    }

    /// Returns the slot recovery adopted, if any image was readable.
    #[must_use]
    pub const fn live_slot(&self) -> Option<SlotIndex> {
        self.live_slot
    }

    /// Returns which slot an interrupted publication damaged, and how.
    ///
    /// At most one slot can be damaged in a store that opened, so this is
    /// unambiguous: two damaged slots fail closed with
    /// [`LockStoreError::NoReadableImage`], and damage no publication could
    /// have left fails closed with [`LockStoreError::UnreadableSlot`].
    ///
    /// The slot index is the load-bearing half. Without it a caller cannot tell
    /// benign residue in the slot that was being written from anything else,
    /// which is the difference between "the crash cost nothing" and a question
    /// worth asking.
    #[must_use]
    pub const fn damaged_slot(&self) -> Option<(SlotIndex, SlotDamage)> {
        match self.slots[0].damage() {
            Some(damage) => Some((SlotIndex::Zero, damage)),
            None => match self.slots[1].damage() {
                Some(damage) => Some((SlotIndex::One, damage)),
                None => None,
            },
        }
    }

    /// Whether this opening found nothing to report.
    ///
    /// A clean opening adopted an image with no damaged slot beside it, in a
    /// directory that already held a store. Anything else — residue from an
    /// interrupted publication, or creating the slot files — is a fact a caller
    /// reopening a store after a crash should have to look at rather than step
    /// over.
    ///
    /// A re-seed is never clean, and does not need its own term here: it
    /// deletes both slot files and the opening that follows creates them, so
    /// `created` is already true. What [`RecoveryReport::reseed`] adds is
    /// *which* creation this was, which is the distinction the paragraph below
    /// says this store cannot make on its own.
    ///
    /// Creation counts deliberately. This store cannot tell a genuinely fresh
    /// replica from one whose directory was emptied, because both arrive here
    /// as an absent pair of files; only the caller knows which it is. Leaving
    /// creation out of this predicate made the difference invisible to the one
    /// party that could see it — and made `created()` a report nothing read,
    /// which is a report that costs nothing to be wrong. A caller that expects
    /// to be creating a store looks at [`RecoveryReport::created`] and carries
    /// on.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        !self.created && self.damaged_slot().is_none()
    }

    /// Whether recovery compared the adopted image's fencing high-water marks
    /// against a second image it found.
    ///
    /// True in three shapes, each with a test naming it:
    ///
    /// - two intact slots, where the comparison runs across the commit boundary
    ///   being recovered —
    ///   `recovery_re_checks_the_marks_across_the_commit_boundary_it_recovers`;
    /// - a whole unsealed image set aside because the partner is strictly newer
    ///   — `setting_a_whole_image_aside_cross_checks_its_marks`;
    /// - a repair that gave one up —
    ///   `a_repair_that_regresses_only_session_progress_is_allowed_and_reported`.
    ///
    /// It used to be true only in the first, which left the other two dropping a
    /// decoded image with nothing checked and nothing said.
    ///
    /// That those are *all* the shapes where a second image exists is a reading
    /// of `choose_live_slot`, not something checked: there is no type here whose
    /// variants a fourth one would have to be added to, so nothing fails to
    /// compile if a later branch adopts an image without comparing. The three
    /// tests pin the three arms; the closure over them is the part a reviewer
    /// still has to supply, and saying so is cheaper than a count that reads as
    /// though something counted.
    ///
    /// False means there was no second image to compare against — an empty
    /// partner, residue holding no whole image, or a re-seeded store, which
    /// keeps no image at all. It is the report saying so rather than implying a
    /// check ran; `a_repair_that_cannot_read_the_discarded_image_says_so` is
    /// that side.
    #[must_use]
    pub const fn cross_checked_marks(&self) -> bool {
        self.cross_checked_marks
    }

    /// What a repair gave up, when this opening was a repair that found work.
    ///
    /// Always `None` for [`LockStore::open`], which has no branch that reaches
    /// it. A [`LockStore::open_and_repair`] over a healthy store reports `None`
    /// too: repairing is not the same as being willing to repair.
    #[must_use]
    pub const fn repair(&self) -> Option<Repair> {
        self.repair
    }

    /// What a re-seed deleted, when this opening was a
    /// [`LockStore::discard_and_reseed`].
    ///
    /// Always `None` for the other two entry points, which have no branch that
    /// reaches it. Unlike [`RecoveryReport::repair`] it is `Some` on **every**
    /// re-seed, including one over a store with nothing wrong with it, because
    /// a re-seed over a healthy store still deletes it. There is no being
    /// willing to re-seed.
    #[must_use]
    pub const fn reseed(&self) -> Option<Reseed> {
        self.reseed
    }
}
