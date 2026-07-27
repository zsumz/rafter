//! What a slot's bytes turned out to be, and what each slot held.
//!
//! [`SlotDamage`] is the vocabulary the `# Which unreadable slots recovery may
//! skip` section of the [module documentation](super) is written in: one
//! variant per boundary an interrupted publication can stop at, and exactly one
//! of them a later opener may skip. [`SlotState`] is the same answer as a
//! [`RecoveryReport`] reports it.

use std::fmt;

use rafter::LogIndex;

// Imported for the intra-doc links the prose below carries. Splitting the
// store's one file moved these types into sibling modules; not one sentence
// that names them changed.
#[allow(unused_imports)]
use super::{error::LockStoreError, report::RecoveryReport, LockStore};

/// Why a slot could not be adopted.
///
/// A damaged slot is the normal residue of an interrupted publication, not a
/// fault, so it is reported through [`RecoveryReport`] rather than as a
/// [`LockStoreError`]. Each variant names the byte boundary the interrupted
/// write reached, which is what lets a crash test prove that its injection bit
/// where it aimed.
///
/// Exactly one of these variants is residue an interrupted publication can
/// leave; see [`SlotDamage::is_publication_residue`]. The rest are reported here
/// because the report says what recovery *found*, but finding one refuses the
/// store rather than skipping the slot.
///
/// "Exactly one" is a closure claim, so it is checked rather than asserted:
/// `exactly_one_slot_damage_is_residue_an_interrupted_publication_leaves`
/// matches on every variant by name, so a variant added later does not compile
/// until somebody has decided which side of the skip rule it falls on.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SlotDamage {
    /// The slot file holds no bytes at all.
    ///
    /// Not even the mark creation writes, which every slot this store creates
    /// carries from its first instant. A slot of zero bytes was therefore
    /// emptied by something that is not this store, and an emptied slot is
    /// unreadable rather than absent: nothing about it says whether it once
    /// held the newest committed image.
    SlotEmptied,
    /// A publication wrote part of an image and never sealed it.
    ///
    /// Both halves of that sentence are checked. The slot carries
    /// `UNSEALED_MARK` in its first byte, *and* the bytes present are not a
    /// whole image: read again with that byte restored to `SEALED_MARK` they
    /// still fail to verify, at a step this build can read. `present` is the
    /// byte boundary the interrupted write reached.
    ///
    /// This is the only damage recovery may skip. The mark alone would not earn
    /// that, and used to be asked to.
    UnsealedPublication {
        /// Bytes present in the slot.
        present: u64,
    },
    /// A whole image that verifies, whose mark says it was never sealed.
    ///
    /// Two histories leave these exact bytes and nothing in them tells the two
    /// apart:
    ///
    /// - a publication that wrote the whole image, reached its durability
    ///   barrier, and died before the one byte that seals it — the
    ///   written-but-not-committed window; or
    /// - a committed, adopted, acknowledged image whose one mark byte later
    ///   rotted from `SEALED_MARK` to `UNSEALED_MARK`.
    ///
    /// They are the same bytes because the mark is the only difference between
    /// those two states on the medium, and one byte cannot record which of them
    /// it is. The generations do not separate them either: the slot being
    /// written carries the live slot's generation plus one, and so does a live
    /// slot whose partner is the stale one, so the pair looks identical under
    /// both readings.
    ///
    /// Skipping is right under the first reading and drops an acknowledged
    /// fencing high-water mark under the second — the exact failure this design
    /// exists to prevent. So recovery refuses and says which it found:
    /// [`LockStoreError::UnreadableSlot`] names it, and
    /// [`LockStore::open_and_repair`] is where a caller who has decided which
    /// reading applies says so by name. Refusing is recoverable under both
    /// readings; skipping is recoverable under only one.
    ///
    /// It is a separate variant from [`SlotDamage::UnsealedPublication`] because
    /// the two are separate facts, and a report that called this one an
    /// interrupted publication would be claiming to know which history happened.
    ///
    /// `generation` is what lets recovery answer the question without an
    /// operator whenever it can be answered. The ambiguity above only matters if
    /// this slot could be the newest image; when the partner holds a *sealed*
    /// image of a strictly greater generation it cannot be, under either
    /// reading, and recovery sets it aside and adopts the partner. That is the
    /// ordinary shape of a publication interrupted in its first bytes, where the
    /// new image's leading bytes are still byte-for-byte the old image's and the
    /// slot therefore still holds the whole older image.
    UnsealedCompleteImage {
        /// Length of the whole image that verified.
        len: u64,
        /// Publication generation that image declares.
        generation: u64,
    },
    /// A sealed slot holds fewer bytes than one slot header needs.
    HeaderIncomplete {
        /// Bytes present in the slot.
        present: u64,
    },
    /// The slot does not begin with this store's magic.
    ///
    /// The mark this store writes into byte zero is part of that magic, so this
    /// also covers a slot whose first byte is neither `SEALED_MARK` nor
    /// `UNSEALED_MARK`.
    NotALockImage {
        /// The four bytes found where the magic belongs, zero-padded when the
        /// slot is shorter than four bytes.
        magic: [u8; 4],
    },
    /// The slot declares a format this build cannot read.
    UnsupportedFormatVersion {
        /// Version byte found in the header.
        version: u8,
    },
    /// The header's own checksum does not match its bytes.
    HeaderChecksumMismatch {
        /// Checksum the header declares.
        declared: u32,
        /// Checksum computed over the header's bytes.
        computed: u32,
    },
    /// The payload the header declares is not entirely present.
    PayloadIncomplete {
        /// Payload length the header declares.
        declared: u64,
        /// Payload bytes actually present.
        present: u64,
    },
    /// The payload is complete and no trailing checksum follows it.
    ///
    /// This is the written-but-not-committed window.
    MissingCommitChecksum,
    /// Fewer bytes remain than one trailing checksum needs.
    PartialCommitChecksum {
        /// Trailer bytes present.
        present: u64,
    },
    /// The trailing checksum does not seal this header and payload.
    CommitChecksumMismatch {
        /// Checksum the trailer declares.
        declared: u32,
        /// Checksum computed over the header and payload.
        computed: u32,
    },
    /// The slot carries bytes after its trailing checksum.
    TrailingBytes {
        /// Bytes beyond the sealed image.
        extra: u64,
    },
}

impl SlotDamage {
    /// Whether an interrupted publication of *this* build left this.
    ///
    /// This is used in one direction only — a slot may be skipped **because**
    /// it is residue — so the implication that has to hold is the one the
    /// caller relies on, written here in that direction:
    ///
    /// > **If this returns `true`, the slot was never the live image.**
    ///
    /// The proof, stated forwards rather than as somebody else's
    /// contrapositive. Suppose this returns `true`. Then the damage is
    /// [`SlotDamage::UnsealedPublication`], and `classify_unsealed` produces
    /// that variant only when **both** of these hold of the slot's bytes:
    ///
    /// 1. byte zero is `UNSEALED_MARK`; and
    /// 2. read again with byte zero restored to `SEALED_MARK` — the value both
    ///    of the slot's checksums are computed over — the bytes still fail to
    ///    verify as a whole image, at a step whose meaning this build knows.
    ///
    /// Now suppose, for contradiction, that the slot *was* the live image. Then
    /// some publication sealed it, which it does by writing byte zero last, only
    /// after every other byte of a whole image has passed a durability barrier.
    /// So the slot's bytes began as a whole image that verified with a sealed
    /// mark. By (2) they no longer verify with a sealed mark, so at least one
    /// byte other than the mark has been lost or altered since. By (1) the mark
    /// byte has *also* changed, from `b'R'` to `0x00`. That is two independent
    /// alterations. The crash contract this file rests on admits exactly one
    /// failure — a crash leaves a prefix of what was written — and a prefix
    /// cannot alter a byte it never reached. So the slot was never the live
    /// image. ∎
    ///
    /// The assumption in the last step is the honest limit, and it is stated
    /// rather than hidden: a medium that alters the mark byte *and* damages the
    /// image beside it defeats this rule, as it defeats every rule with a single
    /// checksum behind it. What is now excluded is the single-fault case, which
    /// is what a one-byte rot is, and
    /// `no_single_byte_change_to_a_sealed_image_is_ever_publication_residue`
    /// checks that exhaustively rather than asserting it here.
    ///
    /// The two things this deliberately does **not** cover are
    /// [`SlotDamage::UnsealedCompleteImage`] — a whole image with an unsealed
    /// mark, where step (2) fails and the answer is a refusal — and every damage
    /// with a sealed mark, where the bytes are what some completed publication
    /// sealed and any damage to them happened afterwards. A sealed image that
    /// has merely lost its last byte is in the second group: it is a strict
    /// prefix of an image this build wrote and would have passed a shape test,
    /// and recovery cannot rule out that it held the newest committed state.
    #[must_use]
    pub const fn is_publication_residue(self) -> bool {
        matches!(self, Self::UnsealedPublication { .. })
    }
}

impl fmt::Display for SlotDamage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SlotEmptied => formatter.write_str("an emptied slot file"),
            Self::UnsealedPublication { present } => {
                write!(formatter, "{present} bytes of an unsealed publication")
            }
            Self::UnsealedCompleteImage { len, generation } => write!(
                formatter,
                "a whole {len} byte image of generation {generation} whose publication mark reads \
                 unsealed"
            ),
            Self::HeaderIncomplete { present } => {
                write!(formatter, "a sealed image cut to {present} bytes")
            }
            Self::NotALockImage { magic } => write!(formatter, "foreign magic {magic:?}"),
            Self::UnsupportedFormatVersion { version } => {
                write!(formatter, "unsupported format version {version}")
            }
            Self::HeaderChecksumMismatch { .. } => formatter.write_str("a corrupt header"),
            Self::PayloadIncomplete { declared, present } => write!(
                formatter,
                "{present} bytes of a declared {declared} byte payload"
            ),
            Self::MissingCommitChecksum => formatter.write_str("a written but uncommitted image"),
            Self::PartialCommitChecksum { .. } => formatter.write_str("a partial commit checksum"),
            Self::CommitChecksumMismatch { .. } => {
                formatter.write_str("a commit checksum that seals nothing")
            }
            Self::TrailingBytes { extra } => {
                write!(formatter, "{extra} bytes beyond the sealed image")
            }
        }
    }
}

/// What one slot held when the store was opened.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SlotState {
    /// The slot carries its creation mark and nothing more.
    ///
    /// Nothing has ever been sealed into it. A publication that emitted only
    /// its first byte before dying leaves the same thing, and says the same
    /// thing, so the two are not distinguished.
    Empty,
    /// An interrupted or corrupted publication left the slot unusable.
    Damaged(SlotDamage),
    /// The slot holds a sealed image.
    Intact {
        /// Publication generation the image declares.
        generation: u64,
        /// Applied Raft index the image declares.
        applied_index: LogIndex,
    },
}

impl SlotState {
    /// Returns the damage this slot carries, if any.
    #[must_use]
    pub const fn damage(self) -> Option<SlotDamage> {
        match self {
            Self::Damaged(damage) => Some(damage),
            Self::Empty | Self::Intact { .. } => None,
        }
    }
}

impl fmt::Display for SlotState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("no image"),
            Self::Damaged(damage) => write!(formatter, "{damage}"),
            Self::Intact {
                generation,
                applied_index,
            } => write!(
                formatter,
                "generation {generation} at applied index {applied_index}"
            ),
        }
    }
}
