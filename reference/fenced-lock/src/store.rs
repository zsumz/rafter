//! The fenced lock service's durable transactional application backend.
//!
//! [`LockStore`] holds every fact the contract enumerates — the lock table,
//! every tracked resource's fencing high-water mark, all sessions with their
//! cached operation, fingerprint, and result, the replicated logical time, and
//! the applied Raft index — and moves all of them across one atomic, durable
//! commit point. A reader auditing this store should be able to answer, from
//! this file alone, what the commit point is and what a crash on either side of
//! it leaves.
//!
//! # Why two alternating whole images
//!
//! The contract requires one transaction to bind six kinds of fact together,
//! and it singles out one of them: a per-resource high-water mark that must
//! never decrease, through release, expiration, recreation, snapshot,
//! compaction, and restart. Losing a mark reissues a token that a guarded
//! resource has already accepted, which is the exact failure fencing exists to
//! prevent. That requirement, rather than throughput, decided this design.
//!
//! The store keeps two slot files and publishes into whichever one is *not*
//! currently authoritative. A slot is a whole image with a trailing checksum,
//! and the newest slot whose checksum verifies wins. This buys three things:
//!
//! 1. **The commit point is one `sync_data`, and there is only one write
//!    path.** A publication never renames, never stages a third file, never
//!    appends after earlier bytes, and never truncates a tail it has to reason
//!    about. Applying a batch and installing a snapshot are the same
//!    publication with different applied-index rules, so there is one crash
//!    argument to audit instead of two.
//! 2. **The authoritative image is never the one being written.** That single
//!    sentence is the whole atomicity argument: a crash at any byte of a
//!    publication leaves the previous image untouched and readable, because the
//!    file holding it was not open for writing.
//! 3. **The previous image survives the commit that replaces it**, which is
//!    what lets recovery *prove* rather than assume that marks did not regress
//!    across the transaction it is recovering. A design that discarded history
//!    on every commit would have to take the newest image's word for it.
//!
//! A write-ahead journal of appended frames — the shape the sibling ledger
//! consumer chose — was the alternative. It is a better fit for state that
//! grows by deltas or that wants an audit trail, and it makes the "written but
//! not committed" window explicit in the file layout. It was rejected here for
//! two reasons. The lock's durable state is bounded twice over by
//! [`LockConfig`] and, crucially, its tracked-resource table never shrinks, so
//! an appending journal would need a compaction mechanism forever just to bound
//! a state that is already bounded — and compaction is a second publication
//! path, in the one system where a bookkeeping slip loses a mark. And an
//! appending journal keeps history only until that compaction runs, so the
//! cross-check in (3) would hold only sometimes.
//!
//! What this design gives up, stated plainly: it overwrites in place, so it
//! relies on the argument in (2) rather than on append-only immutability, and
//! it rewrites the whole image for a one-field change. The first is checked by
//! the crash sweep in `durable_crash.rs`; the second is affordable because the
//! image is config-bounded and small.
//!
//! # Mark durability
//!
//! Two checks defend the marks at the narrowest points available:
//!
//! - **Every publication** must dominate the marks the store has already
//!   durably acknowledged: every resource tracked in the live image must be
//!   tracked in the proposed one with a mark at least as high. A commit or a
//!   snapshot install that would lower one is refused with
//!   [`LockStoreError::MarkRegression`] before a byte is written.
//! - **Every recovery** that finds both slots intact re-runs that comparison
//!   across the commit boundary it is recovering, using the older slot the
//!   design happens to preserve.
//!
//! Neither check can substitute for the model's own bookkeeping, and neither is
//! meant to: [`LockService`] remains the semantic authority on which token a
//! resource issues next. These are the durability boundary refusing to publish
//! or adopt a state that contradicts contract invariant 2.
//!
//! # Republishing one commit point
//!
//! [`LockStore::install`] may republish the applied index the store already
//! holds, because adopting the state a replica already has must not require
//! inventing an index. That is the one publication whose freshness the applied
//! index cannot judge — two images can name the same index and still disagree
//! about which requests have completed.
//!
//! The session cache is what disagrees. Dropping a client slot's cached
//! completion makes an acknowledged operation executable again, and for an
//! acquisition that mints a *second* fencing token for one tenure — the same
//! failure as a lost mark, reached by another road. So an install at an
//! unchanged applied index must also dominate the durable session cache, slot
//! by slot, through [`SessionProgress`]: the session epoch first, then the
//! highest completed sequence under it. Opening a newer epoch is what
//! legitimately clears an older epoch's cache, which is why the epoch outranks
//! the sequence rather than sitting beside it.
//!
//! The check is scoped to an unchanged index deliberately. Above that index the
//! model has legitimately advanced, and it — not the durability boundary — is
//! the authority on which sessions it retired or replaced along the way. At an
//! unchanged index nothing legitimately changed, so a state that lost a
//! completion is simply a poorer image of the same commit point, and
//! [`LockStoreError::SessionCacheRegression`] refuses it before a byte is
//! written.
//!
//! # Format
//!
//! The store owns one directory containing exactly two files, `lock-state.0`
//! and `lock-state.1`. Nothing else in the directory is durable state, and no
//! temporary or staging file is ever created, so there is nothing to clean up
//! at open.
//!
//! Ownership of that directory is assumed rather than enforced. Two live stores
//! over one directory would publish into the same slots and destroy each
//! other's images, and nothing here stops them. A durable process composition
//! needs a real exclusive lock; that arrives with the process slice.
//!
//! Unless a record says otherwise:
//!
//! - integers are unsigned and big-endian;
//! - records are packed with no alignment or padding;
//! - a magic or version other than the one named here refuses the store, and is
//!   never quietly skipped in favour of the other slot;
//! - checksums are CRC-32/IEEE, an accidental-corruption check and not an
//!   authentication tag; and
//! - a slot file of zero bytes is damage: creation writes a mark into each
//!   slot, so an empty slot is not a state this store ever leaves behind.
//!
//! ## Slot header (`RFLK`)
//!
//! The header has a fixed size of [`SLOT_HEADER_LEN`] bytes at offset zero.
//!
//! ```text
//! magic          [4]   "RFLK", with byte 0 held at 0x00 until the image seals
//! version        u8    1
//! generation     u64
//! applied_index  u64
//! max_clients    u32
//! max_resources  u32
//! payload_len    u32
//! crc32          u32
//! ```
//!
//! The magic's first byte doubles as the **publication mark**. A publication
//! writes it as `0x00` and promotes it to `b'R'` once the rest of the image is
//! durable, so a slot says of itself whether it was ever sealed. That one byte
//! is the whole of recovery's skip rule; see the section on it below. A sealed
//! slot is byte-for-byte what it would be without the mark, and both checksums
//! are computed over the sealed form, so an unsealed image cannot accidentally
//! verify either.
//!
//! `generation` is what orders the two slots, and it is why the header carries
//! its own checksum: recovery must not choose between two images using bytes it
//! has not verified. The same checksum is what makes `payload_len` safe to
//! trust when locating the trailer.
//!
//! `max_clients` and `max_resources` are the [`LockConfig`] the slot was
//! written under. Opening under different bounds is rejected rather than
//! reinterpreted, because the bounds decide which images are valid.
//!
//! `applied_index` is deliberately duplicated here and inside the payload. This
//! copy is what recovery orders, reports, and range-checks without decoding
//! anything; the payload's copy is what the Raft snapshot install path
//! cross-checks against its descriptor. A slot whose two copies disagree is
//! refused rather than reconciled.
//!
//! ## Payload
//!
//! `payload_len` bytes holding exactly one application snapshot frame, as
//! produced by the adapter's `encode_snapshot`. The store commits the same
//! bytes the Raft install path carries: the contract enumerates one set of
//! facts for the durable transaction and for the application snapshot, so
//! encoding them twice would be two chances to forget a high-water mark.
//!
//! ## Trailer
//!
//! ```text
//! crc32          u32
//! ```
//!
//! [`SLOT_TRAILER_LEN`] bytes covering the header and the payload together. A
//! slot's image counts only when its header verifies, its payload is entirely
//! present, and this checksum matches everything before it. Covering the header
//! as well as the payload is what stops a payload from one generation being
//! read under a header from another.
//!
//! The trailer is not the commit marker, and the difference matters: a trailer
//! is at the end, so losing bytes destroys it, and an artifact whose only proof
//! of completeness sits where truncation lands cannot tell "never finished"
//! from "finished and then cut". The publication mark in byte zero is what says
//! an image was sealed; the trailer is what says the sealed bytes are intact.
//!
//! # Crash contract
//!
//! The authoritative artifact is the pair of slot files. The logical commit
//! point of a publication is the return of the second `sync_data`: the one that
//! follows the single byte sealing an image already made durable by the first.
//! `Ok` means the new state is what a fresh opener sees. `Err` means the
//! outcome is unknown, and reopening is the oracle that decides it — never an
//! inference that `Err` left no bytes changed.
//!
//! A crash at any byte boundary leaves the store recoverable to exactly the
//! pre-transaction or the post-transaction state, never between:
//!
//! - Before the stale slot is opened, both files are unchanged.
//! - From the first byte of the new image to the last, that slot carries the
//!   unsealed mark and is [`SlotDamage::UnsealedPublication`], whatever mixture
//!   of new prefix and older tail it holds. It cannot be chosen, and the live
//!   slot still holds the pre-transaction state.
//! - With the whole image durable and the seal not yet written, the image is
//!   written but not committed. This is the window a write-ahead design makes
//!   explicit in its layout and this one makes explicit in its mark, and it is
//!   still [`SlotDamage::UnsealedPublication`] — the point of the mark is that
//!   this window looks like every earlier one rather than like a nearly
//!   finished image.
//! - After the seal's sync returns, the new slot is committed, outranks the old
//!   one by generation, and is what recovery adopts.
//!
//! Nothing is truncated or repaired at open. The next publication overwrites
//! the slot it does not adopt, so the store heals itself without a repair path
//! that could run at the wrong moment.
//!
//! # Which unreadable slots recovery may skip
//!
//! Recovery is allowed to skip a slot it cannot read only when it can *prove*
//! that slot was not the live image. This section is that proof, and it is
//! written in the direction the proof is used.
//!
//! An earlier shape of this store argued the other direction and got away with
//! it for a while. It enumerated what an interrupted publication leaves — a
//! short header, a short payload, a payload with no trailer, a torn trailer —
//! showed the list was exhaustive, and then treated finding one of those shapes
//! as proof that a publication had been interrupted. That converse is false,
//! and one byte is enough to show it. A sealed image that loses its last byte
//! is a torn trailer. It is a strict prefix of an image this build wrote; it
//! carries this store's magic and this build's version; every byte present was
//! written and no checksum over present bytes fails. It satisfies the
//! enumeration exactly, and it is the live image. Skipping it adopts the stale
//! partner, drops an acknowledged fencing high-water mark, and reissues a token
//! a guarded resource has already accepted — the exact failure this design
//! exists to prevent, reached through the rule that was supposed to prevent it.
//!
//! No test on the bytes can separate those two cases, because they *are* the
//! same bytes: "a slot holding a prefix of the image with generation g, beside
//! a sealed image with generation g-1" describes both an interrupted
//! publication and a torn-off live image, and the generations differ by one
//! either way. So the write path puts the distinction into the artifact instead
//! of asking recovery to infer it.
//!
//! **A slot's first byte is its publication mark.** It is `0x00` while an image
//! is being written and `b'R'` — the magic's leading byte — once that image is
//! sealed. A publication writes the unsealed mark before any other byte of the
//! image, makes the whole image durable, and only then writes the one byte that
//! seals it. Since a crash leaves a prefix of what was written, byte zero is
//! written first and *every* interrupted publication leaves the unsealed mark.
//! Contrapositively, a sealed mark proves no publication was interrupted in
//! that slot, so its bytes are exactly what some completed publication sealed
//! there, and any damage to them happened after the seal.
//!
//! That is [`SlotDamage::is_publication_residue`], and it now holds in the
//! direction it is used: residue proves the slot was the one being written,
//! which proves it was never the live image, which is what makes skipping it
//! safe. **Every other damage refuses the whole store.** A foreign magic, a
//! version this build does not write, a checksum over bytes that are all
//! present, bytes beyond the seal, a sealed image cut short, an emptied file:
//! none of them carries the unsealed mark, so recovery has no argument that the
//! slot was the stale one, and adopting its partner would silently roll the
//! store back one generation. That is [`LockStoreError::UnreadableSlot`], and
//! it is a refusal to open rather than damage to skip.
//!
//! Two consequences of that ordering are worth stating on their own, because
//! both were wrong when the shapes were doing the work:
//!
//! - **The magic and the version are tested at every length**, before anything
//!   classifies a slot by how many bytes it has. The old order put both behind
//!   a full-header slice, so twenty bytes of a foreign format were read as this
//!   build's own residue, and the same version byte was refused at one length
//!   and ignored at another. The argument for refusing a version is about the
//!   field, so it has to hold wherever the field is present.
//! - **A slot file of zero bytes is damage.** Creation writes the unsealed mark
//!   into each slot, and no publication ever shortens a slot to nothing, so an
//!   empty slot file is not a state this store leaves behind at any point in
//!   its life. A pair of them is not a store that has never committed; it is a
//!   store whose files were emptied, and opening a fresh service over them
//!   would discard every fencing high-water mark with nothing reported.
//!   Likewise a slot file that should exist and does not:
//!   [`LockStoreError::MissingSlot`].
//!
//! A format-version mismatch is still worth naming on its own, because it needs
//! no corruption at all: a binary downgrade produces it from two entirely
//! healthy files. It is always a refusal.
//!
//! Recovery also refuses when **both** slots are damaged, whatever the damage,
//! rather than starting empty. A lock service that cannot read any image cannot
//! know its high-water marks, and one that started empty would hand out token 1
//! for a resource whose guarded downstream has already accepted a far higher
//! token. That is [`LockStoreError::NoReadableImage`].
//!
//! A store that has never committed is a different case and is not refused: its
//! slots carry their creation marks, which is not damage. More generally,
//! whenever recovery adopts an image it has shown that every slot it did not
//! adopt holds no sealed image at all — either the creation mark or residue an
//! interrupted publication left — so the image it adopts is the newest one any
//! publication ever sealed.
//!
//! [`RecoveryReport::damaged_slot`] names **which** slot was damaged as well as
//! how. That index is the load-bearing half: it is what tells a caller the
//! residue sat in the slot that was being written, rather than leaving benign
//! crash residue and a lost generation looking alike.
//!
//! After any write error the handle is poisoned and every later publication is
//! refused with [`LockStoreError::StoreRequiresReopen`], because a store that
//! failed mid-publication cannot say what its stale slot now contains.
//!
//! # What the crash tests do not prove
//!
//! `durable_crash.rs` interrupts publications inside one live process, so it
//! proves which bytes reached the file and what a fresh opener makes of them.
//! It cannot prove that a barrier reached the medium: a process that never dies
//! reads its own writes back through the page cache whether or not `sync_data`
//! ran. Deleting a `sync_data` from this file leaves the suite green. Those
//! calls are justified by the ordering argument above and by review, not by a
//! test, and a claim that this store survives power loss on a particular
//! filesystem needs evidence this suite does not supply.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

use rafter::LogIndex;

use crate::{
    adapter::{decode_snapshot, encode_snapshot},
    ClientId, FencingToken, LockCodecError, LockConfig, LockService, ResourceName, Sequence,
    SessionEpoch, SnapshotError,
};

/// Fixed length of a slot header, in bytes.
pub const SLOT_HEADER_LEN: usize = 37;
/// Fixed length of a slot's trailing commit checksum, in bytes.
pub const SLOT_TRAILER_LEN: usize = 4;

/// Offset of the header's own checksum within the header.
const HEADER_CHECKSUM_OFFSET: usize = SLOT_HEADER_LEN - 4;
/// Offset of the header's applied-index field.
const HEADER_APPLIED_INDEX_OFFSET: usize = 13;

const SLOT_MAGIC: [u8; 4] = *b"RFLK";

/// First byte of a slot whose image is sealed: the magic's leading byte.
///
/// See [`UNSEALED_MARK`] for what the same byte says while an image is being
/// written, and the module's recovery section for why the whole argument rests
/// on this one byte.
const SEALED_MARK: u8 = SLOT_MAGIC[0];

/// First byte of a slot whose image is being written.
///
/// A publication writes this value before any other byte of the image and
/// replaces it with [`SEALED_MARK`] only after every other byte is durable, so
/// a slot still carrying it is a slot no publication ever sealed.
const UNSEALED_MARK: u8 = 0x00;

/// Content of a slot file the moment it is created.
///
/// One unsealed mark and nothing else. A created slot is therefore not an empty
/// file, which is what lets a slot file of zero bytes be damage rather than the
/// ordinary state of a store that has never committed.
const CREATION_MARK: [u8; 1] = [UNSEALED_MARK];

/// Version byte of every slot this build writes.
const SLOT_FORMAT_VERSION: u8 = 1;

/// Stable names of the two slots inside the store's directory.
const SLOT_FILE_NAMES: [&str; 2] = ["lock-state.0", "lock-state.1"];

/// Which of the two slots a value names.
///
/// There are exactly two, forever: the design's whole atomicity argument is
/// that the live image is never the one being written, and one spare slot is
/// all that takes.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SlotIndex {
    /// `lock-state.0`.
    Zero,
    /// `lock-state.1`.
    One,
}

impl SlotIndex {
    /// Returns the slot a publication writes when this one is authoritative.
    #[must_use]
    pub const fn other(self) -> Self {
        match self {
            Self::Zero => Self::One,
            Self::One => Self::Zero,
        }
    }

    /// Returns the slot's stable file name.
    #[must_use]
    pub const fn file_name(self) -> &'static str {
        SLOT_FILE_NAMES[self.position()]
    }

    const fn position(self) -> usize {
        match self {
            Self::Zero => 0,
            Self::One => 1,
        }
    }
}

impl fmt::Display for SlotIndex {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.file_name())
    }
}

const CRC32_POLYNOMIAL: u32 = 0xEDB8_8320;

/// Half-byte residue table for [`crc32`], folded at compile time.
const CRC32_NIBBLES: [u32; 16] = {
    let mut table = [0_u32; 16];
    let mut index = 0_usize;
    let mut nibble = 0_u32;
    while index < 16 {
        let mut residue = nibble;
        let mut bit = 0;
        while bit < 4 {
            residue = if residue & 1 == 1 {
                (residue >> 1) ^ CRC32_POLYNOMIAL
            } else {
                residue >> 1
            };
            bit += 1;
        }
        table[index] = residue;
        index += 1;
        nibble += 1;
    }
    table
};

/// CRC-32/IEEE over `bytes`.
///
/// Folded a nibble at a time through a sixteen-entry table that is built at
/// compile time from the polynomial, so the constant a reviewer has to trust is
/// the polynomial itself rather than 256 pre-computed words.
/// `checksums_match_the_published_vector` pins the result to the standard check
/// value, and `checksums_agree_with_an_independently_written_implementation`
/// compares it against a bit-at-a-time version written the other way round.
fn crc32(bytes: &[u8]) -> u32 {
    let mut state = 0xFFFF_FFFF_u32;
    for byte in bytes {
        state ^= u32::from(*byte);
        state = (state >> 4) ^ CRC32_NIBBLES[(state & 0x0F) as usize];
        state = (state >> 4) ^ CRC32_NIBBLES[(state & 0x0F) as usize];
    }
    !state
}

/// Failure of a durable lock store operation.
///
/// This enum is exhaustive because the slot format is closed over these
/// corruption, configuration, and publication failures, and because a caller
/// deciding whether a transaction committed has to be able to match on all of
/// them.
///
/// It is deliberately neither `Clone` nor `Eq`: a variant carrying a live
/// [`std::io::Error`] has no meaningful value equality, and pretending
/// otherwise would invite tests to assert on a projection of an operating
/// system's diagnostics.
#[derive(Debug)]
pub enum LockStoreError {
    /// A filesystem operation failed.
    Io {
        /// Lowercase verb phrase naming the attempted operation.
        operation: &'static str,
        /// Path the operation addressed.
        path: PathBuf,
        /// Underlying filesystem failure.
        source: std::io::Error,
    },
    /// Neither slot holds a readable image, and at least one is damaged.
    ///
    /// A lock service that cannot read an image cannot know its fencing
    /// high-water marks. Opening empty would hand out token 1 for a resource
    /// whose guarded downstream has already accepted a higher token, so this
    /// fails closed instead.
    NoReadableImage {
        /// What each slot looked like, indexed by [`SlotIndex`].
        slots: [SlotState; 2],
    },
    /// One slot holds bytes no interrupted publication could have left behind.
    ///
    /// Recovery cannot rule out that this slot was the live image, so adopting
    /// its partner would silently roll the store back one generation: an
    /// acknowledged fencing high-water mark would drop and the next acquisition
    /// would reissue a token a guarded resource has already accepted. A slot
    /// this build cannot read is a refusal to open, never residue to skip.
    ///
    /// See [`SlotDamage::is_publication_residue`] for which damage this catches
    /// and why the rest is safe to skip.
    UnreadableSlot {
        /// Slot that could not be read.
        slot: SlotIndex,
        /// Why it could not be read.
        damage: SlotDamage,
        /// What the partner slot held — the image recovery declined to adopt in
        /// its place.
        other: SlotState,
    },
    /// A slot file that should exist does not.
    ///
    /// Every store this build creates has both slot files from its first
    /// instant, so a directory holding one of them lost the other. Recreating
    /// it and adopting the survivor would open the store one generation back —
    /// an acknowledged fencing high-water mark dropped, with `is_clean()` true
    /// and nothing reported. A slot that should exist and does not is
    /// unreadable rather than absent.
    MissingSlot {
        /// Slot whose file is gone.
        slot: SlotIndex,
        /// What the surviving partner held — the image recovery declined to
        /// adopt in its place.
        other: SlotState,
    },
    /// Both slots claim the same generation.
    ///
    /// Publications assign strictly increasing generations, so this is
    /// corruption rather than a crash residue, and it leaves recovery no rule
    /// for choosing between two images.
    AmbiguousGeneration {
        /// Generation both slots declare.
        generation: u64,
    },
    /// A slot was written under different resource bounds.
    ConfigMismatch {
        /// Slot the mismatch was found in.
        slot: SlotIndex,
        /// Client-slot bound recorded in the image.
        image_max_clients: u32,
        /// Tracked-resource bound recorded in the image.
        image_max_resources: u32,
        /// Client-slot bound the caller opened with.
        requested_max_clients: u32,
        /// Tracked-resource bound the caller opened with.
        requested_max_resources: u32,
    },
    /// A verified slot's payload is not a decodable application snapshot.
    ///
    /// The trailing checksum already proved these are the bytes that were
    /// written, so this is a build or encoding fault rather than a torn write,
    /// and it is an error rather than a reason to ignore the slot.
    Image {
        /// Slot the payload came from.
        slot: SlotIndex,
        /// Why the payload could not be decoded.
        source: LockCodecError,
    },
    /// A verified slot's payload violates a lock service invariant.
    Snapshot {
        /// Slot the payload came from.
        slot: SlotIndex,
        /// Which invariant the restored state broke.
        source: SnapshotError,
    },
    /// A slot's header and payload disagree about the applied Raft index.
    AppliedIndexDisagreement {
        /// Slot the disagreement was found in.
        slot: SlotIndex,
        /// Index the header declares.
        header_index: LogIndex,
        /// Index the payload declares.
        payload_index: LogIndex,
    },
    /// A publication or recovery would move the applied floor backwards.
    ///
    /// Recovery in this shape would make an acknowledged command executable
    /// again, which reissues a fencing token.
    AppliedIndexRegression {
        /// Applied index already durable.
        previous: LogIndex,
        /// Applied index that was offered.
        found: LogIndex,
    },
    /// A publication or recovery would lower a fencing high-water mark.
    ///
    /// This is contract invariant 2 refused at the durability boundary. A
    /// resource that vanishes from the state is the same failure as one whose
    /// mark decreases, and both are reported here.
    MarkRegression {
        /// Resource whose mark would move backwards.
        resource: ResourceName,
        /// Mark this store has durably acknowledged.
        acknowledged: FencingToken,
        /// Mark the offered state carries, if it tracks the resource at all.
        offered: Option<FencingToken>,
    },
    /// A republication at an unchanged applied index would move a client slot's
    /// session cache backwards.
    ///
    /// The applied Raft index is not the whole ordering key for the session
    /// cache: two images can name the same index and still disagree about which
    /// requests have completed. Adopting the poorer one makes an acknowledged
    /// operation executable again, and for an acquisition that mints a second
    /// fencing token for one tenure.
    SessionCacheRegression {
        /// Client slot whose session cache would move backwards.
        client: ClientId,
        /// Progress this store has durably acknowledged for that slot.
        acknowledged: SessionProgress,
        /// Progress the offered state carries, if it holds the slot at all.
        offered: Option<SessionProgress>,
    },
    /// An image is larger than the slot header's length field can describe.
    ImageTooLarge {
        /// Encoded length of the payload.
        length: u64,
    },
    /// An earlier write left this handle unable to say what its stale slot
    /// holds.
    ///
    /// Reopen the store; recovery is the only thing that can decide what the
    /// interrupted publication left behind.
    StoreRequiresReopen,
    /// A deterministic fault from the store's test construction fired.
    ///
    /// This is the injected-crash seam described on [`FaultPlan`]. It is a
    /// write failure like any other: the handle is poisoned and reopening
    /// decides what the interrupted publication left behind.
    InjectedFault {
        /// The fault that fired.
        fault: WriteFault,
        /// One-based ordinal of the publication it fired on.
        publication: u64,
    },
}

impl fmt::Display for LockStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} at {}: {source}",
                path.display()
            ),
            Self::NoReadableImage { slots } => write!(
                formatter,
                "no slot holds a readable lock image: {} holds {}, {} holds {}",
                SlotIndex::Zero,
                slots[0],
                SlotIndex::One,
                slots[1]
            ),
            Self::UnreadableSlot {
                slot,
                damage,
                other,
            } => write!(
                formatter,
                "{slot} holds {damage}, which no interrupted publication leaves, so it may have been \
                 the live image; {} holds {other} and is not adopted in its place",
                slot.other()
            ),
            Self::MissingSlot { slot, other } => write!(
                formatter,
                "{slot} is missing, so it may have been the live image; {} holds {other} and is not \
                 adopted in its place",
                slot.other()
            ),
            Self::AmbiguousGeneration { generation } => write!(
                formatter,
                "both slots claim generation {generation}, so neither outranks the other"
            ),
            Self::ConfigMismatch {
                slot,
                image_max_clients,
                image_max_resources,
                requested_max_clients,
                requested_max_resources,
            } => write!(
                formatter,
                "{slot} was written for {image_max_clients} clients and {image_max_resources} resources, \
                 but was opened for {requested_max_clients} clients and {requested_max_resources} resources"
            ),
            Self::Image { slot, source } => {
                write!(formatter, "malformed image in {slot}: {source}")
            }
            Self::Snapshot { slot, source } => {
                write!(formatter, "invalid image in {slot}: {source:?}")
            }
            Self::AppliedIndexDisagreement {
                slot,
                header_index,
                payload_index,
            } => write!(
                formatter,
                "{slot} declares applied index {header_index} in its header and {payload_index} in its payload"
            ),
            Self::AppliedIndexRegression { previous, found } => write!(
                formatter,
                "applied index {found} does not advance on the durable {previous}"
            ),
            Self::MarkRegression {
                resource,
                acknowledged,
                offered,
            } => write_mark_regression(formatter, *resource, *acknowledged, *offered),
            Self::SessionCacheRegression {
                client,
                acknowledged,
                offered,
            } => write_session_cache_regression(formatter, *client, *acknowledged, *offered),
            Self::ImageTooLarge { length } => write!(
                formatter,
                "image of {length} bytes exceeds the slot header's length field"
            ),
            Self::StoreRequiresReopen => formatter.write_str(
                "an earlier write failed mid-publication; reopen the store before mutating it",
            ),
            Self::InjectedFault { fault, publication } => {
                write!(formatter, "injected {fault} on publication {publication}")
            }
        }
    }
}

/// Renders a mark regression, which reads differently when the resource
/// vanished from the offered state than when its mark merely dropped.
fn write_mark_regression(
    formatter: &mut fmt::Formatter<'_>,
    resource: ResourceName,
    acknowledged: FencingToken,
    offered: Option<FencingToken>,
) -> fmt::Result {
    match offered {
        Some(offered) => write!(
            formatter,
            "resource {} would drop from fencing high-water mark {} to {}",
            resource.as_str(),
            acknowledged.get(),
            offered.get()
        ),
        None => write!(
            formatter,
            "resource {} would lose its fencing high-water mark of {}",
            resource.as_str(),
            acknowledged.get()
        ),
    }
}

/// Renders a session cache regression, which reads differently when the client
/// slot vanished from the offered state than when its progress merely dropped.
fn write_session_cache_regression(
    formatter: &mut fmt::Formatter<'_>,
    client: ClientId,
    acknowledged: SessionProgress,
    offered: Option<SessionProgress>,
) -> fmt::Result {
    match offered {
        Some(offered) => write!(
            formatter,
            "client slot {} would drop from {acknowledged} to {offered} at an unchanged applied index",
            client.get()
        ),
        None => write!(
            formatter,
            "client slot {} would lose its session cache at {acknowledged}",
            client.get()
        ),
    }
}

impl Error for LockStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Image { source, .. } => Some(source),
            Self::NoReadableImage { .. }
            | Self::UnreadableSlot { .. }
            | Self::MissingSlot { .. }
            | Self::AmbiguousGeneration { .. }
            | Self::ConfigMismatch { .. }
            | Self::Snapshot { .. }
            | Self::AppliedIndexDisagreement { .. }
            | Self::AppliedIndexRegression { .. }
            | Self::MarkRegression { .. }
            | Self::SessionCacheRegression { .. }
            | Self::ImageTooLarge { .. }
            | Self::StoreRequiresReopen
            | Self::InjectedFault { .. } => None,
        }
    }
}

/// A deterministic fault injected into one of the store's publications.
///
/// Each variant names a boundary inside a publication rather than a wall-clock
/// moment, so a crash test reproduces an exact byte offset instead of racing
/// for one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteFault {
    /// Fail before the stale slot is opened.
    ///
    /// The stale slot keeps whatever earlier image it held, which is the case
    /// that proves recovery orders by generation rather than by recency.
    BeforeFirstByte,
    /// Emit the first `bytes` bytes of the unsealed image, make them durable,
    /// then fail.
    ///
    /// The emitted prefix is synced deliberately. The interesting recovery case
    /// is the one where a partial write did reach the medium, and a test that
    /// left the prefix in a write-back cache would be proving something weaker
    /// than it claims. `AfterBytes(0)` writes nothing, so the stale slot keeps
    /// the earlier image it held.
    AfterBytes(u64),
    /// Emit every byte of the unsealed image, then fail its durability barrier.
    AtSlotSync,
    /// Make the whole unsealed image durable, then fail before it is sealed.
    ///
    /// This is the window the seal exists to make representable: every byte of
    /// the new image is on the medium and none of it counts.
    BeforeSeal,
    /// Seal the image, then fail the barrier that makes the seal durable.
    ///
    /// Either outcome is legal — the seal may or may not have reached the
    /// medium — which is exactly why the caller is told the result is unknown.
    AtSealSync,
}

impl fmt::Display for WriteFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeFirstByte => formatter.write_str("failure before the first byte"),
            Self::AfterBytes(bytes) => write!(formatter, "failure after {bytes} bytes"),
            Self::AtSlotSync => formatter.write_str("failure at the slot sync"),
            Self::BeforeSeal => formatter.write_str("failure before the seal"),
            Self::AtSealSync => formatter.write_str("failure at the seal sync"),
        }
    }
}

/// Deterministic fault schedule attached to one store instance.
///
/// Injection is part of a store's construction rather than a process-wide
/// switch: a plan travels with the handle it was built for, so two stores in
/// one test — or two tests in one process — cannot observe each other's faults.
///
/// Plans are addressed by publication ordinal. Every publication the store
/// performs, whether an apply commit or a snapshot install, consumes the next
/// ordinal starting at one, so a scenario names the exact transaction it means
/// to interrupt.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FaultPlan {
    faults: Vec<(u64, WriteFault)>,
}

impl FaultPlan {
    /// A plan that injects nothing.
    #[must_use]
    pub const fn none() -> Self {
        Self { faults: Vec::new() }
    }

    /// A plan that injects `fault` on the `publication`-th publication.
    #[must_use]
    pub fn at(publication: u64, fault: WriteFault) -> Self {
        Self::none().and(publication, fault)
    }

    /// Adds another injection to this plan.
    #[must_use]
    pub fn and(mut self, publication: u64, fault: WriteFault) -> Self {
        self.faults.push((publication, fault));
        self
    }

    fn fault_for(&self, publication: u64) -> Option<WriteFault> {
        self.faults
            .iter()
            .find(|(ordinal, _)| *ordinal == publication)
            .map(|(_, fault)| *fault)
    }
}

impl fmt::Display for FaultPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.faults.is_empty() {
            return formatter.write_str("no injected faults");
        }
        let mut separator = "";
        for (publication, fault) in &self.faults {
            write!(formatter, "{separator}publication {publication}: {fault}")?;
            separator = ", ";
        }
        Ok(())
    }
}

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
    /// A publication wrote these bytes and never sealed them.
    ///
    /// The slot still carries [`UNSEALED_MARK`] in its first byte, which a
    /// publication writes before any other byte and replaces only after every
    /// other byte is durable. This is the only damage recovery may skip, and
    /// `present` is the byte boundary the interrupted write reached.
    UnsealedPublication {
        /// Bytes present in the slot.
        present: u64,
    },
    /// A sealed slot holds fewer bytes than one slot header needs.
    HeaderIncomplete {
        /// Bytes present in the slot.
        present: u64,
    },
    /// The slot does not begin with this store's magic.
    ///
    /// The mark this store writes into byte zero is part of that magic, so this
    /// also covers a slot whose first byte is neither [`SEALED_MARK`] nor
    /// [`UNSEALED_MARK`].
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
    /// it is residue — so it is the converse that has to hold: this returning
    /// `true` must prove the slot was never the live image. One variant carries
    /// that proof, and it carries it because a publication puts the proof in
    /// the bytes rather than leaving recovery to infer it.
    ///
    /// A publication writes [`UNSEALED_MARK`] into byte zero before any other
    /// byte of the image, and replaces it with [`SEALED_MARK`] only after every
    /// other byte has passed a durability barrier. A crash leaves a prefix of
    /// what was written — that is the crash contract this whole file rests on —
    /// so byte zero is written first and every interrupted publication leaves
    /// the unsealed mark behind. Contrapositively, a slot whose first byte is
    /// sealed holds exactly the bytes some completed publication sealed there,
    /// and any damage to it happened *after* that seal.
    ///
    /// So [`SlotDamage::UnsealedPublication`] proves the slot was the one being
    /// written and was never adopted by any opener, which is what makes
    /// skipping it safe. Every other damage — including a sealed image that has
    /// merely lost its last byte, which is a strict prefix of an image this
    /// build wrote and would have passed a shape test — proves nothing of the
    /// kind. Recovery cannot rule out that such a slot held the newest
    /// committed state, so it refuses to open instead.
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
    created: bool,
    slots: [SlotState; 2],
    live_slot: Option<SlotIndex>,
    cross_checked_marks: bool,
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

    /// Whether recovery found a second intact slot and re-checked the fencing
    /// high-water marks across the commit boundary between them.
    #[must_use]
    pub const fn cross_checked_marks(&self) -> bool {
        self.cross_checked_marks
    }
}

/// Whether this handle may still publish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Health {
    Healthy,
    ReopenRequired,
}

/// A durable, transactional lock store over two alternating slot files.
///
/// See the [module documentation](self) for the format, the crash contract, and
/// the argument for this shape.
#[derive(Debug)]
pub struct LockStore {
    directory: PathBuf,
    config: LockConfig,
    service: LockService,
    applied_index: LogIndex,
    generation: u64,
    live_slot: Option<SlotIndex>,
    /// Every fencing high-water mark this store has durably acknowledged.
    acknowledged_marks: BTreeMap<ResourceName, FencingToken>,
    health: Health,
    faults: FaultPlan,
    /// Publications this handle has started, which is what [`FaultPlan`] keys on.
    publications: u64,
    fired_fault: Option<WriteFault>,
    recovery: RecoveryReport,
}

impl LockStore {
    /// Opens the store in `directory`, creating and recovering as needed.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory or a slot cannot be read, when no
    /// slot holds a readable image, when a slot was written under different
    /// resource bounds, when a sealed image is malformed or violates a lock
    /// service invariant, or when the two slots disagree about generation
    /// ordering or fencing high-water marks.
    pub fn open(directory: &Path, config: LockConfig) -> Result<Self, LockStoreError> {
        Self::open_with_faults(directory, config, FaultPlan::none())
    }

    /// Opens the store with a deterministic fault schedule.
    ///
    /// This is the crash-test construction described on [`FaultPlan`]. A store
    /// opened with [`FaultPlan::none`] behaves exactly as [`LockStore::open`].
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`LockStore::open`]. Faults apply to
    /// publications, so opening never injects one.
    pub fn open_with_faults(
        directory: &Path,
        config: LockConfig,
        faults: FaultPlan,
    ) -> Result<Self, LockStoreError> {
        fs::create_dir_all(directory).map_err(|source| LockStoreError::Io {
            operation: "create the lock store directory",
            path: directory.to_path_buf(),
            source,
        })?;

        let created = establish_slot_files(directory)?;

        let mut states = [SlotState::Empty; 2];
        let mut images: [Option<DecodedImage>; 2] = [None, None];
        for slot in [SlotIndex::Zero, SlotIndex::One] {
            let bytes = read_slot(directory, slot)?;
            match verify_slot(&bytes) {
                Ok(None) => states[slot.position()] = SlotState::Empty,
                Ok(Some(sealed)) => {
                    let image = decode_image(slot, &sealed, config)?;
                    states[slot.position()] = SlotState::Intact {
                        generation: sealed.generation,
                        applied_index: image.applied_index,
                    };
                    images[slot.position()] = Some(image);
                }
                Err(damage) => states[slot.position()] = SlotState::Damaged(damage),
            }
        }

        let adopted = choose_live_slot(&states, images)?;
        let (service, applied_index, generation, live_slot, cross_checked_marks) = match adopted {
            Some(adopted) => (
                adopted.image.service,
                adopted.image.applied_index,
                adopted.generation,
                Some(adopted.slot),
                adopted.cross_checked_marks,
            ),
            None => (LockService::new(config), LogIndex::ZERO, 0, None, false),
        };
        let acknowledged_marks = marks_of(&service);

        Ok(Self {
            directory: directory.to_path_buf(),
            config,
            service,
            applied_index,
            generation,
            live_slot,
            acknowledged_marks,
            health: Health::Healthy,
            faults,
            publications: 0,
            fired_fault: None,
            recovery: RecoveryReport {
                created,
                slots: states,
                live_slot,
                cross_checked_marks,
            },
        })
    }

    /// Returns what opening this store found and did.
    #[must_use]
    pub const fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// Returns the resource bounds this store's slots were written under.
    #[must_use]
    pub const fn config(&self) -> LockConfig {
        self.config
    }

    /// Returns the durable lock service state.
    #[must_use]
    pub const fn service(&self) -> &LockService {
        &self.service
    }

    /// Returns the durable applied Raft index.
    #[must_use]
    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    /// Returns the publication generation of the live image.
    ///
    /// Zero means no publication has committed.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the slot the live image occupies, if one has committed.
    #[must_use]
    pub const fn live_slot(&self) -> Option<SlotIndex> {
        self.live_slot
    }

    /// Returns the slot the next publication will write.
    #[must_use]
    pub const fn next_slot(&self) -> SlotIndex {
        match self.live_slot {
            Some(slot) => slot.other(),
            None => SlotIndex::Zero,
        }
    }

    /// Returns the fencing high-water mark this store has durably acknowledged
    /// for `resource`.
    ///
    /// This is the value the whole design exists to protect: no state this
    /// store publishes or adopts may ever carry a lower one.
    #[must_use]
    pub fn acknowledged_mark(&self, resource: ResourceName) -> Option<FencingToken> {
        self.acknowledged_marks.get(&resource).copied()
    }

    /// Whether an earlier write poisoned this handle.
    #[must_use]
    pub const fn requires_reopen(&self) -> bool {
        matches!(self.health, Health::ReopenRequired)
    }

    /// Returns the injected fault that fired on this handle, if any.
    ///
    /// A crash test asserts on this the way a failpoint scenario asserts that
    /// its guard triggered: a plan that never fired proves nothing, and a suite
    /// of such plans would pass while testing an uninterrupted store.
    #[must_use]
    pub const fn fired_fault(&self) -> Option<WriteFault> {
        self.fired_fault
    }

    /// Returns the byte length one publication of `service` at `applied_index`
    /// would write.
    ///
    /// Crash tests sweep every boundary inside that length, so they need it
    /// before they arm the fault that stops inside it.
    ///
    /// The generation is a fixed-width header field, so the length does not
    /// depend on which one a publication would assign and this does not need to
    /// know. That is why it can be answered without a store.
    ///
    /// # Errors
    ///
    /// Returns an error when the image cannot be encoded or does not fit the
    /// slot header's length field.
    pub fn planned_image_len(
        config: LockConfig,
        service: &LockService,
        applied_index: LogIndex,
    ) -> Result<u64, LockStoreError> {
        Ok(as_u64(
            encode_image(config, service, applied_index, 1)?.len(),
        ))
    }

    /// Commits one transaction, publishing it into the stale slot.
    ///
    /// The transaction carries the whole application state — the lock table,
    /// every high-water mark, sessions with their cached operations,
    /// fingerprints, and results, and the replicated logical time — together
    /// with `applied_index`. `Ok` means every one of them is durable; nothing
    /// partial is ever recoverable.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle is poisoned, when `applied_index` does
    /// not advance, when the state would lower a fencing high-water mark, when
    /// the image cannot be encoded, or when the write or its durability barrier
    /// fails. After any of the latter the handle is poisoned and the caller must
    /// reopen to learn what committed.
    pub fn commit(
        &mut self,
        service: &LockService,
        applied_index: LogIndex,
    ) -> Result<(), LockStoreError> {
        self.check_health()?;
        // A commit must advance the floor. A batch that applied nothing never
        // reaches here, so an index that does not advance is a caller error
        // rather than a no-op.
        if applied_index <= self.applied_index {
            return Err(LockStoreError::AppliedIndexRegression {
                previous: self.applied_index,
                found: applied_index,
            });
        }
        self.publish(service, applied_index)
    }

    /// Publishes an installed snapshot's state into the stale slot.
    ///
    /// Unlike [`LockStore::commit`] this accepts an `applied_index` equal to
    /// the current one, because installing the state a replica already holds
    /// must not require inventing a new index. It is otherwise the same
    /// publication, byte for byte and crash window for crash window.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle is poisoned, when `applied_index` would
    /// move the applied floor backwards, when the state would lower a fencing
    /// high-water mark, when republishing an unchanged applied index would move
    /// a session cache backwards, when the image cannot be encoded, or when the
    /// write or its durability barrier fails.
    pub fn install(
        &mut self,
        service: &LockService,
        applied_index: LogIndex,
    ) -> Result<(), LockStoreError> {
        self.check_health()?;
        if applied_index < self.applied_index {
            return Err(LockStoreError::AppliedIndexRegression {
                previous: self.applied_index,
                found: applied_index,
            });
        }
        if applied_index == self.applied_index {
            // The one publication the applied floor cannot judge. Two images at
            // one index can still disagree about which requests have completed,
            // and the poorer one makes an acknowledged acquisition executable
            // again — a second fencing token for one tenure. Above this index
            // the model is the authority on the sessions it retired along the
            // way, so the check is scoped to exactly here.
            verify_session_cache_dominates(
                &session_progress_of(&self.service),
                &session_progress_of(service),
            )?;
        }
        self.publish(service, applied_index)
    }

    /// The one write path: check, encode, write the stale slot, sync, adopt.
    fn publish(
        &mut self,
        service: &LockService,
        applied_index: LogIndex,
    ) -> Result<(), LockStoreError> {
        let proposed = marks_of(service);
        // Refused before a byte is written: a state that loses a mark is not
        // one this store will make durable, whatever produced it.
        verify_marks_dominate(&self.acknowledged_marks, &proposed)?;

        let generation = self.generation + 1;
        let slot = self.next_slot();
        let image = encode_image(self.config, service, applied_index, generation)?;
        let publication = self.begin_publication();
        self.write_slot(slot, &image, publication)?;

        self.service = service.clone();
        self.applied_index = applied_index;
        self.generation = generation;
        self.live_slot = Some(slot);
        self.acknowledged_marks = proposed;
        Ok(())
    }

    fn check_health(&self) -> Result<(), LockStoreError> {
        if self.requires_reopen() {
            return Err(LockStoreError::StoreRequiresReopen);
        }
        Ok(())
    }

    /// Allocates the next publication ordinal.
    fn begin_publication(&mut self) -> u64 {
        self.publications += 1;
        self.publications
    }

    /// Records that a publication failed, poisoning the handle.
    fn publication_failed(&mut self, error: LockStoreError) -> LockStoreError {
        self.health = Health::ReopenRequired;
        error
    }

    /// Takes the fault armed for `publication`, if any, remembering that it
    /// fired.
    fn take_fault(&mut self, publication: u64, at: WriteFaultSite) -> Option<LockStoreError> {
        let fault = self.faults.fault_for(publication)?;
        if !at.matches(fault) {
            return None;
        }
        self.fired_fault = Some(fault);
        Some(LockStoreError::InjectedFault { fault, publication })
    }

    /// Writes `image` into the stale slot unsealed, makes it durable, and only
    /// then seals it.
    ///
    /// The slot being written is never the authoritative one, so every failure
    /// below leaves the live image whole. The directory entry was made durable
    /// at open, so nothing here touches the directory.
    ///
    /// Two things about the order are load bearing, and both exist to make
    /// recovery's skip rule provable rather than plausible:
    ///
    /// 1. **Byte zero goes out first, and goes out unsealed.** A crash leaves a
    ///    prefix of what was written, so every interrupted publication leaves
    ///    [`UNSEALED_MARK`] in the slot's first byte. That is what a later
    ///    opener reads as proof that this image was never the live one.
    /// 2. **The seal is one byte, written after the barrier below returned.**
    ///    Nothing that follows the barrier can reach the medium before the
    ///    image it seals, and a single byte cannot be half written, so a slot
    ///    is either sealed or not.
    ///
    /// The slot is cut back to the new image's length before the barrier rather
    /// than truncated to nothing before the first byte. Truncating first would
    /// leave an empty file in the crash window, and an empty file is the one
    /// artifact this store must be able to call damage — see
    /// [`SlotDamage::SlotEmptied`].
    fn write_slot(
        &mut self,
        slot: SlotIndex,
        image: &[u8],
        publication: u64,
    ) -> Result<(), LockStoreError> {
        if let Some(error) = self.take_fault(publication, WriteFaultSite::BeforeFirstByte) {
            return Err(self.publication_failed(error));
        }

        let path = slot_path(&self.directory, slot);
        let mut file = OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|source| {
                self.publication_failed(LockStoreError::Io {
                    operation: "open a lock store slot for publication",
                    path: path.clone(),
                    source,
                })
            })?;

        let mut unsealed = image.to_vec();
        unsealed[0] = UNSEALED_MARK;
        let emitted = self.emit(&mut file, &unsealed, publication, &path)?;
        if emitted < image.len() {
            let error = self
                .take_fault(publication, WriteFaultSite::AfterBytes)
                .expect("a short emit only happens when a byte-boundary fault fired");
            return Err(self.publication_failed(error));
        }

        // Cutting back is what keeps a shorter image from inheriting a longer
        // one's tail. It is safe precisely because this slot is the stale one,
        // and it happens while the slot is still marked unsealed.
        file.set_len(as_u64(image.len())).map_err(|source| {
            self.publication_failed(LockStoreError::Io {
                operation: "resize a lock store slot for publication",
                path: path.clone(),
                source,
            })
        })?;

        if let Some(error) = self.take_fault(publication, WriteFaultSite::AtSlotSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LockStoreError::Io {
                operation: "sync a published lock store slot",
                path: path.clone(),
                source,
            })
        })?;

        self.seal_slot(&mut file, publication, &path)
    }

    /// Replaces a written slot's unsealed mark with the sealed one.
    ///
    /// This is the commit point. Everything before it is a slot a later opener
    /// may skip; everything after it is a slot a later opener must be able to
    /// read or refuse over.
    fn seal_slot(
        &mut self,
        file: &mut File,
        publication: u64,
        path: &Path,
    ) -> Result<(), LockStoreError> {
        if let Some(error) = self.take_fault(publication, WriteFaultSite::BeforeSeal) {
            return Err(self.publication_failed(error));
        }
        file.seek(SeekFrom::Start(0))
            .and_then(|_| file.write_all(&[SEALED_MARK]))
            .map_err(|source| {
                self.publication_failed(LockStoreError::Io {
                    operation: "seal a published lock store slot",
                    path: path.to_path_buf(),
                    source,
                })
            })?;

        if let Some(error) = self.take_fault(publication, WriteFaultSite::AtSealSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LockStoreError::Io {
                operation: "sync a sealed lock store slot",
                path: path.to_path_buf(),
                source,
            })
        })
    }

    /// Writes `bytes` to `file`, honoring a byte-boundary fault.
    ///
    /// Returns how many bytes were emitted; a short return means a fault
    /// stopped the publication, and the prefix was synced so recovery meets the
    /// worst case where it reached the medium.
    fn emit(
        &mut self,
        file: &mut File,
        bytes: &[u8],
        publication: u64,
        path: &Path,
    ) -> Result<usize, LockStoreError> {
        let limit = match self.faults.fault_for(publication) {
            Some(WriteFault::AfterBytes(stop)) => {
                usize::try_from(stop).unwrap_or(usize::MAX).min(bytes.len())
            }
            _ => bytes.len(),
        };

        file.write_all(&bytes[..limit]).map_err(|source| {
            self.publication_failed(LockStoreError::Io {
                operation: "write a lock store slot",
                path: path.to_path_buf(),
                source,
            })
        })?;
        if limit < bytes.len() {
            file.sync_data().map_err(|source| {
                self.publication_failed(LockStoreError::Io {
                    operation: "sync an interrupted lock store write",
                    path: path.to_path_buf(),
                    source,
                })
            })?;
        }
        Ok(limit)
    }
}

/// The step a fault is armed at.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteFaultSite {
    BeforeFirstByte,
    AfterBytes,
    AtSlotSync,
    BeforeSeal,
    AtSealSync,
}

impl WriteFaultSite {
    const fn matches(self, fault: WriteFault) -> bool {
        matches!(
            (self, fault),
            (Self::BeforeFirstByte, WriteFault::BeforeFirstByte)
                | (Self::AfterBytes, WriteFault::AfterBytes(_))
                | (Self::AtSlotSync, WriteFault::AtSlotSync)
                | (Self::BeforeSeal, WriteFault::BeforeSeal)
                | (Self::AtSealSync, WriteFault::AtSealSync)
        )
    }
}

/// A slot whose header, payload, and trailing checksum all verified.
struct SealedImage<'a> {
    generation: u64,
    applied_index: LogIndex,
    max_clients: u32,
    max_resources: u32,
    payload: &'a [u8],
}

/// A sealed slot's payload, restored through the model's validating path.
struct DecodedImage {
    service: LockService,
    applied_index: LogIndex,
}

/// The slot recovery adopted.
struct AdoptedImage {
    slot: SlotIndex,
    generation: u64,
    image: DecodedImage,
    cross_checked_marks: bool,
}

/// Returns the four bytes where a slot's magic belongs, zero-padded when the
/// slot is shorter than that.
fn magic_of(bytes: &[u8]) -> [u8; 4] {
    let mut magic = [0_u8; 4];
    let present = bytes.len().min(magic.len());
    magic[..present].copy_from_slice(&bytes[..present]);
    magic
}

/// Checks that the bytes present are ones this store and this build wrote, as
/// far as they go.
///
/// This runs on **every** slot long enough to carry a field, before anything
/// classifies the slot by its length. That ordering is the point: the older
/// shape put the magic and version tests behind a full-header slice, so a short
/// slot was attributed to this build rather than shown to belong to it, and
/// twenty bytes of a foreign format were read as this build's own residue.
///
/// Byte zero is the publication mark and is checked here too, because it is the
/// magic's leading byte: a slot that begins with neither mark is not a slot
/// this build ever wrote.
fn verify_identity(bytes: &[u8]) -> Result<(), SlotDamage> {
    if bytes[0] != UNSEALED_MARK && bytes[0] != SEALED_MARK {
        return Err(SlotDamage::NotALockImage {
            magic: magic_of(bytes),
        });
    }
    let present = bytes.len().min(SLOT_MAGIC.len());
    if bytes[1..present] != SLOT_MAGIC[1..present] {
        return Err(SlotDamage::NotALockImage {
            magic: magic_of(bytes),
        });
    }
    if let Some(version) = bytes.get(4) {
        if *version != SLOT_FORMAT_VERSION {
            return Err(SlotDamage::UnsupportedFormatVersion { version: *version });
        }
    }
    Ok(())
}

/// Verifies one slot's bytes, returning its sealed image or the damage found.
///
/// `Ok(None)` means the slot carries its creation mark and nothing has ever
/// been sealed into it, which is not damage. A slot of zero bytes is not that
/// case: creation writes the mark, so an empty file is something else's doing.
fn verify_slot(bytes: &[u8]) -> Result<Option<SealedImage<'_>>, SlotDamage> {
    if bytes.is_empty() {
        return Err(SlotDamage::SlotEmptied);
    }
    if bytes == CREATION_MARK {
        return Ok(None);
    }
    // Identity first, at every length, then the seal, and only then anything
    // that depends on how many bytes are present.
    verify_identity(bytes)?;
    if bytes[0] == UNSEALED_MARK {
        return Err(SlotDamage::UnsealedPublication {
            present: as_u64(bytes.len()),
        });
    }

    // Sealed from here down. Every failure below is damage to bytes some
    // completed publication sealed, so none of it is residue to skip.
    let Some(header) = bytes.get(..SLOT_HEADER_LEN) else {
        return Err(SlotDamage::HeaderIncomplete {
            present: as_u64(bytes.len()),
        });
    };
    let declared_header_crc = read_u32(&header[HEADER_CHECKSUM_OFFSET..SLOT_HEADER_LEN]);
    let computed_header_crc = crc32(&header[..HEADER_CHECKSUM_OFFSET]);
    if declared_header_crc != computed_header_crc {
        return Err(SlotDamage::HeaderChecksumMismatch {
            declared: declared_header_crc,
            computed: computed_header_crc,
        });
    }

    // Only now is `payload_len` trustworthy enough to locate the trailer.
    let payload_len = read_u32(&header[29..33]) as usize;
    let Some(payload) = bytes.get(SLOT_HEADER_LEN..SLOT_HEADER_LEN + payload_len) else {
        return Err(SlotDamage::PayloadIncomplete {
            declared: as_u64(payload_len),
            present: as_u64(bytes.len() - SLOT_HEADER_LEN),
        });
    };

    let trailer_start = SLOT_HEADER_LEN + payload_len;
    let available = bytes.len() - trailer_start;
    if available == 0 {
        return Err(SlotDamage::MissingCommitChecksum);
    }
    let Some(trailer) = bytes.get(trailer_start..trailer_start + SLOT_TRAILER_LEN) else {
        return Err(SlotDamage::PartialCommitChecksum {
            present: as_u64(available),
        });
    };
    let declared_commit_crc = read_u32(trailer);
    let computed_commit_crc = crc32(&bytes[..trailer_start]);
    if declared_commit_crc != computed_commit_crc {
        return Err(SlotDamage::CommitChecksumMismatch {
            declared: declared_commit_crc,
            computed: computed_commit_crc,
        });
    }
    let sealed_len = trailer_start + SLOT_TRAILER_LEN;
    if bytes.len() > sealed_len {
        return Err(SlotDamage::TrailingBytes {
            extra: as_u64(bytes.len() - sealed_len),
        });
    }

    Ok(Some(SealedImage {
        generation: read_u64(&header[5..13]),
        applied_index: LogIndex(read_u64(
            &header[HEADER_APPLIED_INDEX_OFFSET..HEADER_APPLIED_INDEX_OFFSET + 8],
        )),
        max_clients: read_u32(&header[21..25]),
        max_resources: read_u32(&header[25..29]),
        payload,
    }))
}

/// Restores a sealed slot's payload through the model's own validating path.
fn decode_image(
    slot: SlotIndex,
    sealed: &SealedImage<'_>,
    config: LockConfig,
) -> Result<DecodedImage, LockStoreError> {
    if sealed.max_clients != config.max_clients() || sealed.max_resources != config.max_resources()
    {
        return Err(LockStoreError::ConfigMismatch {
            slot,
            image_max_clients: sealed.max_clients,
            image_max_resources: sealed.max_resources,
            requested_max_clients: config.max_clients(),
            requested_max_resources: config.max_resources(),
        });
    }

    let (payload_index, snapshot) =
        decode_snapshot(sealed.payload).map_err(|source| LockStoreError::Image { slot, source })?;
    let payload_index = LogIndex(payload_index);
    if payload_index != sealed.applied_index {
        return Err(LockStoreError::AppliedIndexDisagreement {
            slot,
            header_index: sealed.applied_index,
            payload_index,
        });
    }

    // The model decides whether these parts describe a legal service. A slot
    // whose checksums verify still cannot produce a state that breaks the
    // expiry invariant or the held-token/high-water-mark equality.
    let service = LockService::from_snapshot(config, snapshot)
        .map_err(|source| LockStoreError::Snapshot { slot, source })?;
    Ok(DecodedImage {
        service,
        applied_index: sealed.applied_index,
    })
}

/// Picks the slot recovery adopts, refusing any slot it cannot read.
///
/// Unreadability is settled *before* anything is chosen, because the question
/// the generation comparison answers — which image is newer — is exactly the
/// question a slot this build cannot read has already made unanswerable.
fn choose_live_slot(
    states: &[SlotState; 2],
    images: [Option<DecodedImage>; 2],
) -> Result<Option<AdoptedImage>, LockStoreError> {
    // Both damaged is reported as the stronger fact it is: there is no image at
    // all, whatever each slot's damage happened to be. A lock service that
    // cannot read any image cannot know its high-water marks, and opening empty
    // would reissue token 1 for a resource whose guarded downstream has already
    // accepted far more.
    if states.iter().all(|state| state.damage().is_some()) {
        return Err(LockStoreError::NoReadableImage { slots: *states });
    }
    // One damaged slot. Skipping it is safe only when its damage proves it was
    // the slot being written; anything else could be the live image, and
    // adopting the partner would roll an acknowledged mark back one generation.
    for slot in [SlotIndex::Zero, SlotIndex::One] {
        let Some(damage) = states[slot.position()].damage() else {
            continue;
        };
        if !damage.is_publication_residue() {
            return Err(LockStoreError::UnreadableSlot {
                slot,
                damage,
                other: states[slot.other().position()],
            });
        }
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
            }))
        }
        // One committed image beside a slot holding none. Reaching here is a
        // proof rather than a default: the partner is empty, or it holds
        // residue, and residue means the partner was the slot being written —
        // so whatever it last committed was older than what is adopted here.
        // There is no second committed image to compare against, and
        // `cross_checked_marks` says so rather than implying a check ran.
        (Some(generation), None, Some(image), _) => Ok(Some(AdoptedImage {
            slot: SlotIndex::Zero,
            generation,
            image,
            cross_checked_marks: false,
        })),
        (None, Some(generation), _, Some(image)) => Ok(Some(AdoptedImage {
            slot: SlotIndex::One,
            generation,
            image,
            cross_checked_marks: false,
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

/// Returns every tracked resource's fencing high-water mark.
fn marks_of(service: &LockService) -> BTreeMap<ResourceName, FencingToken> {
    service
        .view()
        .resources
        .into_iter()
        .map(|resource| (resource.resource, resource.token_floor))
        .collect()
}

/// Returns every client slot's session progress.
fn session_progress_of(service: &LockService) -> BTreeMap<ClientId, SessionProgress> {
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
fn verify_session_cache_dominates(
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

/// Refuses a state that would lower or drop any acknowledged mark.
///
/// A resource that disappears is the same failure as one whose mark decreases:
/// both let a later acquisition reissue a token a guarded resource has accepted.
fn verify_marks_dominate(
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

/// Encodes one whole slot image: header, payload, and the trailer that seals
/// them.
fn encode_image(
    config: LockConfig,
    service: &LockService,
    applied_index: LogIndex,
    generation: u64,
) -> Result<Vec<u8>, LockStoreError> {
    let payload = encode_snapshot(applied_index.0, &service.snapshot()).map_err(|source| {
        LockStoreError::Image {
            slot: SlotIndex::Zero,
            source,
        }
    })?;
    let payload_len = u32::try_from(payload.len()).map_err(|_| LockStoreError::ImageTooLarge {
        length: as_u64(payload.len()),
    })?;

    let mut image = Vec::with_capacity(SLOT_HEADER_LEN + payload.len() + SLOT_TRAILER_LEN);
    image.extend_from_slice(&SLOT_MAGIC);
    image.push(SLOT_FORMAT_VERSION);
    image.extend_from_slice(&generation.to_be_bytes());
    image.extend_from_slice(&applied_index.0.to_be_bytes());
    image.extend_from_slice(&config.max_clients().to_be_bytes());
    image.extend_from_slice(&config.max_resources().to_be_bytes());
    image.extend_from_slice(&payload_len.to_be_bytes());
    let header_checksum = crc32(&image);
    image.extend_from_slice(&header_checksum.to_be_bytes());

    image.extend_from_slice(&payload);
    let commit_checksum = crc32(&image);
    image.extend_from_slice(&commit_checksum.to_be_bytes());
    Ok(image)
}

/// Brings both slot files into existence, or refuses, and says whether this
/// open created them.
///
/// Creating a store is not the same act as opening one, and the difference is
/// invisible from inside a directory that has lost a file. So creation is
/// allowed in exactly two shapes, and both are provable:
///
/// - **Neither slot exists.** There is no store here, and making one loses
///   nothing.
/// - **Slot zero exists, carries only its creation mark, and slot one does
///   not exist.** A creation was interrupted between its two files, and
///   finishing it loses nothing.
///
/// The second shape is narrower than "one slot is missing and the other has
/// never been published to", and the difference is the whole point. Creation
/// writes slot zero first and publications write slot zero first, so slot zero
/// carrying only its creation mark proves no publication ever committed
/// *anywhere*. The mirror statement is false: slot one carrying only its
/// creation mark proves only that generation two never landed, and slot zero —
/// the one that is missing — is exactly where generation one would have been.
/// Accepting that shape would recreate the store's first committed generation
/// as an empty file and report a clean opening.
///
/// Anything else is [`LockStoreError::MissingSlot`]. A slot that should exist
/// and does not is unreadable rather than absent, and re-creating it would open
/// the store one generation back with nothing reported.
fn establish_slot_files(directory: &Path) -> Result<bool, LockStoreError> {
    let present = [
        slot_path(directory, SlotIndex::Zero).exists(),
        slot_path(directory, SlotIndex::One).exists(),
    ];

    match present {
        [true, true] => return Ok(false),
        [false, false] => {}
        // Slot zero survives, so the interrupted-creation reading is available
        // if its bytes support it.
        [true, false] => {
            let bytes = read_slot(directory, SlotIndex::Zero)?;
            if !matches!(verify_slot(&bytes), Ok(None)) {
                return Err(LockStoreError::MissingSlot {
                    slot: SlotIndex::One,
                    other: slot_state(&bytes),
                });
            }
        }
        // Slot zero is the one that is gone, and it is the slot the first
        // publication writes. Whatever slot one holds, it cannot speak for it.
        [false, true] => {
            let bytes = read_slot(directory, SlotIndex::One)?;
            return Err(LockStoreError::MissingSlot {
                slot: SlotIndex::Zero,
                other: slot_state(&bytes),
            });
        }
    }

    // Slot zero is created first, so the only state an interrupted creation can
    // leave is the one accepted above. Both entries are made durable with one
    // directory sync, and every later publication is a pure content rewrite, so
    // no publication ever has a directory-entry crash window.
    for slot in [SlotIndex::Zero, SlotIndex::One] {
        if !present[slot.position()] {
            create_slot(directory, slot)?;
        }
    }
    sync_directory(directory)?;
    Ok(true)
}

/// Creates one slot file carrying its creation mark.
///
/// The mark is what separates a created slot from an emptied one. A file of
/// zero bytes is not a state this store leaves behind at any point in its life,
/// so meeting one is damage rather than the ordinary state of a store that has
/// never committed.
fn create_slot(directory: &Path, slot: SlotIndex) -> Result<(), LockStoreError> {
    let path = slot_path(directory, slot);
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)
        .map_err(|source| LockStoreError::Io {
            operation: "create a lock store slot",
            path: path.clone(),
            source,
        })?;
    file.write_all(&CREATION_MARK)
        .and_then(|()| file.sync_data())
        .map_err(|source| LockStoreError::Io {
            operation: "mark a created lock store slot",
            path,
            source,
        })
}

/// Summarizes one slot's bytes without decoding its payload.
fn slot_state(bytes: &[u8]) -> SlotState {
    match verify_slot(bytes) {
        Ok(None) => SlotState::Empty,
        Ok(Some(sealed)) => SlotState::Intact {
            generation: sealed.generation,
            applied_index: sealed.applied_index,
        },
        Err(damage) => SlotState::Damaged(damage),
    }
}

fn read_slot(directory: &Path, slot: SlotIndex) -> Result<Vec<u8>, LockStoreError> {
    let path = slot_path(directory, slot);
    fs::read(&path).map_err(|source| LockStoreError::Io {
        operation: "read a lock store slot",
        path,
        source,
    })
}

/// Makes a directory's entries durable.
fn sync_directory(directory: &Path) -> Result<(), LockStoreError> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|source| LockStoreError::Io {
            operation: "sync the lock store directory",
            path: directory.to_path_buf(),
            source,
        })
}

fn slot_path(directory: &Path, slot: SlotIndex) -> PathBuf {
    directory.join(slot.file_name())
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("callers pass four bytes"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("callers pass eight bytes"))
}

fn as_u64(value: usize) -> u64 {
    u64::try_from(value).expect("slot sizes fit a u64")
}

/// Direct access to a slot file's bytes, for crash tests only.
///
/// Everything here reaches past [`LockStore`] and reads or rewrites the artifact
/// the store owns. That is not a capability a durable application ever needs: an
/// application publishes through [`LockStore::commit`] and
/// [`LockStore::install`] and reads through [`LockStore::open`], and nothing
/// else in this crate calls into this module.
///
/// It is a named public module rather than a hidden item because the honest
/// statement is that these functions *are* reachable, and hiding them from the
/// rendered documentation would not change that. A `#[doc(hidden)]` function
/// sitting beside the store's own API reads at the call site exactly like API; a
/// call that has to name `raw_slot` says what it is doing every time it appears,
/// and greps for one word. The crate's dependency boundary forbids gating this
/// behind a feature or an internal hook — a consumer manifest must resolve like
/// an external user's — so the guard is the name, this paragraph, and review.
///
/// Nothing here validates anything. The store's own checks are what a forged
/// artifact is aimed at, so a caller is responsible for the bytes being the ones
/// it means to present.
pub mod raw_slot {
    use super::{
        crc32, slot_path, File, LockStoreError, LogIndex, OpenOptions, Path, Read, SlotIndex,
        Write, HEADER_APPLIED_INDEX_OFFSET, HEADER_CHECKSUM_OFFSET, SLOT_HEADER_LEN,
        SLOT_TRAILER_LEN,
    };

    /// Reads one slot's raw bytes.
    ///
    /// Crash tests corrupt sealed images to prove the checksums are load
    /// bearing, which needs the exact bytes the store wrote.
    ///
    /// # Errors
    ///
    /// Returns an error when the slot cannot be read.
    pub fn read(directory: &Path, slot: SlotIndex) -> Result<Vec<u8>, LockStoreError> {
        let path = slot_path(directory, slot);
        let mut bytes = Vec::new();
        File::open(&path)
            .and_then(|mut file| file.read_to_end(&mut bytes))
            .map_err(|source| LockStoreError::Io {
                operation: "read a lock store slot",
                path,
                source,
            })?;
        Ok(bytes)
    }

    /// Overwrites one slot's raw bytes.
    ///
    /// This is the corruption half of [`read`].
    ///
    /// # Errors
    ///
    /// Returns an error when the slot cannot be written.
    pub fn write(directory: &Path, slot: SlotIndex, bytes: &[u8]) -> Result<(), LockStoreError> {
        let path = slot_path(directory, slot);
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .and_then(|mut file| file.write_all(bytes).and_then(|()| file.sync_all()))
            .map_err(|source| LockStoreError::Io {
                operation: "rewrite a lock store slot",
                path,
                source,
            })
    }

    /// Recomputes both checksums over a modified slot image.
    ///
    /// Several header fields — the generation, the declared bounds, the applied
    /// index — are protected by a checksum precisely so recovery can trust
    /// them, which means flipping one of them cannot reach the check that reads
    /// it: the checksum catches the flip first. Resealing is how a test
    /// presents a *well-formed* image that nonetheless says something a correct
    /// store would never write, so that the checks behind the checksums are
    /// reachable at all.
    ///
    /// # Panics
    ///
    /// Panics when `image` is too short to hold a header and a trailer, which
    /// is not an image any store wrote.
    #[must_use]
    pub fn reseal(mut image: Vec<u8>) -> Vec<u8> {
        assert!(
            image.len() >= SLOT_HEADER_LEN + SLOT_TRAILER_LEN,
            "a resealable image is at least a header and a trailer"
        );
        let header_checksum = crc32(&image[..HEADER_CHECKSUM_OFFSET]);
        image[HEADER_CHECKSUM_OFFSET..SLOT_HEADER_LEN]
            .copy_from_slice(&header_checksum.to_be_bytes());
        let sealed = image.len() - SLOT_TRAILER_LEN;
        let commit_checksum = crc32(&image[..sealed]);
        image[sealed..].copy_from_slice(&commit_checksum.to_be_bytes());
        image
    }

    /// Overwrites the applied Raft index a slot header declares, leaving the
    /// payload's own copy alone.
    ///
    /// The two copies exist so recovery can order and report without decoding,
    /// and they are cross-checked because a disagreement means the artifact is
    /// not what it says it is. A correct store cannot produce that disagreement
    /// — both copies come from one argument — so this is the only way to reach
    /// the check.
    ///
    /// # Panics
    ///
    /// Panics when `image` is too short to hold a header and a trailer.
    #[must_use]
    pub fn overwrite_header_applied_index(mut image: Vec<u8>, applied_index: LogIndex) -> Vec<u8> {
        assert!(
            image.len() >= SLOT_HEADER_LEN + SLOT_TRAILER_LEN,
            "a rewritable image is at least a header and a trailer"
        );
        image[HEADER_APPLIED_INDEX_OFFSET..HEADER_APPLIED_INDEX_OFFSET + 8]
            .copy_from_slice(&applied_index.0.to_be_bytes());
        reseal(image)
    }
}

#[cfg(test)]
mod tests {
    use super::{crc32, CRC32_POLYNOMIAL};

    /// A second implementation, deliberately written a different way.
    ///
    /// The store's own `crc32` folds four bits at a time through a table built
    /// from the polynomial; this one walks the message a single bit at a time
    /// and never builds a table. Agreement between two shapes is worth more
    /// than agreement between one shape and itself.
    fn reference_crc32(bytes: &[u8]) -> u32 {
        let mut state = u32::MAX;
        for byte in bytes {
            state ^= u32::from(*byte);
            for _ in 0..8 {
                state = if state & 1 == 1 {
                    (state >> 1) ^ CRC32_POLYNOMIAL
                } else {
                    state >> 1
                };
            }
        }
        !state
    }

    #[test]
    fn checksums_match_the_published_vector() {
        assert_eq!(
            crc32(b"123456789"),
            0xCBF4_3926,
            "the store's CRC-32 must be the standard IEEE one"
        );
        assert_eq!(crc32(&[]), 0, "the empty message checksums to zero");
    }

    #[test]
    fn checksums_agree_with_an_independently_written_implementation() {
        let mut sample = Vec::new();
        for length in 0..96_usize {
            sample.push(u8::try_from(length * 11 % 251).expect("the modulus fits a byte"));
            assert_eq!(
                crc32(&sample),
                reference_crc32(&sample),
                "the two implementations disagree at length {length}"
            );
        }
    }
}
