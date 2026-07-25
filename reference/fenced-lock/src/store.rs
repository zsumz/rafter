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
//! - a magic or version other than the one named here is rejected;
//! - checksums are CRC-32/IEEE, an accidental-corruption check and not an
//!   authentication tag; and
//! - a slot file of zero bytes has never been published to.
//!
//! ## Slot header (`RFLK`)
//!
//! The header has a fixed size of [`SLOT_HEADER_LEN`] bytes at offset zero.
//!
//! ```text
//! magic          [4]   "RFLK"
//! version        u8    1
//! generation     u64
//! applied_index  u64
//! max_clients    u32
//! max_resources  u32
//! payload_len    u32
//! crc32          u32
//! ```
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
//! [`SLOT_TRAILER_LEN`] bytes covering the header and the payload together.
//! **The trailer is the commit marker.** A slot counts only when its header
//! verifies, its payload is entirely present, and this checksum matches
//! everything before it. Covering the header as well as the payload is what
//! stops a payload from one generation being read under a header from another.
//!
//! # Crash contract
//!
//! The authoritative artifact is the pair of slot files. The logical commit
//! point of a publication is the return of the `sync_data` that follows its
//! last byte. `Ok` means the new state is what a fresh opener sees. `Err` means
//! the outcome is unknown, and reopening is the oracle that decides it — never
//! an inference that `Err` left no bytes changed.
//!
//! A crash at any byte boundary leaves the store recoverable to exactly the
//! pre-transaction or the post-transaction state, never between:
//!
//! - Before the stale slot is opened, both files are unchanged.
//! - After the stale slot is truncated and before its header is complete, that
//!   slot is empty or is a [`SlotDamage::HeaderIncomplete`] fragment. Either
//!   way it cannot be chosen, and the live slot still holds the pre-transaction
//!   state.
//! - Part-way through the payload, the slot is
//!   [`SlotDamage::PayloadIncomplete`].
//! - With the payload complete and no trailer, the image is written but not
//!   committed: [`SlotDamage::MissingCommitChecksum`]. This is the window a
//!   write-ahead design makes explicit in its layout and this one makes
//!   explicit in its trailer.
//! - Part-way through the trailer, [`SlotDamage::PartialCommitChecksum`].
//! - After the trailer's sync returns, the new slot is committed, outranks the
//!   old one by generation, and is what recovery adopts.
//!
//! Nothing is truncated or repaired at open. A damaged slot is simply not
//! eligible, and the next publication overwrites it, so the store heals itself
//! without a repair path that could run at the wrong moment.
//!
//! Recovery refuses to open when **both** slots are damaged, rather than
//! starting empty. A lock service that cannot read any image cannot know its
//! high-water marks, and one that started empty would hand out token 1 for a
//! resource whose guarded downstream has already accepted a far higher token.
//! Failing closed is the only safe answer. A store that has never committed is
//! a different case and is not refused: its slots are empty, not damaged.
//!
//! One residue is deliberately *not* refused, and it is worth naming because
//! the files cannot distinguish it. A publication that dies immediately after
//! truncating leaves its slot empty, so "one damaged slot beside an empty one"
//! could in principle mean either "the first publication ever tore" — where
//! opening empty is correct — or "the live slot was corrupted while the stale
//! one was mid-truncation", where it is not. The second needs a corruption of a
//! file this store never opened for writing, which is outside the crash model
//! above; the first is ordinary. Refusing both would make a store unopenable
//! whenever its very first transaction was interrupted, so the ordinary case
//! wins and the extraordinary one is stated here rather than silently handled.
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
    io::{Read, Write},
    path::{Path, PathBuf},
};

use rafter::LogIndex;

use crate::{
    adapter::{decode_snapshot, encode_snapshot},
    FencingToken, LockCodecError, LockConfig, LockService, ResourceName, SnapshotError,
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
            } => match offered {
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
            },
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

impl Error for LockStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Image { source, .. } => Some(source),
            Self::NoReadableImage { .. }
            | Self::AmbiguousGeneration { .. }
            | Self::ConfigMismatch { .. }
            | Self::Snapshot { .. }
            | Self::AppliedIndexDisagreement { .. }
            | Self::AppliedIndexRegression { .. }
            | Self::MarkRegression { .. }
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
    /// Truncate the stale slot, emit the first `bytes` bytes, make them
    /// durable, then fail.
    ///
    /// The emitted prefix is synced deliberately. The interesting recovery case
    /// is the one where a partial write did reach the medium, and a test that
    /// left the prefix in a write-back cache would be proving something weaker
    /// than it claims. `AfterBytes(0)` is the truncate-then-die case, which
    /// leaves the stale slot empty rather than damaged.
    AfterBytes(u64),
    /// Emit every byte of the image, then fail its durability barrier.
    AtSlotSync,
}

impl fmt::Display for WriteFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeFirstByte => formatter.write_str("failure before the first byte"),
            Self::AfterBytes(bytes) => write!(formatter, "failure after {bytes} bytes"),
            Self::AtSlotSync => formatter.write_str("failure at the slot sync"),
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

/// Why a slot could not be adopted.
///
/// A damaged slot is the normal residue of an interrupted publication, not a
/// fault, so it is reported through [`RecoveryReport`] rather than as a
/// [`LockStoreError`]. Each variant names the byte boundary the interrupted
/// write reached, which is what lets a crash test prove that its injection bit
/// where it aimed.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SlotDamage {
    /// Fewer bytes are present than one slot header needs.
    HeaderIncomplete {
        /// Bytes present in the slot.
        present: u64,
    },
    /// The slot does not begin with this store's magic.
    NotALockImage {
        /// The four bytes found where the magic belongs.
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

impl fmt::Display for SlotDamage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeaderIncomplete { present } => {
                write!(formatter, "an incomplete header of {present} bytes")
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
    /// The slot has never been published to.
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

    /// Returns the damage an interrupted publication left behind.
    ///
    /// At most one slot can be damaged in a store that opened: two damaged
    /// slots fail closed, so this is unambiguous.
    #[must_use]
    pub const fn damaged_slot(&self) -> Option<SlotDamage> {
        match self.slots[0].damage() {
            Some(damage) => Some(damage),
            None => self.slots[1].damage(),
        }
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

        // Both slots are created up front and the directory entry is made
        // durable once. Every later publication is a pure content rewrite, so
        // no publication ever has a directory-entry crash window.
        let mut created = false;
        for slot in [SlotIndex::Zero, SlotIndex::One] {
            created |= create_slot_if_absent(directory, slot)?;
        }
        if created {
            sync_directory(directory)?;
        }

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
    /// high-water mark, when the image cannot be encoded, or when the write or
    /// its durability barrier fails.
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

    /// Truncates the stale slot, writes `image` into it, and makes it durable.
    ///
    /// The slot being written is never the authoritative one, so every failure
    /// below leaves the live image whole. The directory entry was made durable
    /// at open, so nothing here touches the directory.
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
        // Truncating is what keeps a shorter image from inheriting a longer
        // one's tail. It is safe precisely because this slot is the stale one.
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&path)
            .map_err(|source| {
                self.publication_failed(LockStoreError::Io {
                    operation: "open a lock store slot for publication",
                    path: path.clone(),
                    source,
                })
            })?;

        let emitted = self.emit(&mut file, image, publication, &path)?;
        if emitted < image.len() {
            let error = self
                .take_fault(publication, WriteFaultSite::AfterBytes)
                .expect("a short emit only happens when a byte-boundary fault fired");
            return Err(self.publication_failed(error));
        }

        if let Some(error) = self.take_fault(publication, WriteFaultSite::AtSlotSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LockStoreError::Io {
                operation: "sync a published lock store slot",
                path,
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
}

impl WriteFaultSite {
    const fn matches(self, fault: WriteFault) -> bool {
        matches!(
            (self, fault),
            (Self::BeforeFirstByte, WriteFault::BeforeFirstByte)
                | (Self::AfterBytes, WriteFault::AfterBytes(_))
                | (Self::AtSlotSync, WriteFault::AtSlotSync)
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

/// Verifies one slot's bytes, returning its sealed image or the damage found.
///
/// An empty slot has never been published to, which is not damage.
fn verify_slot(bytes: &[u8]) -> Result<Option<SealedImage<'_>>, SlotDamage> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let Some(header) = bytes.get(..SLOT_HEADER_LEN) else {
        return Err(SlotDamage::HeaderIncomplete {
            present: as_u64(bytes.len()),
        });
    };
    if header[..4] != SLOT_MAGIC {
        let mut magic = [0_u8; 4];
        magic.copy_from_slice(&header[..4]);
        return Err(SlotDamage::NotALockImage { magic });
    }
    if header[4] != SLOT_FORMAT_VERSION {
        return Err(SlotDamage::UnsupportedFormatVersion { version: header[4] });
    }
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

/// Picks the slot recovery adopts, failing closed when neither is readable.
fn choose_live_slot(
    states: &[SlotState; 2],
    images: [Option<DecodedImage>; 2],
) -> Result<Option<AdoptedImage>, LockStoreError> {
    let generations = [generation_of(states[0]), generation_of(states[1])];
    let [zero, one] = images;

    let adopted = match (generations[0], generations[1], zero, one) {
        (Some(left), Some(right), _, _) if left == right => {
            return Err(LockStoreError::AmbiguousGeneration { generation: left });
        }
        (Some(left), Some(right), Some(zero), Some(one)) => {
            // Both slots survived, which is the ordinary case and the one that
            // lets recovery re-check the marks across the commit boundary it is
            // recovering rather than take the newest image's word for it.
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
            Some(AdoptedImage {
                slot: newer_slot,
                generation: left.max(right),
                image: newer,
                cross_checked_marks: true,
            })
        }
        (Some(generation), None, Some(image), _) => Some(AdoptedImage {
            slot: SlotIndex::Zero,
            generation,
            image,
            cross_checked_marks: false,
        }),
        (None, Some(generation), _, Some(image)) => Some(AdoptedImage {
            slot: SlotIndex::One,
            generation,
            image,
            cross_checked_marks: false,
        }),
        _ => None,
    };
    if adopted.is_some() {
        return Ok(adopted);
    }

    // No image was readable. A store whose slots are *both* damaged cannot know
    // its high-water marks, and opening empty would reissue token 1, so it
    // fails closed. One damaged slot beside an empty one is the different case
    // the rule must not catch: a publication only ever writes the stale slot,
    // so an empty partner means no publication has ever committed and there was
    // never a mark to lose.
    if states.iter().all(|state| state.damage().is_some()) {
        return Err(LockStoreError::NoReadableImage { slots: *states });
    }
    Ok(None)
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

/// Creates one slot file if it does not exist, returning whether it did.
fn create_slot_if_absent(directory: &Path, slot: SlotIndex) -> Result<bool, LockStoreError> {
    let path = slot_path(directory, slot);
    match OpenOptions::new().create_new(true).write(true).open(&path) {
        Ok(file) => {
            drop(file);
            Ok(true)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
        Err(source) => Err(LockStoreError::Io {
            operation: "create a lock store slot",
            path,
            source,
        }),
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

/// Reads one slot's raw bytes.
///
/// Crash tests corrupt sealed images to prove the checksums are load bearing,
/// which needs the exact bytes the store wrote.
///
/// # Errors
///
/// Returns an error when the slot cannot be read.
pub fn read_slot_bytes(directory: &Path, slot: SlotIndex) -> Result<Vec<u8>, LockStoreError> {
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

/// Recomputes both checksums over a modified slot image.
///
/// Several header fields — the generation, the declared bounds, the applied
/// index — are protected by a checksum precisely so recovery can trust them,
/// which means flipping one of them cannot reach the check that reads it: the
/// checksum catches the flip first. Resealing is how a test presents a
/// *well-formed* image that nonetheless says something a correct store would
/// never write, so that the checks behind the checksums are reachable at all.
///
/// It exists for crash tests and has no place in a durable application's own
/// code path.
///
/// # Panics
///
/// Panics when `image` is too short to hold a header and a trailer, which is
/// not an image any store wrote.
#[must_use]
pub fn reseal_image(mut image: Vec<u8>) -> Vec<u8> {
    assert!(
        image.len() >= SLOT_HEADER_LEN + SLOT_TRAILER_LEN,
        "a resealable image is at least a header and a trailer"
    );
    let header_checksum = crc32(&image[..HEADER_CHECKSUM_OFFSET]);
    image[HEADER_CHECKSUM_OFFSET..SLOT_HEADER_LEN].copy_from_slice(&header_checksum.to_be_bytes());
    let sealed = image.len() - SLOT_TRAILER_LEN;
    let commit_checksum = crc32(&image[..sealed]);
    image[sealed..].copy_from_slice(&commit_checksum.to_be_bytes());
    image
}

/// Overwrites the applied Raft index a slot header declares, leaving the
/// payload's own copy alone.
///
/// The two copies exist so recovery can order and report without decoding, and
/// they are cross-checked because a disagreement means the artifact is not what
/// it says it is. A correct store cannot produce that disagreement — both
/// copies come from one argument — so this is the only way to reach the check.
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
    reseal_image(image)
}

/// Overwrites one slot's raw bytes.
///
/// This is the corruption half of [`read_slot_bytes`]; it exists for crash
/// tests and has no place in a durable application's own code path.
///
/// # Errors
///
/// Returns an error when the slot cannot be written.
pub fn write_slot_bytes(
    directory: &Path,
    slot: SlotIndex,
    bytes: &[u8],
) -> Result<(), LockStoreError> {
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
