//! The ledger's durable transactional application backend.
//!
//! [`LedgerStore`] holds every fact the contract enumerates — account
//! balances, active sessions, the deduplication cache with its exact cached
//! mutation and cached result, the external deposit total, and the applied Raft
//! index — and moves all of them across one atomic, durable commit point. A
//! reader auditing this store should be able to answer, from this file alone,
//! what the commit point is and what a crash on either side of it leaves.
//!
//! # Why a journal of whole images
//!
//! The contract requires one transaction to bind four different kinds of fact
//! together: account mutations, the session and deduplication mutation, the
//! cached command result, and the applied Raft index. A write-ahead journal
//! makes that binding a single record. The transaction is committed exactly
//! when its commit record is present and both of its checksums verify, so the
//! four facts are reachable together or not at all, and there is one byte
//! offset in the file where that changes.
//!
//! Each frame carries the whole application state rather than a delta. The
//! ledger's durable state is bounded by [`LedgerConfig`], so a whole image is
//! affordable, and it buys the property that every committed frame is
//! independently complete: recovery decodes the newest committed frame and
//! stops. A delta journal would need a base image, a checkpoint protocol, and
//! a rule for ordering the two on recovery — three mechanisms to audit instead
//! of one, and three chances for a torn tail to strand a delta whose base is
//! gone.
//!
//! The image is exactly the adapter's application snapshot frame. The contract
//! enumerates the same facts for the durable transaction and for the
//! application snapshot, so encoding them twice would be two chances to forget
//! the deduplication cache. Recovery decodes an image through
//! [`Ledger::from_snapshot`], which is the model's own validating restore path,
//! so a frame whose checksums verify still cannot produce a ledger that
//! violates a resource or supply invariant.
//!
//! Renaming is used for exactly one job: replacing the journal wholesale, when
//! the new content does not extend the old. Snapshot install and compaction are
//! both that job, so they share one mechanism rather than growing a second one.
//!
//! # Format
//!
//! The store owns one directory containing the journal `ledger.journal`.
//! Anything that does not extend the journal — a rewrite, and the creation of
//! the journal itself — stages `ledger.journal.tmp` beside it and renames it
//! into place. No other file is durable state, and every staging file present
//! at open is removed.
//!
//! "Every staging file" is meant literally: the sweep removes anything beside
//! the journal whose name begins with the journal's name and a dot, whoever
//! wrote it. That is a deliberate widening. A staging file is left behind by a
//! process that *died*, so the process that finds one is never the process that
//! wrote it, and a name only its author could recognize — a process ID, say —
//! is a name nobody ever removes. An earlier shape of this store carried the
//! process ID and could therefore clean up after every crash except the one
//! kind of crash that produces staging files, leaking a whole image per
//! interrupted rewrite.
//!
//! Removing another writer's file is safe because there is no other writer.
//! Ownership of the directory is assumed rather than enforced here — two live
//! stores over one directory would interleave appends and corrupt each other,
//! and nothing in this file stops them — but the composition supplies it, and
//! the sweep leans on that rather than on a name.
//!
//! The process composition supplies the missing exclusion without changing this
//! file. A replica process takes `rafter-storage`'s operating-system lock over
//! its Raft store directory *before* it opens this journal, and holds it for
//! the process's life, so a second process is refused at the sibling directory
//! and never reaches this one. That is an ordering discipline stated in
//! `CONTRACT.md` rather than a lock this store holds, and the difference
//! matters to anyone embedding [`LedgerStore`] on its own: alone, it defends
//! nothing. It is also what the staging sweep above rests on: at the moment the
//! sweep runs there is exactly one live writer for this directory, so every
//! staging file it finds was abandoned by an incarnation that is gone.
//!
//! # Creating the journal
//!
//! Creation is a rename, for the same reason a rewrite is. Writing a header
//! into a freshly created file leaves a window between the two in which the
//! directory holds a journal too short to be one — and a later open cannot
//! recover from that, because the file exists, so creation never runs again and
//! the header is never written. The directory would be bricked by a crash in a
//! two-statement function.
//!
//! So creation stages the header in `ledger.journal.tmp`, syncs it, renames it
//! to `ledger.journal`, and syncs the directory. The journal therefore appears
//! with its header or does not appear at all, an interrupted creation leaves
//! only a staging file the next open sweeps, and the next open then creates the
//! journal properly.
//!
//! A journal shorter than its header is consequently not a state this store can
//! produce, and [`LedgerStoreError::HeaderTruncated`] stays a refusal rather
//! than becoming a reason to create the journal again. Re-creating it would be
//! the same mistake this file's recovery rules exist to avoid, in its plainest
//! form: a file that has been truncated to nothing is unreadable, not absent,
//! and nothing about it says whether it once held committed transactions.
//! Opening a fresh empty ledger over it would be a silent history deletion with
//! no corrupted byte anywhere. The crash window is closed by the rename; the
//! refusal is what covers everything the rename does not explain.
//!
//! Unless a record says otherwise:
//!
//! - integers are unsigned and big-endian;
//! - records are packed with no alignment or padding;
//! - a magic or version other than the one named here is rejected, and never
//!   quietly discarded as a tail;
//! - each record's trailing `crc32` is CRC-32/IEEE over every preceding byte of
//!   that record, and checksum coverage ends immediately before `crc32`; and
//! - CRC-32 is an accidental-corruption check, not an authentication tag.
//!
//! ## Journal header (`RLDG`)
//!
//! The header has a fixed size of 21 bytes and appears once, at offset zero.
//!
//! ```text
//! magic          [4]   "RLDG"
//! version        u8    1
//! max_clients    u32
//! max_accounts   u64
//! crc32          u32
//! ```
//!
//! `max_clients` and `max_accounts` are the [`LedgerConfig`] the journal was
//! created under. Opening a journal under different bounds is rejected rather
//! than reinterpreted, because the bounds decide which images are valid.
//!
//! ## Transaction frame
//!
//! Every byte after the header belongs to a transaction frame. One frame is a
//! begin record, then its image, then a commit record.
//!
//! ### Begin record (`RLBG`)
//!
//! The begin record has a fixed size of 17 bytes.
//!
//! ```text
//! magic          [4]   "RLBG"
//! version        u8    1
//! image_len      u32
//! image_crc32    u32
//! crc32          u32
//! ```
//!
//! `image_crc32` covers the image bytes that follow this record. The record's
//! own `crc32` covers the preceding 13 bytes, which is what makes `image_len`
//! safe to trust: recovery uses that length to find the commit record, and a
//! corrupt length would otherwise send it to a wild offset.
//!
//! ### Image
//!
//! `image_len` bytes, holding exactly one application snapshot frame. The image
//! carries its own leading version byte and its own applied Raft index, so it
//! is self-describing independently of this journal's framing.
//!
//! ### Commit record (`RLCM`)
//!
//! The commit record has a fixed size of 13 bytes.
//!
//! ```text
//! magic          [4]   "RLCM"
//! version        u8    1
//! frame_crc32    u32
//! crc32          u32
//! ```
//!
//! `frame_crc32` covers the begin record and the image together. It is not
//! redundant with the two checksums above it: it binds this commit record to
//! this begin record and this image, so a commit record surviving from an
//! abandoned tail cannot seal a different frame that happens to end at the same
//! offset.
//!
//! # Crash contract
//!
//! The authoritative artifact is the journal. The logical commit point of a
//! transaction is the return of the `sync_data` that follows its commit record;
//! the logical commit point of a rewrite is the return of the directory sync
//! that follows its rename. `Ok` means the new state is visible to a fresh
//! opener. `Err` means the outcome is unknown, and reopening is the oracle that
//! decides it — never an inference that `Err` left no bytes changed.
//!
//! A crash at any byte boundary leaves the store recoverable to exactly the
//! pre-transaction or the post-transaction state, never between:
//!
//! - Before the first byte of a frame, the journal is unchanged, so recovery
//!   sees the pre-transaction state with a clean tail.
//! - Part-way through the begin record, the image, or the commit record, the
//!   trailing bytes fail one of the checks above. Recovery stops at the last
//!   committed frame — the pre-transaction state — and reports the residue as
//!   a [`TornTail`].
//! - With the image complete and no commit record, the transaction is written
//!   but not committed, which is the same pre-transaction state. This is the
//!   window a write-ahead journal exists to make representable.
//! - After the commit record's sync returns, the frame is committed, so
//!   recovery sees the post-transaction state.
//!
//! An interrupted rewrite leaves either the original journal or the staged
//! file, never a partial journal, because the staged file is only named
//! `ledger.journal` by an atomic rename. A crash between that rename and the
//! directory sync may leave either the old or the new name durable; both are
//! whole, valid journals, so the pre-or-post property still holds and only the
//! `Ok` is withheld.
//!
//! Recovery truncates a torn tail before the store accepts another
//! transaction, so an append can never follow abandoned bytes. Truncation
//! discards only bytes that no commit point ever covered, and it is idempotent:
//! a crash during it leaves work a later open repeats.
//!
//! # Which residue [`LedgerStore::open`] may truncate
//!
//! That last sentence is a promise, and keeping it needs a rule sharper than
//! "the scan stopped here". Recovery walks frames from the header and stops at
//! the first one it cannot read. Where it stopped is *not* evidence about
//! whether the bytes beyond it were committed: a frame in the middle of a long
//! journal can become unreadable, and everything after it — whole, correctly
//! sealed, acknowledged transactions — is then unreachable too, because the
//! next frame's offset is only knowable through the one that cannot be read.
//!
//! So `open` may truncate only residue it can *prove* no commit point covered,
//! and there is exactly one proof available. An append writes one frame forward
//! at the end of the file, so every state it can be interrupted in is a strict
//! prefix of that frame, and a prefix has four possible shapes: fewer bytes
//! than a begin record, a partial image, a whole image with no commit record,
//! and a partial commit record. [`TornTail::is_interrupted_append`] names those
//! four. A prefix carries this store's magic and this build's version byte, and
//! every byte it does carry is a byte that was written, so it can never fail a
//! checksum computed over bytes that are all present.
//!
//! The other three shapes — a whole begin record that does not verify, a whole
//! image that does not match its checksum, a whole commit record that seals
//! nothing — are therefore not prefixes of anything this build wrote. They are
//! corruption, they may sit at or below the last commit point, and they make
//! everything after them unreadable. `open` refuses with
//! [`LedgerStoreError::UnreadableFrame`] rather than treating them as a tail,
//! because treating them as a tail means deleting acknowledged history from the
//! medium during what the caller asked for as a *read*.
//!
//! # Repairing, as a separate act
//!
//! Discarding a region a commit point may have covered is sometimes the only
//! way forward, so it is available — as [`LedgerStore::open_and_repair`], never
//! as a side effect of opening. That entry point discards from the unreadable
//! frame to the end of the file and records what it did in
//! [`RecoveryReport::repair`]: the offset, the corruption that stopped the
//! scan, and the byte count.
//!
//! It cannot report how many *transactions* were in that region, and that is
//! the honest limit rather than an omission: the frames past a corrupt one
//! cannot be located, let alone counted, which is precisely why discarding them
//! has to be something a caller asks for by name.
//!
//! After any write error the handle is poisoned and every later mutation is
//! refused with [`LedgerStoreError::StoreRequiresReopen`], because a store that
//! failed mid-publication cannot say where its file ends.
//!
//! # What the crash tests do not prove
//!
//! `durable_crash.rs` interrupts publications inside one live process, so it
//! proves which bytes reached the file and what a fresh opener makes of them.
//! It cannot prove that a barrier reached the medium: a process that never dies
//! reads its own writes back through the page cache whether or not `sync_data`
//! ran. Deleting a `sync_data` or a directory sync from this file leaves the
//! suite green. Those calls are justified by the ordering argument above and by
//! review, not by a test, and a claim that this store survives power loss on a
//! particular filesystem needs evidence this suite does not supply.

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
    adapter::codec::{decode_snapshot, encode_snapshot},
    ClientId, Ledger, LedgerCodecError, LedgerConfig, Sequence, SessionEpoch, SnapshotError,
};

/// Fixed length of the journal header, in bytes.
pub const HEADER_LEN: usize = 21;
/// Fixed length of a transaction begin record, in bytes.
pub const BEGIN_LEN: usize = 17;
/// Fixed length of a transaction commit record, in bytes.
pub const COMMIT_LEN: usize = 13;

const JOURNAL_MAGIC: [u8; 4] = *b"RLDG";
const BEGIN_MAGIC: [u8; 4] = *b"RLBG";
const COMMIT_MAGIC: [u8; 4] = *b"RLCM";

/// Version byte of every record this build writes.
const JOURNAL_FORMAT_VERSION: u8 = 1;

/// Stable name of the journal inside the store's directory.
const JOURNAL_FILE_NAME: &str = "ledger.journal";

/// Stable name of the file a rewrite or a creation stages beside the journal.
///
/// There is no process ID in it. An abandoned staging file is by definition the
/// work of a process that died, so a name only its author could recognize is a
/// name nobody ever removes; exclusivity comes from the directory ownership
/// discipline instead, and the sweep at open removes whatever it finds.
const STAGED_FILE_NAME: &str = "ledger.journal.tmp";

const CRC32_POLYNOMIAL: u32 = 0xEDB8_8320;

/// CRC-32/IEEE over `bytes`.
///
/// This is the bitwise reference form rather than a table-driven one. The store
/// commits bounded images, so the byte-scanning cost is irrelevant next to
/// being auditable at a glance, and `checksums_match_the_published_vector`
/// pins it to the standard check value.
fn crc32(bytes: &[u8]) -> u32 {
    let mut state = 0xFFFF_FFFF_u32;
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

/// Failure of a durable ledger store operation.
///
/// This enum is exhaustive because the journal format is closed over these
/// corruption, configuration, and publication failures, and because a caller
/// deciding whether a transaction committed has to be able to match on all of
/// them.
///
/// It is deliberately neither `Clone` nor `Eq`: a variant carrying a live
/// [`std::io::Error`] has no meaningful value equality, and pretending
/// otherwise would invite tests to assert on a projection of an operating
/// system's diagnostics.
#[derive(Debug)]
pub enum LedgerStoreError {
    /// A filesystem operation failed.
    Io {
        /// Lowercase verb phrase naming the attempted operation.
        operation: &'static str,
        /// Path the operation addressed.
        path: PathBuf,
        /// Underlying filesystem failure.
        source: std::io::Error,
    },
    /// The journal is shorter than its fixed header.
    HeaderTruncated {
        /// Bytes present in the journal.
        length: u64,
    },
    /// The journal does not begin with this store's magic.
    NotALedgerJournal {
        /// The four bytes found where the magic belongs.
        magic: [u8; 4],
    },
    /// The journal declares a format this build cannot read.
    UnsupportedFormatVersion {
        /// Version byte found in the header.
        version: u8,
    },
    /// The journal header's checksum does not match its bytes.
    HeaderChecksumMismatch {
        /// Checksum the header declares.
        expected: u32,
        /// Checksum computed over the header's bytes.
        found: u32,
    },
    /// The journal was created under different resource bounds.
    ConfigMismatch {
        /// Client-slot bound recorded in the journal header.
        journal_max_clients: u32,
        /// Account bound recorded in the journal header.
        journal_max_accounts: u64,
        /// Client-slot bound the caller opened with.
        requested_max_clients: u32,
        /// Account bound the caller opened with.
        requested_max_accounts: u64,
    },
    /// The journal holds a frame no interrupted append could have left.
    ///
    /// Where the scan stopped says nothing about whether the bytes beyond it
    /// were committed: everything after an unreadable frame is unreachable,
    /// because the next frame's offset is only knowable through the one that
    /// cannot be read. Treating that as a torn tail deletes acknowledged
    /// history from the medium during a read, so `open` refuses instead.
    ///
    /// [`LedgerStore::open_and_repair`] is the entry point that discards it, by
    /// name and with a report.
    UnreadableFrame {
        /// Byte offset the unreadable frame begins at.
        offset: u64,
        /// Why the frame could not be read.
        corruption: TornTail,
        /// Committed frames the scan replayed before reaching it.
        committed_frames: u64,
        /// Bytes from `offset` to the end of the journal, which is what a
        /// repair would discard.
        unreadable_bytes: u64,
    },
    /// A committed frame's image is not a decodable application snapshot.
    Image(LedgerCodecError),
    /// A committed frame's image violates a model resource or supply
    /// invariant.
    Snapshot(SnapshotError),
    /// Committed frames report applied indexes that do not increase.
    ///
    /// A journal in this shape would let recovery move the applied floor
    /// backwards and make an acknowledged command executable again.
    NonMonotonicAppliedIndex {
        /// Applied index of the previous committed frame.
        previous: LogIndex,
        /// Applied index of the frame that followed it.
        found: LogIndex,
    },
    /// A rewrite at an unchanged applied index would move a client slot's
    /// deduplication state backwards.
    ///
    /// The applied Raft index is not the whole ordering key for the
    /// deduplication cache: two images can name the same index and still
    /// disagree about which requests have completed. Adopting the poorer one
    /// makes an acknowledged mutation executable again, which is the one thing
    /// the cache exists to prevent.
    DeduplicationRegression {
        /// Client slot whose deduplication state would move backwards.
        client_id: ClientId,
        /// Progress this store has durably acknowledged for that slot.
        acknowledged: DeduplicationProgress,
        /// Progress the offered ledger carries, if it holds the slot at all.
        offered: Option<DeduplicationProgress>,
    },
    /// An encoded image does not fit the begin record's length field.
    ///
    /// The frame declares its image length as a `u32`, so an image above that
    /// bound could not be found again by recovery.
    ImageTooLarge {
        /// Encoded length of the image.
        length: u64,
    },
    /// An earlier write left this handle unable to say where its file ends.
    ///
    /// Reopen the store; recovery is the only thing that can decide what the
    /// interrupted publication left behind.
    StoreRequiresReopen,
    /// A deterministic fault from the store's test construction fired.
    ///
    /// This is the injected-crash seam described on [`FaultPlan`]. It is a
    /// write failure like any other: the handle is poisoned and reopening
    /// decides what the interrupted plan left behind.
    InjectedFault {
        /// The fault that fired.
        fault: WriteFault,
        /// One-based ordinal of the write plan it fired on.
        plan: u64,
    },
}

impl fmt::Display for LedgerStoreError {
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
            Self::HeaderTruncated { length } => write!(
                formatter,
                "journal is {length} bytes, shorter than its {HEADER_LEN}-byte header"
            ),
            Self::NotALedgerJournal { magic } => {
                write!(formatter, "journal magic {magic:?} is not a ledger journal")
            }
            Self::UnsupportedFormatVersion { version } => {
                write!(formatter, "unsupported journal format version {version}")
            }
            Self::HeaderChecksumMismatch { expected, found } => write!(
                formatter,
                "journal header declares checksum {expected:#010x} but its bytes checksum {found:#010x}"
            ),
            Self::ConfigMismatch {
                journal_max_clients,
                journal_max_accounts,
                requested_max_clients,
                requested_max_accounts,
            } => write!(
                formatter,
                "journal was created for {journal_max_clients} clients and {journal_max_accounts} accounts, \
                 but was opened for {requested_max_clients} clients and {requested_max_accounts} accounts"
            ),
            Self::UnreadableFrame {
                offset,
                corruption,
                committed_frames,
                unreadable_bytes,
            } => write!(
                formatter,
                "the frame at byte {offset} is {corruption}, which no interrupted append leaves; \
                 {committed_frames} frames were readable before it and the {unreadable_bytes} bytes \
                 from there on may hold committed transactions"
            ),
            Self::Image(error) => write!(formatter, "malformed committed image: {error}"),
            Self::Snapshot(error) => write!(formatter, "invalid committed image: {error:?}"),
            Self::NonMonotonicAppliedIndex { previous, found } => write!(
                formatter,
                "committed frame at applied index {found} follows one at {previous}"
            ),
            Self::DeduplicationRegression {
                client_id,
                acknowledged,
                offered,
            } => write_deduplication_regression(formatter, *client_id, *acknowledged, *offered),
            Self::ImageTooLarge { length } => {
                write!(formatter, "image of {length} bytes exceeds the frame's length field")
            }
            Self::StoreRequiresReopen => formatter.write_str(
                "an earlier write failed mid-publication; reopen the store before mutating it",
            ),
            Self::InjectedFault { fault, plan } => {
                write!(formatter, "injected {fault} on write plan {plan}")
            }
        }
    }
}

/// Renders a deduplication regression, which reads differently when the client
/// slot vanished from the offered ledger than when its progress merely dropped.
fn write_deduplication_regression(
    formatter: &mut fmt::Formatter<'_>,
    client_id: ClientId,
    acknowledged: DeduplicationProgress,
    offered: Option<DeduplicationProgress>,
) -> fmt::Result {
    match offered {
        Some(offered) => write!(
            formatter,
            "client slot {} would drop from {acknowledged} to {offered} at an unchanged applied index",
            client_id.get()
        ),
        None => write!(
            formatter,
            "client slot {} would lose its deduplication state at {acknowledged}",
            client_id.get()
        ),
    }
}

impl Error for LedgerStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Image(error) => Some(error),
            Self::HeaderTruncated { .. }
            | Self::NotALedgerJournal { .. }
            | Self::UnsupportedFormatVersion { .. }
            | Self::HeaderChecksumMismatch { .. }
            | Self::ConfigMismatch { .. }
            | Self::UnreadableFrame { .. }
            | Self::Snapshot(_)
            | Self::NonMonotonicAppliedIndex { .. }
            | Self::DeduplicationRegression { .. }
            | Self::ImageTooLarge { .. }
            | Self::StoreRequiresReopen
            | Self::InjectedFault { .. } => None,
        }
    }
}

/// How far one client slot's session had progressed when it was made durable.
///
/// This is the key the deduplication cache is ordered by, and it is deliberately
/// not the applied Raft index: a rewrite may republish the index the store
/// already holds, and at that index the index itself says nothing about which
/// requests have completed.
///
/// Ordering is lexicographic — the session epoch first, then the highest
/// completed sequence under it — because opening a newer epoch is exactly what
/// legitimately clears an older epoch's cache. A slot on a later epoch has not
/// lost anything by holding no completion yet.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DeduplicationProgress {
    /// Session generation the slot was on.
    pub session_epoch: SessionEpoch,
    /// Highest completed sequence cached under that epoch, if any.
    pub completed: Option<Sequence>,
}

impl fmt::Display for DeduplicationProgress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.completed {
            Some(sequence) => write!(
                formatter,
                "epoch {} through sequence {}",
                self.session_epoch.get(),
                sequence.get()
            ),
            None => write!(
                formatter,
                "epoch {} with nothing completed",
                self.session_epoch.get()
            ),
        }
    }
}

/// A deterministic fault injected into one of the store's write plans.
///
/// Each variant names a boundary inside a publication rather than a wall-clock
/// moment, so a crash test reproduces an exact byte offset instead of racing
/// for one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WriteFault {
    /// Fail before the plan emits its first byte.
    BeforeFirstByte,
    /// Emit the first `bytes` bytes of the plan, make them durable, then fail.
    ///
    /// The emitted prefix is synced deliberately. The interesting recovery case
    /// is the one where a partial write did reach the medium, and a test that
    /// left the prefix in a write-back cache would be proving something weaker
    /// than it claims.
    AfterBytes(u64),
    /// Emit every byte of the plan, then fail its file durability barrier.
    AtFileSync,
    /// Emit and sync the staged file, then fail before the rename publishes it.
    ///
    /// Only a rewrite renames, so this never fires on an append.
    BeforeRename,
    /// Rename the staged file, then fail before the directory entry is durable.
    ///
    /// Only a rewrite renames, so this never fires on an append.
    AfterRename,
}

impl fmt::Display for WriteFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BeforeFirstByte => formatter.write_str("failure before the first byte"),
            Self::AfterBytes(bytes) => write!(formatter, "failure after {bytes} bytes"),
            Self::AtFileSync => formatter.write_str("failure at the file sync"),
            Self::BeforeRename => formatter.write_str("failure before the rename"),
            Self::AfterRename => formatter.write_str("failure after the rename"),
        }
    }
}

/// Deterministic fault schedule attached to one store instance.
///
/// Injection is part of a store's construction rather than a process-wide
/// switch: a plan travels with the handle it was built for, so two stores in
/// one test — or two tests in one process — cannot observe each other's faults.
///
/// Plans are addressed by write-plan ordinal. Every publication the store
/// performs, whether an append or a rewrite, consumes the next ordinal starting
/// at one, so a scenario names the exact transaction it means to interrupt.
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

    /// A plan that injects `fault` on the `plan`-th write plan.
    #[must_use]
    pub fn at(plan: u64, fault: WriteFault) -> Self {
        Self::none().and(plan, fault)
    }

    /// Adds another injection to this plan.
    #[must_use]
    pub fn and(mut self, plan: u64, fault: WriteFault) -> Self {
        self.faults.push((plan, fault));
        self
    }

    fn fault_for(&self, plan: u64) -> Option<WriteFault> {
        self.faults
            .iter()
            .find(|(ordinal, _)| *ordinal == plan)
            .map(|(_, fault)| *fault)
    }
}

impl fmt::Display for FaultPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.faults.is_empty() {
            return formatter.write_str("no injected faults");
        }
        let mut separator = "";
        for (plan, fault) in &self.faults {
            write!(formatter, "{separator}plan {plan}: {fault}")?;
            separator = ", ";
        }
        Ok(())
    }
}

/// Why recovery stopped before the end of the journal.
///
/// A torn tail is a normal residue of an interrupted transaction, not a fault,
/// so it is reported here rather than as a [`LedgerStoreError`]. Each variant
/// names the byte boundary the interrupted write reached, which is what lets a
/// crash test prove that its injection bit where it aimed.
///
/// Only four of these variants are residue an interrupted append can actually
/// leave; see [`TornTail::is_interrupted_append`]. The other three name
/// corruption, and reaching one refuses the store rather than truncating.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum TornTail {
    /// Fewer bytes remain than one begin record needs.
    PartialBeginRecord,
    /// The begin record's magic, version, or own checksum does not verify.
    BeginRecordCorrupt,
    /// The image the begin record declares is not fully present.
    PartialImage,
    /// The image is complete but does not match its declared checksum.
    ImageCorrupt,
    /// The image is complete and no commit record follows it.
    ///
    /// This is the write-ahead window: the transaction was written and never
    /// committed.
    MissingCommitRecord,
    /// Fewer bytes remain than one commit record needs.
    PartialCommitRecord,
    /// The commit record is complete but does not seal this frame.
    CommitRecordCorrupt,
}

impl TornTail {
    /// Whether an interrupted append of *this* build could have left this.
    ///
    /// An append writes one frame forward at the end of the journal, so every
    /// state it can be interrupted in is a strict prefix of that frame. The
    /// four shapes below are exactly those prefixes: fewer bytes than a begin
    /// record, a partial image, a whole image with no commit record, and a
    /// partial commit record.
    ///
    /// Nothing else is a prefix of anything this build writes. A prefix carries
    /// this store's magic and this build's version byte, and every byte it does
    /// carry is a byte that was written, so it cannot fail a checksum computed
    /// over bytes that are all present.
    ///
    /// That asymmetry is what makes truncation safe. Residue proves the bytes
    /// beyond the last committed frame were never committed, so discarding them
    /// discards nothing. The other three shapes prove nothing of the kind: the
    /// unreadable frame may itself have been committed, and every frame after
    /// it is unreachable regardless, so `open` refuses rather than shortening
    /// the journal on a caller's behalf.
    #[must_use]
    pub const fn is_interrupted_append(self) -> bool {
        matches!(
            self,
            Self::PartialBeginRecord
                | Self::PartialImage
                | Self::MissingCommitRecord
                | Self::PartialCommitRecord
        )
    }
}

impl fmt::Display for TornTail {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::PartialBeginRecord => "a partial begin record",
            Self::BeginRecordCorrupt => "a corrupt begin record",
            Self::PartialImage => "a partial image",
            Self::ImageCorrupt => "a corrupt image",
            Self::MissingCommitRecord => "a written but uncommitted transaction",
            Self::PartialCommitRecord => "a partial commit record",
            Self::CommitRecordCorrupt => "a corrupt commit record",
        })
    }
}

/// What opening the store found and did.
///
/// Recovery actions are observations, not failures, so they stay out of
/// [`LedgerStoreError`]. A test asserts on this report to show which crash
/// window it actually reproduced.
///
/// The report describes one opening and never changes afterwards. A later
/// transaction does not edit the history of how this handle came to exist; a
/// caller that wants the journal's current shape reads
/// [`LedgerStore::journal_len`], and a caller that wants to know what a fresh
/// opener would find reopens.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoveryReport {
    created: bool,
    committed_frames: u64,
    torn_tail: Option<TornTail>,
    discarded_bytes: u64,
    removed_staged_file: bool,
    repair: Option<Repair>,
}

/// What [`LedgerStore::open_and_repair`] discarded, when it discarded anything.
///
/// A repair is the one thing this store does that can lose committed
/// transactions, so it is recorded rather than implied, and it is unreachable
/// through [`LedgerStore::open`] at all.
///
/// The count of *transactions* lost is deliberately absent. Frames past a
/// corrupt one cannot be located, let alone decoded, so nobody can count them;
/// pretending otherwise would put a number in a report that no one computed.
/// The byte count and the offset are what is actually known.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Repair {
    offset: u64,
    corruption: TornTail,
    discarded_bytes: u64,
}

impl Repair {
    /// Byte offset the unreadable frame began at.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Why the frame at [`Repair::offset`] could not be read.
    #[must_use]
    pub const fn corruption(&self) -> TornTail {
        self.corruption
    }

    /// Bytes discarded, from [`Repair::offset`] to the end of the journal.
    ///
    /// Any number of committed transactions may have been inside them.
    #[must_use]
    pub const fn discarded_bytes(&self) -> u64 {
        self.discarded_bytes
    }
}

impl fmt::Display for Repair {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "discarded {} bytes from byte {}, where the journal held {}",
            self.discarded_bytes, self.offset, self.corruption
        )
    }
}

impl RecoveryReport {
    /// Whether the journal was created by this open.
    #[must_use]
    pub const fn created(&self) -> bool {
        self.created
    }

    /// Number of committed frames recovery replayed.
    #[must_use]
    pub const fn committed_frames(&self) -> u64 {
        self.committed_frames
    }

    /// The residue an interrupted transaction left, if any.
    #[must_use]
    pub const fn torn_tail(&self) -> Option<TornTail> {
        self.torn_tail
    }

    /// Bytes truncated from the journal's uncommitted tail.
    ///
    /// These are bytes no commit point ever covered — a strict prefix of one
    /// interrupted append — so discarding them discards nothing. Bytes lost to
    /// a repair are counted separately, by [`Repair::discarded_bytes`], because
    /// they are a different kind of loss.
    #[must_use]
    pub const fn discarded_bytes(&self) -> u64 {
        self.discarded_bytes
    }

    /// Whether an abandoned staging file was removed.
    #[must_use]
    pub const fn removed_staged_file(&self) -> bool {
        self.removed_staged_file
    }

    /// What a repair discarded, when this opening was a repair that found work.
    ///
    /// Always `None` for [`LedgerStore::open`], which refuses rather than
    /// repairing.
    #[must_use]
    pub const fn repair(&self) -> Option<Repair> {
        self.repair
    }

    /// Whether this opening found nothing that needs a decision.
    ///
    /// A clean opening created the journal or read it whole. Anything else —
    /// residue from an interrupted transaction, a staging file an earlier
    /// incarnation abandoned, a repair that discarded a region — is a fact a
    /// caller reopening a store after a crash should have to look at rather
    /// than step over.
    #[must_use]
    pub const fn is_clean(&self) -> bool {
        self.torn_tail.is_none() && !self.removed_staged_file && self.repair.is_none()
    }
}

/// Whether this handle may still publish.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Health {
    Healthy,
    ReopenRequired,
}

/// A durable, transactional ledger store over one journal file.
///
/// See the [module documentation](self) for the format and the crash contract.
#[derive(Debug)]
pub struct LedgerStore {
    directory: PathBuf,
    journal_path: PathBuf,
    config: LedgerConfig,
    ledger: Ledger,
    applied_index: LogIndex,
    journal_len: u64,
    health: Health,
    faults: FaultPlan,
    /// Write plans this handle has started, which is what [`FaultPlan`] keys on.
    write_plans: u64,
    fired_fault: Option<WriteFault>,
    recovery: RecoveryReport,
}

impl LedgerStore {
    /// Opens the store in `directory`, creating and recovering as needed.
    ///
    /// # Errors
    ///
    /// Returns an error when the directory or journal cannot be opened, when
    /// the journal header is corrupt or was written under different resource
    /// bounds, when a committed frame's image is malformed or violates a model
    /// invariant, or when the journal holds a frame no interrupted append could
    /// have left — see [`LedgerStoreError::UnreadableFrame`], and
    /// [`LedgerStore::open_and_repair`] for the entry point that discards it.
    pub fn open(directory: &Path, config: LedgerConfig) -> Result<Self, LedgerStoreError> {
        Self::open_with_faults(directory, config, FaultPlan::none())
    }

    /// Opens the store, discarding an unreadable frame and everything after it.
    ///
    /// This is the destructive half of [`LedgerStore::open`], and it is a
    /// separate entry point because it is a separate decision. Opening a store
    /// is a read; a caller that runs this one has decided that a journal it
    /// cannot fully read is better shortened than left alone, and
    /// [`RecoveryReport::repair`] tells it exactly what that cost — the offset,
    /// the corruption, and the byte count.
    ///
    /// A journal with nothing wrong is opened exactly as [`LedgerStore::open`]
    /// opens it, and reports no repair. Repairing is not the same as being
    /// willing to repair.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`LedgerStore::open`] except
    /// [`LedgerStoreError::UnreadableFrame`], which is what this discards.
    pub fn open_and_repair(
        directory: &Path,
        config: LedgerConfig,
    ) -> Result<Self, LedgerStoreError> {
        Self::open_inner(
            directory,
            config,
            FaultPlan::none(),
            OnUnreadableFrame::Discard,
        )
    }

    /// Opens the store with a deterministic fault schedule.
    ///
    /// This is the crash-test construction described on [`FaultPlan`]. A store
    /// opened with [`FaultPlan::none`] behaves exactly as [`LedgerStore::open`].
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`LedgerStore::open`]. Faults apply to write
    /// plans, so opening never injects one.
    pub fn open_with_faults(
        directory: &Path,
        config: LedgerConfig,
        faults: FaultPlan,
    ) -> Result<Self, LedgerStoreError> {
        Self::open_inner(directory, config, faults, OnUnreadableFrame::Refuse)
    }

    /// The one opening path, parameterized by what it does with an unreadable
    /// frame.
    ///
    /// Both entry points read the same bytes and run the same scan. They differ
    /// in exactly one branch, which is the point: a reader auditing this can
    /// see that refusing and repairing agree about everything except whether a
    /// region a commit point may have covered is allowed to disappear.
    fn open_inner(
        directory: &Path,
        config: LedgerConfig,
        faults: FaultPlan,
        on_unreadable: OnUnreadableFrame,
    ) -> Result<Self, LedgerStoreError> {
        fs::create_dir_all(directory).map_err(|source| LedgerStoreError::Io {
            operation: "create the ledger store directory",
            path: directory.to_path_buf(),
            source,
        })?;

        let journal_path = directory.join(JOURNAL_FILE_NAME);
        // Before anything looks at the journal, so an interrupted creation's
        // staging file is gone by the time `exists` decides whether to create.
        let removed_staged_file = sweep_staged_files(directory)?;

        let created = !journal_path.exists();
        if created {
            create_journal(directory, config)?;
        }

        let bytes = fs::read(&journal_path).map_err(|source| LedgerStoreError::Io {
            operation: "read the ledger journal",
            path: journal_path.clone(),
            source,
        })?;
        let scan = scan_journal(&bytes, config)?;

        // The whole fail-closed rule, in one branch. Truncating is only ever
        // legal for a strict prefix of one interrupted append; anything else
        // may sit at or below the last commit point, and shortening the file
        // there would delete acknowledged history during a read.
        let unreadable = scan
            .torn_tail
            .filter(|tail| !tail.is_interrupted_append())
            .map(|corruption| Repair {
                offset: scan.committed_len as u64,
                corruption,
                discarded_bytes: (bytes.len() - scan.committed_len) as u64,
            });
        let repair = match (unreadable, on_unreadable) {
            (Some(repair), OnUnreadableFrame::Refuse) => {
                return Err(LedgerStoreError::UnreadableFrame {
                    offset: repair.offset,
                    corruption: repair.corruption,
                    committed_frames: scan.committed_frames,
                    unreadable_bytes: repair.discarded_bytes,
                })
            }
            (repair, OnUnreadableFrame::Discard) => repair,
            (None, OnUnreadableFrame::Refuse) => None,
        };

        if scan.committed_len < bytes.len() {
            truncate_journal(&journal_path, scan.committed_len)?;
        }

        let (ledger, applied_index) = match scan.image {
            Some((ledger, applied_index)) => (ledger, applied_index),
            None => (Ledger::new(config), LogIndex::ZERO),
        };

        Ok(Self {
            directory: directory.to_path_buf(),
            journal_path,
            config,
            ledger,
            applied_index,
            journal_len: scan.committed_len as u64,
            health: Health::Healthy,
            faults,
            write_plans: 0,
            fired_fault: None,
            recovery: RecoveryReport {
                created,
                committed_frames: scan.committed_frames,
                torn_tail: scan.torn_tail,
                // A repair's losses are counted by the repair, not here: these
                // are the bytes no commit point ever covered.
                discarded_bytes: if repair.is_some() {
                    0
                } else {
                    (bytes.len() - scan.committed_len) as u64
                },
                removed_staged_file,
                repair,
            },
        })
    }

    /// Returns what opening this store found and did.
    #[must_use]
    pub const fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// Returns the resource bounds this journal was created under.
    #[must_use]
    pub const fn config(&self) -> LedgerConfig {
        self.config
    }

    /// Returns the durable ledger state.
    #[must_use]
    pub const fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    /// Returns the durable applied Raft index.
    #[must_use]
    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    /// Returns the journal's committed length in bytes.
    #[must_use]
    pub const fn journal_len(&self) -> u64 {
        self.journal_len
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

    /// Returns the byte length one commit of `ledger` at `applied_index` would
    /// append.
    ///
    /// Crash tests sweep every boundary inside that length, so they need it
    /// before they arm the fault that stops inside it.
    ///
    /// # Errors
    ///
    /// Returns an error when the image cannot be encoded or does not fit the
    /// frame's length field.
    pub fn planned_frame_len(
        ledger: &Ledger,
        applied_index: LogIndex,
    ) -> Result<u64, LedgerStoreError> {
        Ok(encode_frame(ledger, applied_index)?.len() as u64)
    }

    /// Commits one transaction, appending it to the journal.
    ///
    /// The transaction carries the whole application state — account balances,
    /// sessions, the deduplication cache with its cached command results, the
    /// deposit total — and `applied_index` together. `Ok` means every one of
    /// them is durable; nothing partial is ever recoverable.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle is poisoned, when `applied_index` does
    /// not advance, when the image cannot be encoded, or when the append or its
    /// durability barrier fails. After any of the latter the handle is
    /// poisoned and the caller must reopen to learn what committed.
    pub fn commit(
        &mut self,
        ledger: &Ledger,
        applied_index: LogIndex,
    ) -> Result<(), LedgerStoreError> {
        self.check_health()?;
        // An append must advance the floor. A batch that applied nothing never
        // reaches here, so an index that does not advance is a caller error
        // rather than a no-op, and committing it would leave two frames the
        // same age with no rule for choosing between them.
        if applied_index <= self.applied_index {
            return Err(LedgerStoreError::NonMonotonicAppliedIndex {
                previous: self.applied_index,
                found: applied_index,
            });
        }
        let frame = encode_frame(ledger, applied_index)?;
        let plan = self.begin_plan();
        self.append(&frame, plan)?;

        self.journal_len += frame.len() as u64;
        self.adopt(ledger.clone(), applied_index);
        Ok(())
    }

    /// Replaces the journal with one frame holding `ledger` at `applied_index`.
    ///
    /// This is the publication a snapshot install and a compaction share: the
    /// new content does not extend the old, so it is staged beside the journal
    /// and renamed into place rather than appended. Unlike [`LedgerStore::commit`]
    /// it accepts an `applied_index` equal to the current one, because
    /// compacting in place must not require inventing a new index.
    ///
    /// # Errors
    ///
    /// Returns an error when the handle is poisoned, when `applied_index` would
    /// move the applied floor backwards, when republishing an unchanged applied
    /// index would move a client slot's deduplication state backwards, when the
    /// image cannot be encoded, or when staging, renaming, or a durability
    /// barrier fails.
    pub fn replace(
        &mut self,
        ledger: &Ledger,
        applied_index: LogIndex,
    ) -> Result<(), LedgerStoreError> {
        self.check_health()?;
        if applied_index < self.applied_index {
            return Err(LedgerStoreError::NonMonotonicAppliedIndex {
                previous: self.applied_index,
                found: applied_index,
            });
        }
        if applied_index == self.applied_index {
            // The one publication the applied floor cannot judge. Two images at
            // one index can still disagree about which requests have completed,
            // and the poorer one makes an acknowledged mutation executable a
            // second time — exactly what the deduplication cache exists to
            // prevent, reached through the store rather than through a replay.
            // Above this index the model has legitimately advanced and is the
            // authority on the sessions it retired, so the check is scoped to
            // here.
            verify_deduplication_dominates(
                &deduplication_progress_of(&self.ledger),
                &deduplication_progress_of(ledger),
            )?;
        }

        let mut contents = encode_header(self.config);
        contents.extend_from_slice(&encode_frame(ledger, applied_index)?);
        let plan = self.begin_plan();
        self.rewrite(&contents, plan)?;

        self.journal_len = contents.len() as u64;
        self.adopt(ledger.clone(), applied_index);
        Ok(())
    }

    /// Rewrites the journal down to its current state in one frame.
    ///
    /// The journal grows by a whole image per transaction, so a caller bounds
    /// it by compacting. Doing that at an application snapshot point is the
    /// natural pairing: the application has already declared that everything
    /// below its applied index is reconstructible from state rather than from
    /// history.
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`LedgerStore::replace`].
    pub fn compact(&mut self) -> Result<(), LedgerStoreError> {
        let ledger = self.ledger.clone();
        self.replace(&ledger, self.applied_index)
    }

    fn adopt(&mut self, ledger: Ledger, applied_index: LogIndex) {
        self.ledger = ledger;
        self.applied_index = applied_index;
    }

    fn check_health(&self) -> Result<(), LedgerStoreError> {
        if self.requires_reopen() {
            return Err(LedgerStoreError::StoreRequiresReopen);
        }
        Ok(())
    }

    /// Allocates the next write-plan ordinal.
    fn begin_plan(&mut self) -> u64 {
        self.write_plans += 1;
        self.write_plans
    }

    /// Records that a publication failed, poisoning the handle.
    fn publication_failed(&mut self, error: LedgerStoreError) -> LedgerStoreError {
        self.health = Health::ReopenRequired;
        error
    }

    /// Takes the fault armed for `plan`, if any, remembering that it fired.
    fn take_fault(&mut self, plan: u64, at: WriteFaultSite) -> Option<LedgerStoreError> {
        let fault = self.faults.fault_for(plan)?;
        if !at.matches(fault) {
            return None;
        }
        self.fired_fault = Some(fault);
        Some(LedgerStoreError::InjectedFault { fault, plan })
    }

    /// Appends `frame` to the journal and makes it durable.
    ///
    /// The journal's directory entry was made durable when the file was
    /// created, so an append needs only the file's own barrier.
    fn append(&mut self, frame: &[u8], plan: u64) -> Result<(), LedgerStoreError> {
        if let Some(error) = self.take_fault(plan, WriteFaultSite::BeforeFirstByte) {
            return Err(self.publication_failed(error));
        }

        let mut file = OpenOptions::new()
            .append(true)
            .open(&self.journal_path)
            .map_err(|source| LedgerStoreError::Io {
                operation: "open the ledger journal for append",
                path: self.journal_path.clone(),
                source,
            })?;

        let journal_path = self.journal_path.clone();
        let emitted = self.emit(&mut file, frame, plan, &journal_path)?;
        if emitted < frame.len() {
            let error = self
                .take_fault(plan, WriteFaultSite::AfterBytes)
                .expect("a short emit only happens when a byte-boundary fault fired");
            return Err(self.publication_failed(error));
        }

        if let Some(error) = self.take_fault(plan, WriteFaultSite::AtFileSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "sync the appended ledger transaction",
                path: self.journal_path.clone(),
                source,
            })
        })
    }

    /// Stages `contents`, syncs it, renames it over the journal, and syncs the
    /// directory.
    fn rewrite(&mut self, contents: &[u8], plan: u64) -> Result<(), LedgerStoreError> {
        if let Some(error) = self.take_fault(plan, WriteFaultSite::BeforeFirstByte) {
            return Err(self.publication_failed(error));
        }

        let staged_path = staged_path(&self.directory);
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&staged_path)
            .map_err(|source| LedgerStoreError::Io {
                operation: "open the staged ledger journal",
                path: staged_path.clone(),
                source,
            })?;

        let emitted = self.emit(&mut file, contents, plan, &staged_path)?;
        if emitted < contents.len() {
            let error = self
                .take_fault(plan, WriteFaultSite::AfterBytes)
                .expect("a short emit only happens when a byte-boundary fault fired");
            return Err(self.publication_failed(error));
        }

        if let Some(error) = self.take_fault(plan, WriteFaultSite::AtFileSync) {
            return Err(self.publication_failed(error));
        }
        file.sync_data().map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "sync the staged ledger journal",
                path: staged_path.clone(),
                source,
            })
        })?;
        drop(file);

        if let Some(error) = self.take_fault(plan, WriteFaultSite::BeforeRename) {
            return Err(self.publication_failed(error));
        }
        fs::rename(&staged_path, &self.journal_path).map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "publish the staged ledger journal",
                path: self.journal_path.clone(),
                source,
            })
        })?;

        if let Some(error) = self.take_fault(plan, WriteFaultSite::AfterRename) {
            return Err(self.publication_failed(error));
        }
        sync_directory(&self.directory).map_err(|error| self.publication_failed(error))
    }

    /// Writes `bytes` to `file`, honoring a byte-boundary fault.
    ///
    /// Returns how many bytes were emitted; a short return means a fault
    /// stopped the plan, and the prefix was synced so recovery meets the worst
    /// case where it reached the medium.
    fn emit(
        &mut self,
        file: &mut File,
        bytes: &[u8],
        plan: u64,
        path: &Path,
    ) -> Result<usize, LedgerStoreError> {
        let limit = match self.faults.fault_for(plan) {
            Some(WriteFault::AfterBytes(stop)) => {
                usize::try_from(stop).unwrap_or(usize::MAX).min(bytes.len())
            }
            _ => bytes.len(),
        };

        file.write_all(&bytes[..limit]).map_err(|source| {
            self.publication_failed(LedgerStoreError::Io {
                operation: "write the ledger journal",
                path: path.to_path_buf(),
                source,
            })
        })?;
        if limit < bytes.len() {
            file.sync_data().map_err(|source| {
                self.publication_failed(LedgerStoreError::Io {
                    operation: "sync an interrupted ledger write",
                    path: path.to_path_buf(),
                    source,
                })
            })?;
        }
        Ok(limit)
    }
}

/// What an opening does when it meets a frame no interrupted append could have
/// left.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OnUnreadableFrame {
    /// Refuse to open. This is [`LedgerStore::open`].
    Refuse,
    /// Discard from that frame to the end of the journal, and report it. This
    /// is [`LedgerStore::open_and_repair`].
    Discard,
}

/// Returns every client slot's deduplication progress.
fn deduplication_progress_of(ledger: &Ledger) -> BTreeMap<ClientId, DeduplicationProgress> {
    ledger
        .view()
        .sessions
        .into_iter()
        .map(|session| {
            (
                session.client_id,
                DeduplicationProgress {
                    session_epoch: session.session_epoch,
                    completed: session.cached.map(|(sequence, _, _)| sequence),
                },
            )
        })
        .collect()
}

/// Refuses a ledger that would move any client slot's deduplication state
/// backwards.
///
/// A slot that disappears is the same failure as one whose progress decreases:
/// both let a request identity the store has already answered execute again.
fn verify_deduplication_dominates(
    acknowledged: &BTreeMap<ClientId, DeduplicationProgress>,
    offered: &BTreeMap<ClientId, DeduplicationProgress>,
) -> Result<(), LedgerStoreError> {
    for (client_id, progress) in acknowledged {
        let found = offered.get(client_id).copied();
        if found.is_none_or(|offered_progress| offered_progress < *progress) {
            return Err(LedgerStoreError::DeduplicationRegression {
                client_id: *client_id,
                acknowledged: *progress,
                offered: found,
            });
        }
    }
    Ok(())
}

/// The step a fault is armed at.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WriteFaultSite {
    BeforeFirstByte,
    AfterBytes,
    AtFileSync,
    BeforeRename,
    AfterRename,
}

impl WriteFaultSite {
    const fn matches(self, fault: WriteFault) -> bool {
        matches!(
            (self, fault),
            (Self::BeforeFirstByte, WriteFault::BeforeFirstByte)
                | (Self::AfterBytes, WriteFault::AfterBytes(_))
                | (Self::AtFileSync, WriteFault::AtFileSync)
                | (Self::BeforeRename, WriteFault::BeforeRename)
                | (Self::AfterRename, WriteFault::AfterRename)
        )
    }
}

/// Result of scanning a journal's bytes.
struct JournalScan {
    /// Newest committed image, if the journal holds one.
    image: Option<(Ledger, LogIndex)>,
    /// Number of committed frames.
    committed_frames: u64,
    /// Byte length of the committed prefix.
    committed_len: usize,
    /// Residue found after the committed prefix.
    torn_tail: Option<TornTail>,
}

/// Reads the header, then every committed frame, stopping at the first residue.
fn scan_journal(bytes: &[u8], config: LedgerConfig) -> Result<JournalScan, LedgerStoreError> {
    verify_header(bytes, config)?;

    let mut offset = HEADER_LEN;
    let mut image = None;
    let mut committed_frames = 0_u64;
    let mut previous_index: Option<LogIndex> = None;

    let torn_tail = loop {
        let rest = &bytes[offset..];
        if rest.is_empty() {
            break None;
        }
        let frame = match read_frame(rest) {
            Ok(frame) => frame,
            Err(tail) => break Some(tail),
        };

        let (applied_index, snapshot) =
            decode_snapshot(frame.image).map_err(LedgerStoreError::Image)?;
        let applied_index = LogIndex(applied_index);
        if let Some(previous) = previous_index {
            if applied_index < previous {
                return Err(LedgerStoreError::NonMonotonicAppliedIndex {
                    previous,
                    found: applied_index,
                });
            }
        }
        let ledger = Ledger::from_snapshot(config, snapshot).map_err(LedgerStoreError::Snapshot)?;

        previous_index = Some(applied_index);
        image = Some((ledger, applied_index));
        committed_frames += 1;
        offset += frame.len;
    };

    Ok(JournalScan {
        image,
        committed_frames,
        committed_len: offset,
        torn_tail,
    })
}

/// One committed frame's image and total length.
struct Frame<'a> {
    image: &'a [u8],
    len: usize,
}

/// Reads one frame from the front of `bytes`, or says why it is not committed.
fn read_frame(bytes: &[u8]) -> Result<Frame<'_>, TornTail> {
    let Some(begin) = bytes.get(..BEGIN_LEN) else {
        return Err(TornTail::PartialBeginRecord);
    };
    if begin[..4] != BEGIN_MAGIC
        || begin[4] != JOURNAL_FORMAT_VERSION
        || read_u32(&begin[13..17]) != crc32(&begin[..13])
    {
        return Err(TornTail::BeginRecordCorrupt);
    }

    let image_len = read_u32(&begin[5..9]) as usize;
    let image_crc = read_u32(&begin[9..13]);
    let Some(image) = bytes.get(BEGIN_LEN..BEGIN_LEN + image_len) else {
        return Err(TornTail::PartialImage);
    };
    if crc32(image) != image_crc {
        return Err(TornTail::ImageCorrupt);
    }

    let commit_start = BEGIN_LEN + image_len;
    let available = bytes.len() - commit_start;
    if available == 0 {
        return Err(TornTail::MissingCommitRecord);
    }
    let Some(commit) = bytes.get(commit_start..commit_start + COMMIT_LEN) else {
        return Err(TornTail::PartialCommitRecord);
    };
    if commit[..4] != COMMIT_MAGIC
        || commit[4] != JOURNAL_FORMAT_VERSION
        || read_u32(&commit[9..13]) != crc32(&commit[..9])
        || read_u32(&commit[5..9]) != crc32(&bytes[..commit_start])
    {
        return Err(TornTail::CommitRecordCorrupt);
    }

    Ok(Frame {
        image,
        len: commit_start + COMMIT_LEN,
    })
}

/// Validates the journal header against `config`.
fn verify_header(bytes: &[u8], config: LedgerConfig) -> Result<(), LedgerStoreError> {
    let Some(header) = bytes.get(..HEADER_LEN) else {
        return Err(LedgerStoreError::HeaderTruncated {
            length: bytes.len() as u64,
        });
    };
    if header[..4] != JOURNAL_MAGIC {
        let mut magic = [0_u8; 4];
        magic.copy_from_slice(&header[..4]);
        return Err(LedgerStoreError::NotALedgerJournal { magic });
    }
    if header[4] != JOURNAL_FORMAT_VERSION {
        return Err(LedgerStoreError::UnsupportedFormatVersion { version: header[4] });
    }
    let expected = read_u32(&header[17..21]);
    let found = crc32(&header[..17]);
    if expected != found {
        return Err(LedgerStoreError::HeaderChecksumMismatch { expected, found });
    }

    let journal_max_clients = read_u32(&header[5..9]);
    let journal_max_accounts = read_u64(&header[9..17]);
    let requested_max_accounts = config.max_accounts() as u64;
    if journal_max_clients != config.max_clients() || journal_max_accounts != requested_max_accounts
    {
        return Err(LedgerStoreError::ConfigMismatch {
            journal_max_clients,
            journal_max_accounts,
            requested_max_clients: config.max_clients(),
            requested_max_accounts,
        });
    }
    Ok(())
}

/// Encodes the journal header for `config`.
fn encode_header(config: LedgerConfig) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER_LEN);
    bytes.extend_from_slice(&JOURNAL_MAGIC);
    bytes.push(JOURNAL_FORMAT_VERSION);
    bytes.extend_from_slice(&config.max_clients().to_be_bytes());
    bytes.extend_from_slice(&(config.max_accounts() as u64).to_be_bytes());
    let checksum = crc32(&bytes);
    bytes.extend_from_slice(&checksum.to_be_bytes());
    bytes
}

/// Encodes one whole transaction frame.
fn encode_frame(ledger: &Ledger, applied_index: LogIndex) -> Result<Vec<u8>, LedgerStoreError> {
    let image =
        encode_snapshot(applied_index.0, &ledger.snapshot()).map_err(LedgerStoreError::Image)?;
    let image_len = u32::try_from(image.len()).map_err(|_| LedgerStoreError::ImageTooLarge {
        length: image.len() as u64,
    })?;

    let mut frame = Vec::with_capacity(BEGIN_LEN + image.len() + COMMIT_LEN);
    frame.extend_from_slice(&BEGIN_MAGIC);
    frame.push(JOURNAL_FORMAT_VERSION);
    frame.extend_from_slice(&image_len.to_be_bytes());
    frame.extend_from_slice(&crc32(&image).to_be_bytes());
    let begin_checksum = crc32(&frame);
    frame.extend_from_slice(&begin_checksum.to_be_bytes());
    frame.extend_from_slice(&image);

    let frame_checksum = crc32(&frame);
    let mut commit = Vec::with_capacity(COMMIT_LEN);
    commit.extend_from_slice(&COMMIT_MAGIC);
    commit.push(JOURNAL_FORMAT_VERSION);
    commit.extend_from_slice(&frame_checksum.to_be_bytes());
    let commit_checksum = crc32(&commit);
    commit.extend_from_slice(&commit_checksum.to_be_bytes());

    frame.extend_from_slice(&commit);
    Ok(frame)
}

/// Creates the journal by staging its header and renaming it into place.
///
/// Renaming rather than creating-then-writing is what closes the crash window
/// between the two. A journal that existed but held no header could never be
/// completed by a later open — the file exists, so creation would never run
/// again — and the directory would be bricked by a crash inside a
/// two-statement function. With a rename, an interrupted creation leaves only a
/// staging file, the next open sweeps it, and creation runs properly.
fn create_journal(directory: &Path, config: LedgerConfig) -> Result<(), LedgerStoreError> {
    let staged_path = staged_path(directory);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&staged_path)
        .map_err(|source| LedgerStoreError::Io {
            operation: "stage the ledger journal header",
            path: staged_path.clone(),
            source,
        })?;
    file.write_all(&encode_header(config))
        .and_then(|()| file.sync_data())
        .map_err(|source| LedgerStoreError::Io {
            operation: "write the ledger journal header",
            path: staged_path.clone(),
            source,
        })?;
    drop(file);

    let journal_path = directory.join(JOURNAL_FILE_NAME);
    fs::rename(&staged_path, &journal_path).map_err(|source| LedgerStoreError::Io {
        operation: "publish the staged ledger journal",
        path: journal_path,
        source,
    })?;
    sync_directory(directory)
}

/// Discards an uncommitted tail and makes the shortened file durable.
fn truncate_journal(journal_path: &Path, committed_len: usize) -> Result<(), LedgerStoreError> {
    let file = OpenOptions::new()
        .write(true)
        .open(journal_path)
        .map_err(|source| LedgerStoreError::Io {
            operation: "open the ledger journal for truncation",
            path: journal_path.to_path_buf(),
            source,
        })?;
    file.set_len(committed_len as u64)
        .and_then(|()| file.sync_all())
        .map_err(|source| LedgerStoreError::Io {
            operation: "truncate the ledger journal's uncommitted tail",
            path: journal_path.to_path_buf(),
            source,
        })
}

/// Makes a directory's entries durable.
fn sync_directory(directory: &Path) -> Result<(), LedgerStoreError> {
    File::open(directory)
        .and_then(|file| file.sync_all())
        .map_err(|source| LedgerStoreError::Io {
            operation: "sync the ledger store directory",
            path: directory.to_path_buf(),
            source,
        })
}

/// Staging path for a rewrite or a creation.
fn staged_path(directory: &Path) -> PathBuf {
    directory.join(STAGED_FILE_NAME)
}

/// Removes every file this store could have staged, returning whether it
/// removed any.
///
/// The rule is wide on purpose: anything beside the journal whose name begins
/// with the journal's name and a dot is residue, whoever wrote it. That covers
/// the staging name, and it covers the process-scoped names an earlier shape of
/// this store used — names the process that finds them can never have written,
/// and so could never remove.
///
/// Removing a file this incarnation did not write is safe because there is no
/// other writer. The directory ownership discipline is what supplies that: a
/// replica takes `rafter-storage`'s operating-system lock over its Raft store
/// directory before it opens this journal and holds it for the process's life,
/// so a second process is refused before it reaches this directory at all. At
/// the moment this runs there is exactly one live writer, and every staging
/// file present was abandoned by an incarnation that is gone.
fn sweep_staged_files(directory: &Path) -> Result<bool, LedgerStoreError> {
    let prefix = format!("{JOURNAL_FILE_NAME}.");
    let entries = fs::read_dir(directory).map_err(|source| LedgerStoreError::Io {
        operation: "read the ledger store directory",
        path: directory.to_path_buf(),
        source,
    })?;

    let mut removed = false;
    for entry in entries {
        let entry = entry.map_err(|source| LedgerStoreError::Io {
            operation: "read a ledger store directory entry",
            path: directory.to_path_buf(),
            source,
        })?;
        // A name this store cannot have written is left alone, which includes
        // any name that is not valid UTF-8.
        let name = entry.file_name();
        if !name.to_str().is_some_and(|name| name.starts_with(&prefix)) {
            continue;
        }
        let path = entry.path();
        match fs::remove_file(&path) {
            Ok(()) => removed = true,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(source) => {
                return Err(LedgerStoreError::Io {
                    operation: "remove an abandoned staging file",
                    path,
                    source,
                })
            }
        }
    }
    Ok(removed)
}

fn read_u32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("callers pass four bytes"))
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_be_bytes(bytes.try_into().expect("callers pass eight bytes"))
}

/// Direct access to the journal's bytes, for crash tests only.
///
/// Everything here reaches past [`LedgerStore`] and reads or rewrites the file
/// the store owns. That is not a capability a durable application ever needs: an
/// application commits through [`LedgerStore::commit`] and
/// [`LedgerStore::replace`] and reads through [`LedgerStore::open`], and nothing
/// else in this crate calls into this module.
///
/// It is a named public module rather than a hidden item because the honest
/// statement is that these functions *are* reachable, and keeping them out of
/// the rendered documentation would not change that. A `#[doc(hidden)]`
/// function sitting beside the store's own API reads at the call site exactly
/// like API; a call that has to name `raw_journal` says what it is doing every
/// time it appears, and greps for one word. The crate's dependency boundary
/// forbids gating this behind a feature or an internal hook — a consumer
/// manifest must resolve like an external user's — so the guard is the name,
/// this paragraph, and review.
///
/// Nothing here validates anything. The store's own checks are what a corrupted
/// journal is aimed at, so a caller is responsible for the bytes being the ones
/// it means to present.
pub mod raw_journal {
    use super::{File, LedgerStoreError, OpenOptions, Path, Read, Write, JOURNAL_FILE_NAME};

    /// Reads the journal's raw bytes.
    ///
    /// Crash tests corrupt committed frames to prove the checksums are load
    /// bearing, which needs the exact bytes the store wrote.
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be read.
    pub fn read(directory: &Path) -> Result<Vec<u8>, LedgerStoreError> {
        let path = directory.join(JOURNAL_FILE_NAME);
        let mut bytes = Vec::new();
        File::open(&path)
            .and_then(|mut file| file.read_to_end(&mut bytes))
            .map_err(|source| LedgerStoreError::Io {
                operation: "read the ledger journal",
                path,
                source,
            })?;
        Ok(bytes)
    }

    /// Overwrites the journal's raw bytes.
    ///
    /// This is the corruption half of [`read`].
    ///
    /// # Errors
    ///
    /// Returns an error when the journal cannot be written.
    pub fn write(directory: &Path, bytes: &[u8]) -> Result<(), LedgerStoreError> {
        let path = directory.join(JOURNAL_FILE_NAME);
        OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&path)
            .and_then(|mut file| file.write_all(bytes).and_then(|()| file.sync_all()))
            .map_err(|source| LedgerStoreError::Io {
                operation: "rewrite the ledger journal",
                path,
                source,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{crc32, CRC32_POLYNOMIAL};

    /// A second implementation, deliberately written a different way.
    ///
    /// The store's own `crc32` folds the polynomial into a running state; this
    /// one shifts the message through a register. Agreement between two shapes
    /// is worth more than agreement between one shape and itself.
    fn reference_crc32(bytes: &[u8]) -> u32 {
        let mut state = u32::MAX;
        for byte in bytes {
            let mut mask = 1_u8;
            while mask != 0 {
                let bit = u32::from(*byte & mask != 0);
                let top = state & 1;
                state >>= 1;
                if top ^ bit == 1 {
                    state ^= CRC32_POLYNOMIAL;
                }
                mask <<= 1;
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
        for length in 0..64_usize {
            sample.push(u8::try_from(length * 7 % 251).expect("the modulus fits a byte"));
            assert_eq!(
                crc32(&sample),
                reference_crc32(&sample),
                "the two implementations disagree at length {length}"
            );
        }
    }
}
