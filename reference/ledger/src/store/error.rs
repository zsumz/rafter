//! Every way a durable ledger store operation refuses, and how each one reads.
//!
//! The refusals are the store's contract with an operator, so the rendering is
//! part of the artifact rather than a debug aid.

use std::{error::Error, fmt, path::PathBuf};

use rafter::LogIndex;

use crate::{ClientId, LedgerCodecError, SnapshotError};

use super::{
    damage::TornTail, domination::DeduplicationProgress, fault::WriteFault, format::HEADER_LEN,
};

// Imported for the intra-doc links the prose below carries. Splitting the
// store's one file moved these types into sibling modules; not one sentence
// that names them changed.
#[allow(unused_imports)]
use super::{fault::FaultPlan, LedgerStore};

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
    /// The journal holds a frame recovery cannot show an interrupted append left.
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
    /// A frame declares a format version this build cannot read.
    ///
    /// This needs no corruption: a newer build appending over a header this one
    /// still reads produces it from entirely healthy bytes. It is separate from
    /// [`LedgerStoreError::UnreadableFrame`] because it is refused by *both*
    /// entry points — a downgrade is not damage, so the repair that discards
    /// damage must not discard it either.
    UnsupportedFrameVersion {
        /// Byte offset the frame begins at.
        offset: u64,
        /// Version byte found in its begin record.
        version: u8,
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
                "the frame at byte {offset} is {corruption}, which recovery cannot show an \
                 interrupted append left; \
                 {committed_frames} frames were readable before it and the {unreadable_bytes} bytes \
                 from there on may hold committed transactions"
            ),
            Self::UnsupportedFrameVersion { offset, version } => write!(
                formatter,
                "the frame at byte {offset} declares format version {version}, which this build \
                 cannot read; it is a newer build's committed work, not damage to discard"
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
            | Self::UnsupportedFrameVersion { .. }
            | Self::Snapshot(_)
            | Self::NonMonotonicAppliedIndex { .. }
            | Self::DeduplicationRegression { .. }
            | Self::ImageTooLarge { .. }
            | Self::StoreRequiresReopen
            | Self::InjectedFault { .. } => None,
        }
    }
}
