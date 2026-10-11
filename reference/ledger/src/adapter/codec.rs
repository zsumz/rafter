//! Version 1 command and snapshot encoding, with a shared decoding grammar.

mod input;
mod read;
mod snapshot;
mod write;
pub(crate) use write::{encode_snapshot, encode_snapshot_stream, SnapshotEncodingError};

use read::Cursor;
pub(crate) use snapshot::{decode_snapshot, decode_snapshot_stream, snapshot_payload_limit};

use std::{error::Error, fmt};

use crate::{Command, RequestIdentity};

/// Version byte of the replicated command frame.
const COMMAND_FORMAT_VERSION: u8 = 1;
/// Version byte of the application snapshot frame.
pub(super) const SNAPSHOT_FORMAT_VERSION: u8 = 1;

const COMMAND_OPEN_SESSION: u8 = 1;
const COMMAND_EXECUTE: u8 = 2;

const MUTATION_OPEN_ACCOUNT: u8 = 1;
const MUTATION_DEPOSIT: u8 = 2;
const MUTATION_TRANSFER: u8 = 3;
const MUTATION_CLOSE_ACCOUNT: u8 = 4;

const RESULT_ACCOUNT_OPENED: u8 = 1;
const RESULT_DEPOSITED: u8 = 2;
const RESULT_TRANSFERRED: u8 = 3;
const RESULT_ACCOUNT_CLOSED: u8 = 4;
const RESULT_REJECTED: u8 = 5;

const REJECTION_ACCOUNT_ALREADY_EXISTS: u8 = 1;
const REJECTION_ACCOUNT_CAPACITY_EXCEEDED: u8 = 2;
const REJECTION_ACCOUNT_NOT_FOUND: u8 = 3;
const REJECTION_SAME_ACCOUNT: u8 = 4;
const REJECTION_INSUFFICIENT_FUNDS: u8 = 5;
const REJECTION_BALANCE_OVERFLOW: u8 = 6;
const REJECTION_SUPPLY_OVERFLOW: u8 = 7;
const REJECTION_ACCOUNT_NOT_EMPTY: u8 = 8;

const NO_CACHED_COMPLETION: u8 = 0;
const CACHED_COMPLETION: u8 = 1;

/// Malformed replicated command or snapshot bytes.
///
/// Every variant describes a frame a correct peer never produces. The adapter
/// surfaces them as state-machine errors so the group layer poisons instead of
/// guessing at application intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LedgerCodecError {
    /// The frame declares a command format this build cannot decode.
    UnsupportedCommandVersion { version: u8 },
    /// The frame declares a snapshot format this build cannot decode.
    UnsupportedSnapshotVersion { version: u8 },
    /// The frame names an unknown command kind.
    UnknownCommandTag { tag: u8 },
    /// The frame names an unknown mutation kind.
    UnknownMutationTag { tag: u8 },
    /// The frame names an unknown mutation result.
    UnknownResultTag { tag: u8 },
    /// The frame names an unknown business rejection.
    UnknownRejectionTag { tag: u8 },
    /// The frame names an unknown cached-completion marker.
    UnknownCacheMarker { marker: u8 },
    /// A field that the contract requires to be nonzero decoded as zero.
    ZeroValuedField { field: NonZeroField },
    /// The frame ended before a field was complete.
    TruncatedFrame { required: usize, available: usize },
    /// The frame carried bytes after its last declared field.
    TrailingBytes { remaining: usize },
    /// A collection length or its minimum byte extent is not addressable here.
    LengthOutOfRange { length: u64 },
}

/// Contract field that must never encode as zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NonZeroField {
    /// Session generation.
    SessionEpoch,
    /// Request sequence within a session.
    Sequence,
    /// Ledger amount.
    Amount,
}

impl fmt::Display for NonZeroField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SessionEpoch => "session epoch",
            Self::Sequence => "request sequence",
            Self::Amount => "ledger amount",
        })
    }
}

impl fmt::Display for LedgerCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedCommandVersion { version } => {
                write!(formatter, "unsupported command format version {version}")
            }
            Self::UnsupportedSnapshotVersion { version } => {
                write!(formatter, "unsupported snapshot format version {version}")
            }
            Self::UnknownCommandTag { tag } => write!(formatter, "unknown command tag {tag}"),
            Self::UnknownMutationTag { tag } => write!(formatter, "unknown mutation tag {tag}"),
            Self::UnknownResultTag { tag } => {
                write!(formatter, "unknown mutation result tag {tag}")
            }
            Self::UnknownRejectionTag { tag } => {
                write!(formatter, "unknown business rejection tag {tag}")
            }
            Self::UnknownCacheMarker { marker } => {
                write!(formatter, "unknown cached-completion marker {marker}")
            }
            Self::ZeroValuedField { field } => {
                write!(
                    formatter,
                    "{field} decoded as zero, which the contract forbids"
                )
            }
            Self::TruncatedFrame {
                required,
                available,
            } => write!(
                formatter,
                "frame needs {required} more bytes but only {available} remain"
            ),
            Self::TrailingBytes { remaining } => {
                write!(
                    formatter,
                    "frame carried {remaining} unexpected trailing bytes"
                )
            }
            Self::LengthOutOfRange { length } => {
                write!(formatter, "encoded length {length} is not addressable here")
            }
        }
    }
}

impl Error for LedgerCodecError {}

/// Encodes one replicated command into its versioned byte frame.
pub fn encode_command(command: &Command) -> Vec<u8> {
    let mut bytes = Vec::new();
    write::command(&mut bytes, command).unwrap_or_else(|never| match never {});
    bytes
}

/// Decodes one replicated command frame.
///
/// # Errors
///
/// Returns a codec error when the frame is truncated, carries trailing bytes,
/// names an unknown kind, or violates a nonzero field of the contract.
pub fn decode_command(payload: &[u8]) -> Result<Command, LedgerCodecError> {
    let mut cursor = Cursor::new(payload);
    let version = cursor.take_u8()?;
    if version != COMMAND_FORMAT_VERSION {
        return Err(LedgerCodecError::UnsupportedCommandVersion { version });
    }

    let command = match cursor.take_u8()? {
        COMMAND_OPEN_SESSION => Command::OpenSession {
            client_id: cursor.take_client()?,
            session_epoch: cursor.take_session_epoch()?,
        },
        COMMAND_EXECUTE => Command::Execute {
            request: RequestIdentity {
                client_id: cursor.take_client()?,
                session_epoch: cursor.take_session_epoch()?,
                sequence: cursor.take_sequence()?,
            },
            mutation: cursor.take_mutation()?,
        },
        tag => return Err(LedgerCodecError::UnknownCommandTag { tag }),
    };
    cursor.finish()?;
    Ok(command)
}
