//! Versioned byte frames for replicated lock commands and their responses.
//!
//! Every frame starts with a format version and ends exactly where its last
//! declared field ends. Decoding rebuilds the bounded types through their own
//! constructors, so a frame can never smuggle a zero token, an inadmissible
//! resource name, or a name longer than the inline bound into the model.

use std::{error::Error, fmt, str};

use crate::{
    ApplyDisposition, ApplyOutcome, ClientId, Command, FencingToken, LeaseDuration, LockRejection,
    LockResponse, LogicalTime, Operation, OperationResult, RequestFingerprint, RequestIdentity,
    RequestRejection, ResourceName, ResourceNameError, Sequence, SessionEpoch,
    MAX_RESOURCE_NAME_LEN,
};

/// Version byte of the replicated command frame.
const COMMAND_FORMAT_VERSION: u8 = 1;
/// Version byte of the client response frame.
const RESULT_FORMAT_VERSION: u8 = 1;

const COMMAND_OPEN_SESSION: u8 = 1;
const COMMAND_SUBMIT: u8 = 2;

const OPERATION_ACQUIRE: u8 = 1;
const OPERATION_RENEW: u8 = 2;
const OPERATION_RELEASE: u8 = 3;
const OPERATION_EXPIRE_THROUGH: u8 = 4;

const RESPONSE_SESSION_OPENED: u8 = 1;
const RESPONSE_OPERATION: u8 = 2;
const RESPONSE_REJECTED: u8 = 3;

const OPERATION_RESULT_ACQUIRED: u8 = 1;
const OPERATION_RESULT_RENEWED: u8 = 2;
const OPERATION_RESULT_RELEASED: u8 = 3;
const OPERATION_RESULT_EXPIRED: u8 = 4;
const OPERATION_RESULT_REJECTED: u8 = 5;

const LOCK_REJECTION_LOCK_HELD: u8 = 1;
const LOCK_REJECTION_LOCK_NOT_HELD: u8 = 2;
const LOCK_REJECTION_NOT_LOCK_HOLDER: u8 = 3;
const LOCK_REJECTION_FENCING_TOKEN_MISMATCH: u8 = 4;
const LOCK_REJECTION_LEASE_OVERFLOW: u8 = 5;
const LOCK_REJECTION_TOKEN_EXHAUSTED: u8 = 6;
const LOCK_REJECTION_RESOURCE_CAPACITY_EXCEEDED: u8 = 7;
const LOCK_REJECTION_LOGICAL_TIME_NOT_ADVANCED: u8 = 8;

const REQUEST_REJECTION_CLIENT_OUT_OF_RANGE: u8 = 1;
const REQUEST_REJECTION_SESSION_NOT_OPEN: u8 = 2;
const REQUEST_REJECTION_STALE_SESSION: u8 = 3;
const REQUEST_REJECTION_FUTURE_SESSION: u8 = 4;
const REQUEST_REJECTION_STALE_SEQUENCE: u8 = 5;
const REQUEST_REJECTION_SEQUENCE_GAP: u8 = 6;
const REQUEST_REJECTION_CONFLICTING_RETRY: u8 = 7;
const REQUEST_REJECTION_FINGERPRINT_MISMATCH: u8 = 8;

const DISPOSITION_SESSION_OPENED: u8 = 1;
const DISPOSITION_SESSION_REPLACED: u8 = 2;
const DISPOSITION_SESSION_ALREADY_OPEN: u8 = 3;
const DISPOSITION_APPLIED: u8 = 4;
const DISPOSITION_REPLAYED: u8 = 5;
const DISPOSITION_REJECTED: u8 = 6;

/// Malformed replicated command or response bytes.
///
/// Every variant describes a frame that a correct peer never produces. The
/// adapter surfaces them as state-machine errors so the group layer poisons
/// instead of guessing at application intent; a lock rule that merely refuses a
/// well-formed request is an [`crate::OperationResult`], never an error.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockCodecError {
    /// The frame declares a command format this build cannot decode.
    UnsupportedCommandVersion { version: u8 },
    /// The frame declares a response format this build cannot decode.
    UnsupportedResultVersion { version: u8 },
    /// The frame names an unknown command kind.
    UnknownCommandTag { tag: u8 },
    /// The frame names an unknown operation kind.
    UnknownOperationTag { tag: u8 },
    /// The frame names an unknown client response kind.
    UnknownResponseTag { tag: u8 },
    /// The frame names an unknown operation result.
    UnknownOperationResultTag { tag: u8 },
    /// The frame names an unknown lock rejection.
    UnknownLockRejectionTag { tag: u8 },
    /// The frame names an unknown request rejection.
    UnknownRequestRejectionTag { tag: u8 },
    /// The frame names an unknown apply disposition.
    UnknownDispositionTag { tag: u8 },
    /// A field that the contract requires to be nonzero decoded as zero.
    ZeroValuedField { field: NonZeroField },
    /// The frame declared a resource name longer than the inline bound.
    ResourceNameTooLong { declared: usize },
    /// The frame's resource name bytes are not an admissible name.
    InvalidResourceName { reason: ResourceNameError },
    /// The frame ended before a field was complete.
    TruncatedFrame { required: usize, available: usize },
    /// The frame carried bytes after its last declared field.
    TrailingBytes { remaining: usize },
}

/// Contract field that must never encode as zero.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NonZeroField {
    /// Session generation.
    SessionEpoch,
    /// Request sequence within a session.
    Sequence,
    /// Per-resource fencing token.
    FencingToken,
    /// Lease length in replicated logical time.
    LeaseDuration,
}

impl fmt::Display for NonZeroField {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::SessionEpoch => "session epoch",
            Self::Sequence => "request sequence",
            Self::FencingToken => "fencing token",
            Self::LeaseDuration => "lease duration",
        })
    }
}

impl fmt::Display for LockCodecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedCommandVersion { version } => {
                write!(formatter, "unsupported command format version {version}")
            }
            Self::UnsupportedResultVersion { version } => {
                write!(formatter, "unsupported result format version {version}")
            }
            Self::UnknownCommandTag { tag } => write!(formatter, "unknown command tag {tag}"),
            Self::UnknownOperationTag { tag } => write!(formatter, "unknown operation tag {tag}"),
            Self::UnknownResponseTag { tag } => write!(formatter, "unknown response tag {tag}"),
            Self::UnknownOperationResultTag { tag } => {
                write!(formatter, "unknown operation result tag {tag}")
            }
            Self::UnknownLockRejectionTag { tag } => {
                write!(formatter, "unknown lock rejection tag {tag}")
            }
            Self::UnknownRequestRejectionTag { tag } => {
                write!(formatter, "unknown request rejection tag {tag}")
            }
            Self::UnknownDispositionTag { tag } => {
                write!(formatter, "unknown apply disposition tag {tag}")
            }
            Self::ZeroValuedField { field } => write!(
                formatter,
                "{field} decoded as zero, which the contract forbids"
            ),
            Self::ResourceNameTooLong { declared } => write!(
                formatter,
                "frame declares a {declared} byte resource name, above the {MAX_RESOURCE_NAME_LEN} byte bound"
            ),
            Self::InvalidResourceName { reason } => {
                write!(formatter, "frame carries an inadmissible resource name: {reason:?}")
            }
            Self::TruncatedFrame {
                required,
                available,
            } => write!(
                formatter,
                "frame needs {required} more bytes but only {available} remain"
            ),
            Self::TrailingBytes { remaining } => write!(
                formatter,
                "frame carried {remaining} unexpected trailing bytes"
            ),
        }
    }
}

impl Error for LockCodecError {}

/// Encodes one replicated command into its versioned byte frame.
#[must_use]
pub fn encode_command(command: &Command) -> Vec<u8> {
    let mut bytes = vec![COMMAND_FORMAT_VERSION];
    match command {
        Command::OpenSession {
            client_id,
            session_epoch,
        } => {
            bytes.push(COMMAND_OPEN_SESSION);
            put_client(&mut bytes, *client_id);
            put_u64(&mut bytes, session_epoch.get());
        }
        Command::Submit { request, operation } => {
            bytes.push(COMMAND_SUBMIT);
            put_client(&mut bytes, request.client_id);
            put_u64(&mut bytes, request.session_epoch.get());
            put_u64(&mut bytes, request.sequence.get());
            put_u64(&mut bytes, request.fingerprint.get());
            put_operation(&mut bytes, operation);
        }
    }
    bytes
}

/// Decodes one replicated command frame.
///
/// # Errors
///
/// Returns a codec error when the frame is truncated, carries trailing bytes,
/// names an unknown kind, or violates a bound the contract requires.
pub fn decode_command(payload: &[u8]) -> Result<Command, LockCodecError> {
    let mut cursor = Cursor::new(payload);
    let version = cursor.take_u8()?;
    if version != COMMAND_FORMAT_VERSION {
        return Err(LockCodecError::UnsupportedCommandVersion { version });
    }

    let command = match cursor.take_u8()? {
        COMMAND_OPEN_SESSION => Command::OpenSession {
            client_id: cursor.take_client()?,
            session_epoch: cursor.take_session_epoch()?,
        },
        COMMAND_SUBMIT => Command::Submit {
            request: RequestIdentity {
                client_id: cursor.take_client()?,
                session_epoch: cursor.take_session_epoch()?,
                sequence: cursor.take_sequence()?,
                fingerprint: RequestFingerprint::from_digest(cursor.take_u64()?),
            },
            operation: cursor.take_operation()?,
        },
        tag => return Err(LockCodecError::UnknownCommandTag { tag }),
    };
    cursor.finish()?;
    Ok(command)
}

/// Encodes one client-visible outcome into its versioned byte frame.
///
/// A replicated result leaves the state machine on its way back to a client, so
/// it needs the same bounded, versioned treatment the command frame gets.
#[must_use]
pub fn encode_result(outcome: &ApplyOutcome) -> Vec<u8> {
    let mut bytes = vec![RESULT_FORMAT_VERSION];
    put_response(&mut bytes, &outcome.response);
    bytes.push(disposition_tag(outcome.disposition));
    bytes
}

/// Decodes one client-visible outcome frame.
///
/// # Errors
///
/// Returns a codec error when the frame is truncated, carries trailing bytes,
/// names an unknown kind, or violates a bound the contract requires.
pub fn decode_result(payload: &[u8]) -> Result<ApplyOutcome, LockCodecError> {
    let mut cursor = Cursor::new(payload);
    let version = cursor.take_u8()?;
    if version != RESULT_FORMAT_VERSION {
        return Err(LockCodecError::UnsupportedResultVersion { version });
    }
    let response = cursor.take_response()?;
    let disposition = cursor.take_disposition()?;
    cursor.finish()?;
    Ok(ApplyOutcome {
        response,
        disposition,
    })
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn put_client(bytes: &mut Vec<u8>, client_id: ClientId) {
    put_u32(bytes, client_id.get());
}

/// Length-prefixes the name so no name plus a following field can share an
/// encoding with a longer name.
fn put_resource(bytes: &mut Vec<u8>, resource: &ResourceName) {
    let length = u8::try_from(resource.len())
        .expect("resource names are bounded well below the byte maximum");
    bytes.push(length);
    bytes.extend_from_slice(resource.as_bytes());
}

fn put_operation(bytes: &mut Vec<u8>, operation: &Operation) {
    match operation {
        Operation::Acquire { resource, lease } => {
            bytes.push(OPERATION_ACQUIRE);
            put_resource(bytes, resource);
            put_u64(bytes, lease.get());
        }
        Operation::Renew {
            resource,
            token,
            lease,
        } => {
            bytes.push(OPERATION_RENEW);
            put_resource(bytes, resource);
            put_u64(bytes, token.get());
            put_u64(bytes, lease.get());
        }
        Operation::Release { resource, token } => {
            bytes.push(OPERATION_RELEASE);
            put_resource(bytes, resource);
            put_u64(bytes, token.get());
        }
        Operation::ExpireThrough { horizon } => {
            bytes.push(OPERATION_EXPIRE_THROUGH);
            put_u64(bytes, horizon.get());
        }
    }
}

fn put_response(bytes: &mut Vec<u8>, response: &LockResponse) {
    match response {
        LockResponse::SessionOpened { session_epoch } => {
            bytes.push(RESPONSE_SESSION_OPENED);
            put_u64(bytes, session_epoch.get());
        }
        LockResponse::Operation(result) => {
            bytes.push(RESPONSE_OPERATION);
            put_operation_result(bytes, *result);
        }
        LockResponse::Rejected(rejection) => {
            bytes.push(RESPONSE_REJECTED);
            put_request_rejection(bytes, *rejection);
        }
    }
}

fn put_operation_result(bytes: &mut Vec<u8>, result: OperationResult) {
    match result {
        OperationResult::Acquired { token, expiry } => {
            bytes.push(OPERATION_RESULT_ACQUIRED);
            put_u64(bytes, token.get());
            put_u64(bytes, expiry.get());
        }
        OperationResult::Renewed { token, expiry } => {
            bytes.push(OPERATION_RESULT_RENEWED);
            put_u64(bytes, token.get());
            put_u64(bytes, expiry.get());
        }
        OperationResult::Released => bytes.push(OPERATION_RESULT_RELEASED),
        OperationResult::Expired {
            released_locks,
            logical_time,
        } => {
            bytes.push(OPERATION_RESULT_EXPIRED);
            put_u32(bytes, released_locks);
            put_u64(bytes, logical_time.get());
        }
        OperationResult::Rejected(rejection) => {
            bytes.push(OPERATION_RESULT_REJECTED);
            put_lock_rejection(bytes, rejection);
        }
    }
}

fn put_lock_rejection(bytes: &mut Vec<u8>, rejection: LockRejection) {
    match rejection {
        LockRejection::LockHeld {
            owner,
            token,
            expiry,
        } => {
            bytes.push(LOCK_REJECTION_LOCK_HELD);
            put_client(bytes, owner);
            put_u64(bytes, token.get());
            put_u64(bytes, expiry.get());
        }
        LockRejection::LockNotHeld => bytes.push(LOCK_REJECTION_LOCK_NOT_HELD),
        LockRejection::NotLockHolder { owner } => {
            bytes.push(LOCK_REJECTION_NOT_LOCK_HOLDER);
            put_client(bytes, owner);
        }
        LockRejection::FencingTokenMismatch { current } => {
            bytes.push(LOCK_REJECTION_FENCING_TOKEN_MISMATCH);
            put_u64(bytes, current.get());
        }
        LockRejection::LeaseOverflow => bytes.push(LOCK_REJECTION_LEASE_OVERFLOW),
        LockRejection::TokenExhausted => bytes.push(LOCK_REJECTION_TOKEN_EXHAUSTED),
        LockRejection::ResourceCapacityExceeded => {
            bytes.push(LOCK_REJECTION_RESOURCE_CAPACITY_EXCEEDED);
        }
        LockRejection::LogicalTimeNotAdvanced { current } => {
            bytes.push(LOCK_REJECTION_LOGICAL_TIME_NOT_ADVANCED);
            put_u64(bytes, current.get());
        }
    }
}

fn put_request_rejection(bytes: &mut Vec<u8>, rejection: RequestRejection) {
    match rejection {
        RequestRejection::ClientOutOfRange => {
            bytes.push(REQUEST_REJECTION_CLIENT_OUT_OF_RANGE);
        }
        RequestRejection::SessionNotOpen => bytes.push(REQUEST_REJECTION_SESSION_NOT_OPEN),
        RequestRejection::StaleSession { current } => {
            bytes.push(REQUEST_REJECTION_STALE_SESSION);
            put_u64(bytes, current.get());
        }
        RequestRejection::FutureSession { current } => {
            bytes.push(REQUEST_REJECTION_FUTURE_SESSION);
            put_u64(bytes, current.get());
        }
        RequestRejection::StaleSequence { highest } => {
            bytes.push(REQUEST_REJECTION_STALE_SEQUENCE);
            put_u64(bytes, highest.get());
        }
        RequestRejection::SequenceGap { expected } => {
            bytes.push(REQUEST_REJECTION_SEQUENCE_GAP);
            put_u64(bytes, expected.get());
        }
        RequestRejection::ConflictingRetry => bytes.push(REQUEST_REJECTION_CONFLICTING_RETRY),
        RequestRejection::FingerprintMismatch { expected } => {
            bytes.push(REQUEST_REJECTION_FINGERPRINT_MISMATCH);
            put_u64(bytes, expected.get());
        }
    }
}

const fn disposition_tag(disposition: ApplyDisposition) -> u8 {
    match disposition {
        ApplyDisposition::SessionOpened => DISPOSITION_SESSION_OPENED,
        ApplyDisposition::SessionReplaced => DISPOSITION_SESSION_REPLACED,
        ApplyDisposition::SessionAlreadyOpen => DISPOSITION_SESSION_ALREADY_OPEN,
        ApplyDisposition::Applied => DISPOSITION_APPLIED,
        ApplyDisposition::Replayed => DISPOSITION_REPLAYED,
        ApplyDisposition::Rejected => DISPOSITION_REJECTED,
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn take(&mut self, required: usize) -> Result<&'a [u8], LockCodecError> {
        let Some((field, rest)) = self.bytes.split_at_checked(required) else {
            return Err(LockCodecError::TruncatedFrame {
                required,
                available: self.bytes.len(),
            });
        };
        self.bytes = rest;
        Ok(field)
    }

    fn take_u8(&mut self) -> Result<u8, LockCodecError> {
        Ok(self.take(1)?[0])
    }

    fn take_u32(&mut self) -> Result<u32, LockCodecError> {
        let field = self.take(4)?;
        Ok(u32::from_be_bytes(
            field.try_into().expect("four bytes were taken"),
        ))
    }

    fn take_u64(&mut self) -> Result<u64, LockCodecError> {
        let field = self.take(8)?;
        Ok(u64::from_be_bytes(
            field.try_into().expect("eight bytes were taken"),
        ))
    }

    fn take_client(&mut self) -> Result<ClientId, LockCodecError> {
        Ok(ClientId::new(self.take_u32()?))
    }

    fn take_session_epoch(&mut self) -> Result<SessionEpoch, LockCodecError> {
        SessionEpoch::new(self.take_u64()?).ok_or(LockCodecError::ZeroValuedField {
            field: NonZeroField::SessionEpoch,
        })
    }

    fn take_sequence(&mut self) -> Result<Sequence, LockCodecError> {
        Sequence::new(self.take_u64()?).ok_or(LockCodecError::ZeroValuedField {
            field: NonZeroField::Sequence,
        })
    }

    fn take_token(&mut self) -> Result<FencingToken, LockCodecError> {
        FencingToken::new(self.take_u64()?).ok_or(LockCodecError::ZeroValuedField {
            field: NonZeroField::FencingToken,
        })
    }

    fn take_lease(&mut self) -> Result<LeaseDuration, LockCodecError> {
        LeaseDuration::new(self.take_u64()?).ok_or(LockCodecError::ZeroValuedField {
            field: NonZeroField::LeaseDuration,
        })
    }

    /// Rebuilds a name through its own constructor.
    ///
    /// Admissibility is re-decided from the bytes rather than trusted, because
    /// a frame is the one place a name can arrive without having passed
    /// [`ResourceName::new`].
    fn take_resource(&mut self) -> Result<ResourceName, LockCodecError> {
        let declared = usize::from(self.take_u8()?);
        if declared > MAX_RESOURCE_NAME_LEN {
            return Err(LockCodecError::ResourceNameTooLong { declared });
        }
        let field = self.take(declared)?;
        let name = str::from_utf8(field).map_err(|_| LockCodecError::InvalidResourceName {
            reason: ResourceNameError::InvalidByte,
        })?;
        ResourceName::new(name).map_err(|reason| LockCodecError::InvalidResourceName { reason })
    }

    fn take_operation(&mut self) -> Result<Operation, LockCodecError> {
        match self.take_u8()? {
            OPERATION_ACQUIRE => Ok(Operation::Acquire {
                resource: self.take_resource()?,
                lease: self.take_lease()?,
            }),
            OPERATION_RENEW => Ok(Operation::Renew {
                resource: self.take_resource()?,
                token: self.take_token()?,
                lease: self.take_lease()?,
            }),
            OPERATION_RELEASE => Ok(Operation::Release {
                resource: self.take_resource()?,
                token: self.take_token()?,
            }),
            OPERATION_EXPIRE_THROUGH => Ok(Operation::ExpireThrough {
                horizon: LogicalTime::new(self.take_u64()?),
            }),
            tag => Err(LockCodecError::UnknownOperationTag { tag }),
        }
    }

    fn take_response(&mut self) -> Result<LockResponse, LockCodecError> {
        match self.take_u8()? {
            RESPONSE_SESSION_OPENED => Ok(LockResponse::SessionOpened {
                session_epoch: self.take_session_epoch()?,
            }),
            RESPONSE_OPERATION => Ok(LockResponse::Operation(self.take_operation_result()?)),
            RESPONSE_REJECTED => Ok(LockResponse::Rejected(self.take_request_rejection()?)),
            tag => Err(LockCodecError::UnknownResponseTag { tag }),
        }
    }

    fn take_operation_result(&mut self) -> Result<OperationResult, LockCodecError> {
        match self.take_u8()? {
            OPERATION_RESULT_ACQUIRED => Ok(OperationResult::Acquired {
                token: self.take_token()?,
                expiry: LogicalTime::new(self.take_u64()?),
            }),
            OPERATION_RESULT_RENEWED => Ok(OperationResult::Renewed {
                token: self.take_token()?,
                expiry: LogicalTime::new(self.take_u64()?),
            }),
            OPERATION_RESULT_RELEASED => Ok(OperationResult::Released),
            OPERATION_RESULT_EXPIRED => Ok(OperationResult::Expired {
                released_locks: self.take_u32()?,
                logical_time: LogicalTime::new(self.take_u64()?),
            }),
            OPERATION_RESULT_REJECTED => Ok(OperationResult::Rejected(self.take_lock_rejection()?)),
            tag => Err(LockCodecError::UnknownOperationResultTag { tag }),
        }
    }

    fn take_lock_rejection(&mut self) -> Result<LockRejection, LockCodecError> {
        match self.take_u8()? {
            LOCK_REJECTION_LOCK_HELD => Ok(LockRejection::LockHeld {
                owner: self.take_client()?,
                token: self.take_token()?,
                expiry: LogicalTime::new(self.take_u64()?),
            }),
            LOCK_REJECTION_LOCK_NOT_HELD => Ok(LockRejection::LockNotHeld),
            LOCK_REJECTION_NOT_LOCK_HOLDER => Ok(LockRejection::NotLockHolder {
                owner: self.take_client()?,
            }),
            LOCK_REJECTION_FENCING_TOKEN_MISMATCH => Ok(LockRejection::FencingTokenMismatch {
                current: self.take_token()?,
            }),
            LOCK_REJECTION_LEASE_OVERFLOW => Ok(LockRejection::LeaseOverflow),
            LOCK_REJECTION_TOKEN_EXHAUSTED => Ok(LockRejection::TokenExhausted),
            LOCK_REJECTION_RESOURCE_CAPACITY_EXCEEDED => {
                Ok(LockRejection::ResourceCapacityExceeded)
            }
            LOCK_REJECTION_LOGICAL_TIME_NOT_ADVANCED => Ok(LockRejection::LogicalTimeNotAdvanced {
                current: LogicalTime::new(self.take_u64()?),
            }),
            tag => Err(LockCodecError::UnknownLockRejectionTag { tag }),
        }
    }

    fn take_request_rejection(&mut self) -> Result<RequestRejection, LockCodecError> {
        match self.take_u8()? {
            REQUEST_REJECTION_CLIENT_OUT_OF_RANGE => Ok(RequestRejection::ClientOutOfRange),
            REQUEST_REJECTION_SESSION_NOT_OPEN => Ok(RequestRejection::SessionNotOpen),
            REQUEST_REJECTION_STALE_SESSION => Ok(RequestRejection::StaleSession {
                current: self.take_session_epoch()?,
            }),
            REQUEST_REJECTION_FUTURE_SESSION => Ok(RequestRejection::FutureSession {
                current: self.take_session_epoch()?,
            }),
            REQUEST_REJECTION_STALE_SEQUENCE => Ok(RequestRejection::StaleSequence {
                highest: self.take_sequence()?,
            }),
            REQUEST_REJECTION_SEQUENCE_GAP => Ok(RequestRejection::SequenceGap {
                expected: self.take_sequence()?,
            }),
            REQUEST_REJECTION_CONFLICTING_RETRY => Ok(RequestRejection::ConflictingRetry),
            REQUEST_REJECTION_FINGERPRINT_MISMATCH => Ok(RequestRejection::FingerprintMismatch {
                expected: RequestFingerprint::from_digest(self.take_u64()?),
            }),
            tag => Err(LockCodecError::UnknownRequestRejectionTag { tag }),
        }
    }

    fn take_disposition(&mut self) -> Result<ApplyDisposition, LockCodecError> {
        match self.take_u8()? {
            DISPOSITION_SESSION_OPENED => Ok(ApplyDisposition::SessionOpened),
            DISPOSITION_SESSION_REPLACED => Ok(ApplyDisposition::SessionReplaced),
            DISPOSITION_SESSION_ALREADY_OPEN => Ok(ApplyDisposition::SessionAlreadyOpen),
            DISPOSITION_APPLIED => Ok(ApplyDisposition::Applied),
            DISPOSITION_REPLAYED => Ok(ApplyDisposition::Replayed),
            DISPOSITION_REJECTED => Ok(ApplyDisposition::Rejected),
            tag => Err(LockCodecError::UnknownDispositionTag { tag }),
        }
    }

    const fn finish(&self) -> Result<(), LockCodecError> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(LockCodecError::TrailingBytes {
                remaining: self.bytes.len(),
            })
        }
    }
}
