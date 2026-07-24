use std::{error::Error, fmt};

use crate::{
    AccountId, Amount, BusinessRejection, ClientId, Command, LedgerSnapshot, Mutation,
    MutationResult, RequestIdentity, Sequence, SessionEpoch, SessionView,
};

/// Version byte of the replicated command frame.
const COMMAND_FORMAT_VERSION: u8 = 1;
/// Version byte of the application snapshot frame.
const SNAPSHOT_FORMAT_VERSION: u8 = 1;

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
    /// A collection length does not fit this platform's index type.
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
        Command::Execute { request, mutation } => {
            bytes.push(COMMAND_EXECUTE);
            put_client(&mut bytes, request.client_id);
            put_u64(&mut bytes, request.session_epoch.get());
            put_u64(&mut bytes, request.sequence.get());
            put_mutation(&mut bytes, mutation);
        }
    }
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

/// Encodes an application snapshot, including every session and cached
/// completion, into its versioned byte frame.
///
/// # Errors
///
/// Returns a codec error when the snapshot holds more accounts or sessions
/// than the frame's length fields can represent.
pub fn encode_snapshot(
    applied_index: u64,
    snapshot: &LedgerSnapshot,
) -> Result<Vec<u8>, LedgerCodecError> {
    let accounts = snapshot.accounts();
    let sessions = snapshot.sessions();

    let mut bytes = vec![SNAPSHOT_FORMAT_VERSION];
    put_u64(&mut bytes, applied_index);
    bytes.extend_from_slice(&snapshot.successful_deposits().to_be_bytes());
    put_len(&mut bytes, accounts.len())?;
    for (account_id, balance) in accounts {
        put_u64(&mut bytes, account_id.get());
        put_u64(&mut bytes, *balance);
    }
    put_len(&mut bytes, sessions.len())?;
    for session in &sessions {
        put_client(&mut bytes, session.client_id);
        put_u64(&mut bytes, session.session_epoch.get());
        match &session.cached {
            None => bytes.push(NO_CACHED_COMPLETION),
            Some((sequence, mutation, result)) => {
                bytes.push(CACHED_COMPLETION);
                put_u64(&mut bytes, sequence.get());
                put_mutation(&mut bytes, mutation);
                put_result(&mut bytes, result);
            }
        }
    }
    Ok(bytes)
}

/// Decodes an application snapshot frame into its applied index and the
/// opaque model snapshot.
///
/// # Errors
///
/// Returns a codec error when the frame is truncated, carries trailing bytes,
/// names an unknown kind, or violates a nonzero field of the contract.
pub fn decode_snapshot(payload: &[u8]) -> Result<(u64, LedgerSnapshot), LedgerCodecError> {
    let mut cursor = Cursor::new(payload);
    let version = cursor.take_u8()?;
    if version != SNAPSHOT_FORMAT_VERSION {
        return Err(LedgerCodecError::UnsupportedSnapshotVersion { version });
    }

    let applied_index = cursor.take_u64()?;
    let successful_deposits = cursor.take_u128()?;

    let account_count = cursor.take_len()?;
    let mut accounts = Vec::with_capacity(account_count);
    for _ in 0..account_count {
        accounts.push((AccountId::new(cursor.take_u64()?), cursor.take_u64()?));
    }

    let session_count = cursor.take_len()?;
    let mut sessions = Vec::with_capacity(session_count);
    for _ in 0..session_count {
        sessions.push(SessionView {
            client_id: cursor.take_client()?,
            session_epoch: cursor.take_session_epoch()?,
            cached: cursor.take_cached_completion()?,
        });
    }
    cursor.finish()?;

    Ok((
        applied_index,
        LedgerSnapshot::from_parts(accounts, sessions, successful_deposits),
    ))
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn put_client(bytes: &mut Vec<u8>, client_id: ClientId) {
    bytes.extend_from_slice(&client_id.get().to_be_bytes());
}

fn put_len(bytes: &mut Vec<u8>, length: usize) -> Result<(), LedgerCodecError> {
    let field = u32::try_from(length).map_err(|_| LedgerCodecError::LengthOutOfRange {
        length: u64::try_from(length).unwrap_or(u64::MAX),
    })?;
    bytes.extend_from_slice(&field.to_be_bytes());
    Ok(())
}

fn put_mutation(bytes: &mut Vec<u8>, mutation: &Mutation) {
    match *mutation {
        Mutation::OpenAccount { account_id } => {
            bytes.push(MUTATION_OPEN_ACCOUNT);
            put_u64(bytes, account_id.get());
        }
        Mutation::Deposit { account_id, amount } => {
            bytes.push(MUTATION_DEPOSIT);
            put_u64(bytes, account_id.get());
            put_u64(bytes, amount.get());
        }
        Mutation::Transfer { from, to, amount } => {
            bytes.push(MUTATION_TRANSFER);
            put_u64(bytes, from.get());
            put_u64(bytes, to.get());
            put_u64(bytes, amount.get());
        }
        Mutation::CloseAccount { account_id } => {
            bytes.push(MUTATION_CLOSE_ACCOUNT);
            put_u64(bytes, account_id.get());
        }
    }
}

fn put_result(bytes: &mut Vec<u8>, result: &MutationResult) {
    match *result {
        MutationResult::AccountOpened => bytes.push(RESULT_ACCOUNT_OPENED),
        MutationResult::Deposited { balance } => {
            bytes.push(RESULT_DEPOSITED);
            put_u64(bytes, balance);
        }
        MutationResult::Transferred {
            from_balance,
            to_balance,
        } => {
            bytes.push(RESULT_TRANSFERRED);
            put_u64(bytes, from_balance);
            put_u64(bytes, to_balance);
        }
        MutationResult::AccountClosed => bytes.push(RESULT_ACCOUNT_CLOSED),
        MutationResult::Rejected(rejection) => {
            bytes.push(RESULT_REJECTED);
            bytes.push(rejection_tag(rejection));
        }
    }
}

const fn rejection_tag(rejection: BusinessRejection) -> u8 {
    match rejection {
        BusinessRejection::AccountAlreadyExists => REJECTION_ACCOUNT_ALREADY_EXISTS,
        BusinessRejection::AccountCapacityExceeded => REJECTION_ACCOUNT_CAPACITY_EXCEEDED,
        BusinessRejection::AccountNotFound => REJECTION_ACCOUNT_NOT_FOUND,
        BusinessRejection::SameAccount => REJECTION_SAME_ACCOUNT,
        BusinessRejection::InsufficientFunds => REJECTION_INSUFFICIENT_FUNDS,
        BusinessRejection::BalanceOverflow => REJECTION_BALANCE_OVERFLOW,
        BusinessRejection::SupplyOverflow => REJECTION_SUPPLY_OVERFLOW,
        BusinessRejection::AccountNotEmpty => REJECTION_ACCOUNT_NOT_EMPTY,
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn take(&mut self, required: usize) -> Result<&'a [u8], LedgerCodecError> {
        let Some((field, rest)) = self.bytes.split_at_checked(required) else {
            return Err(LedgerCodecError::TruncatedFrame {
                required,
                available: self.bytes.len(),
            });
        };
        self.bytes = rest;
        Ok(field)
    }

    fn take_u8(&mut self) -> Result<u8, LedgerCodecError> {
        Ok(self.take(1)?[0])
    }

    fn take_u32(&mut self) -> Result<u32, LedgerCodecError> {
        let field = self.take(4)?;
        Ok(u32::from_be_bytes(
            field.try_into().expect("four bytes were taken"),
        ))
    }

    fn take_u64(&mut self) -> Result<u64, LedgerCodecError> {
        let field = self.take(8)?;
        Ok(u64::from_be_bytes(
            field.try_into().expect("eight bytes were taken"),
        ))
    }

    fn take_u128(&mut self) -> Result<u128, LedgerCodecError> {
        let field = self.take(16)?;
        Ok(u128::from_be_bytes(
            field.try_into().expect("sixteen bytes were taken"),
        ))
    }

    fn take_len(&mut self) -> Result<usize, LedgerCodecError> {
        let length = self.take_u32()?;
        usize::try_from(length).map_err(|_| LedgerCodecError::LengthOutOfRange {
            length: u64::from(length),
        })
    }

    fn take_client(&mut self) -> Result<ClientId, LedgerCodecError> {
        Ok(ClientId::new(self.take_u32()?))
    }

    fn take_session_epoch(&mut self) -> Result<SessionEpoch, LedgerCodecError> {
        SessionEpoch::new(self.take_u64()?).ok_or(LedgerCodecError::ZeroValuedField {
            field: NonZeroField::SessionEpoch,
        })
    }

    fn take_sequence(&mut self) -> Result<Sequence, LedgerCodecError> {
        Sequence::new(self.take_u64()?).ok_or(LedgerCodecError::ZeroValuedField {
            field: NonZeroField::Sequence,
        })
    }

    fn take_amount(&mut self) -> Result<Amount, LedgerCodecError> {
        Amount::new(self.take_u64()?).ok_or(LedgerCodecError::ZeroValuedField {
            field: NonZeroField::Amount,
        })
    }

    fn take_mutation(&mut self) -> Result<Mutation, LedgerCodecError> {
        match self.take_u8()? {
            MUTATION_OPEN_ACCOUNT => Ok(Mutation::OpenAccount {
                account_id: AccountId::new(self.take_u64()?),
            }),
            MUTATION_DEPOSIT => Ok(Mutation::Deposit {
                account_id: AccountId::new(self.take_u64()?),
                amount: self.take_amount()?,
            }),
            MUTATION_TRANSFER => Ok(Mutation::Transfer {
                from: AccountId::new(self.take_u64()?),
                to: AccountId::new(self.take_u64()?),
                amount: self.take_amount()?,
            }),
            MUTATION_CLOSE_ACCOUNT => Ok(Mutation::CloseAccount {
                account_id: AccountId::new(self.take_u64()?),
            }),
            tag => Err(LedgerCodecError::UnknownMutationTag { tag }),
        }
    }

    fn take_result(&mut self) -> Result<MutationResult, LedgerCodecError> {
        match self.take_u8()? {
            RESULT_ACCOUNT_OPENED => Ok(MutationResult::AccountOpened),
            RESULT_DEPOSITED => Ok(MutationResult::Deposited {
                balance: self.take_u64()?,
            }),
            RESULT_TRANSFERRED => Ok(MutationResult::Transferred {
                from_balance: self.take_u64()?,
                to_balance: self.take_u64()?,
            }),
            RESULT_ACCOUNT_CLOSED => Ok(MutationResult::AccountClosed),
            RESULT_REJECTED => Ok(MutationResult::Rejected(self.take_rejection()?)),
            tag => Err(LedgerCodecError::UnknownResultTag { tag }),
        }
    }

    fn take_rejection(&mut self) -> Result<BusinessRejection, LedgerCodecError> {
        match self.take_u8()? {
            REJECTION_ACCOUNT_ALREADY_EXISTS => Ok(BusinessRejection::AccountAlreadyExists),
            REJECTION_ACCOUNT_CAPACITY_EXCEEDED => Ok(BusinessRejection::AccountCapacityExceeded),
            REJECTION_ACCOUNT_NOT_FOUND => Ok(BusinessRejection::AccountNotFound),
            REJECTION_SAME_ACCOUNT => Ok(BusinessRejection::SameAccount),
            REJECTION_INSUFFICIENT_FUNDS => Ok(BusinessRejection::InsufficientFunds),
            REJECTION_BALANCE_OVERFLOW => Ok(BusinessRejection::BalanceOverflow),
            REJECTION_SUPPLY_OVERFLOW => Ok(BusinessRejection::SupplyOverflow),
            REJECTION_ACCOUNT_NOT_EMPTY => Ok(BusinessRejection::AccountNotEmpty),
            tag => Err(LedgerCodecError::UnknownRejectionTag { tag }),
        }
    }

    fn take_cached_completion(
        &mut self,
    ) -> Result<Option<(Sequence, Mutation, MutationResult)>, LedgerCodecError> {
        match self.take_u8()? {
            NO_CACHED_COMPLETION => Ok(None),
            CACHED_COMPLETION => Ok(Some((
                self.take_sequence()?,
                self.take_mutation()?,
                self.take_result()?,
            ))),
            marker => Err(LedgerCodecError::UnknownCacheMarker { marker }),
        }
    }

    const fn finish(&self) -> Result<(), LedgerCodecError> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err(LedgerCodecError::TrailingBytes {
                remaining: self.bytes.len(),
            })
        }
    }
}
