//! Version-1 output grammar shared by inline buffers and streaming sinks.

#[cfg(test)]
#[path = "write_tests.rs"]
mod tests;

use std::{
    convert::Infallible,
    io::{self, Write},
};

use super::{
    LedgerCodecError, CACHED_COMPLETION, COMMAND_EXECUTE, COMMAND_FORMAT_VERSION,
    COMMAND_OPEN_SESSION, MUTATION_CLOSE_ACCOUNT, MUTATION_DEPOSIT, MUTATION_OPEN_ACCOUNT,
    MUTATION_TRANSFER, NO_CACHED_COMPLETION, REJECTION_ACCOUNT_ALREADY_EXISTS,
    REJECTION_ACCOUNT_CAPACITY_EXCEEDED, REJECTION_ACCOUNT_NOT_EMPTY, REJECTION_ACCOUNT_NOT_FOUND,
    REJECTION_BALANCE_OVERFLOW, REJECTION_INSUFFICIENT_FUNDS, REJECTION_SAME_ACCOUNT,
    REJECTION_SUPPLY_OVERFLOW, RESULT_ACCOUNT_CLOSED, RESULT_ACCOUNT_OPENED, RESULT_DEPOSITED,
    RESULT_REJECTED, RESULT_TRANSFERRED, SNAPSHOT_FORMAT_VERSION,
};
use crate::{BusinessRejection, Command, Ledger, Mutation, MutationResult};

pub(super) trait Output {
    type Error;
    fn emit(&mut self, bytes: &[u8]) -> Result<(), Self::Error>;
}

impl Output for Vec<u8> {
    type Error = Infallible;
    fn emit(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.extend_from_slice(bytes);
        Ok(())
    }
}

struct Sink<'a, W>(&'a mut W);
impl<W: Write> Output for Sink<'_, W> {
    type Error = io::Error;
    fn emit(&mut self, bytes: &[u8]) -> Result<(), Self::Error> {
        self.0.write_all(bytes)
    }
}

pub(crate) enum SnapshotEncodingError {
    Codec(LedgerCodecError),
    Sink(io::Error),
}

pub(crate) fn encode_snapshot(at: u64, ledger: &Ledger) -> Result<Vec<u8>, LedgerCodecError> {
    let counts = counts(ledger)?;
    let mut bytes = Vec::new();
    snapshot(&mut bytes, at, ledger, counts).unwrap_or_else(|never| match never {});
    Ok(bytes)
}

pub(crate) fn encode_snapshot_stream(
    sink: &mut impl Write,
    at: u64,
    ledger: &Ledger,
) -> Result<(), SnapshotEncodingError> {
    let counts = counts(ledger).map_err(SnapshotEncodingError::Codec)?;
    snapshot(&mut Sink(sink), at, ledger, counts).map_err(SnapshotEncodingError::Sink)
}

fn counts(ledger: &Ledger) -> Result<(u32, u32), LedgerCodecError> {
    let narrow = |len: usize| {
        u32::try_from(len).map_err(|_| LedgerCodecError::LengthOutOfRange {
            length: u64::try_from(len).unwrap_or(u64::MAX),
        })
    };
    Ok((
        narrow(ledger.snapshot_accounts().len())?,
        narrow(ledger.snapshot_sessions().len())?,
    ))
}

fn snapshot<O: Output>(
    sink: &mut O,
    at: u64,
    ledger: &Ledger,
    counts: (u32, u32),
) -> Result<(), O::Error> {
    let mut out = Fields(sink);
    out.byte(SNAPSHOT_FORMAT_VERSION)?;
    out.u64(at)?;
    out.0.emit(&ledger.snapshot_supply().to_be_bytes())?;
    out.0.emit(&counts.0.to_be_bytes())?;
    for (id, balance) in ledger.snapshot_accounts() {
        out.u64(id.get())?;
        out.u64(balance)?;
    }
    out.0.emit(&counts.1.to_be_bytes())?;
    for session in ledger.snapshot_sessions() {
        out.0.emit(&session.client_id.get().to_be_bytes())?;
        out.u64(session.session_epoch.get())?;
        if let Some((sequence, mutation, result)) = &session.cached {
            out.byte(CACHED_COMPLETION)?;
            out.u64(sequence.get())?;
            out.mutation(mutation)?;
            out.result(result)?;
        } else {
            out.byte(NO_CACHED_COMPLETION)?;
        }
    }
    Ok(())
}

pub(super) fn command<O: Output>(sink: &mut O, command: &Command) -> Result<(), O::Error> {
    let mut out = Fields(sink);
    out.byte(COMMAND_FORMAT_VERSION)?;
    match command {
        Command::OpenSession {
            client_id,
            session_epoch,
        } => {
            out.byte(COMMAND_OPEN_SESSION)?;
            out.0.emit(&client_id.get().to_be_bytes())?;
            out.u64(session_epoch.get())?;
        }
        Command::Execute { request, mutation } => {
            out.byte(COMMAND_EXECUTE)?;
            out.0.emit(&request.client_id.get().to_be_bytes())?;
            out.u64(request.session_epoch.get())?;
            out.u64(request.sequence.get())?;
            out.mutation(mutation)?;
        }
    }
    Ok(())
}

struct Fields<'a, O>(&'a mut O);
impl<O: Output> Fields<'_, O> {
    fn byte(&mut self, value: u8) -> Result<(), O::Error> {
        self.0.emit(&[value])
    }
    fn u64(&mut self, value: u64) -> Result<(), O::Error> {
        self.0.emit(&value.to_be_bytes())
    }
    fn mutation(&mut self, mutation: &Mutation) -> Result<(), O::Error> {
        match *mutation {
            Mutation::OpenAccount { account_id } => {
                self.byte(MUTATION_OPEN_ACCOUNT)?;
                self.u64(account_id.get())?;
            }
            Mutation::Deposit { account_id, amount } => {
                self.byte(MUTATION_DEPOSIT)?;
                self.u64(account_id.get())?;
                self.u64(amount.get())?;
            }
            Mutation::Transfer { from, to, amount } => {
                self.byte(MUTATION_TRANSFER)?;
                self.u64(from.get())?;
                self.u64(to.get())?;
                self.u64(amount.get())?;
            }
            Mutation::CloseAccount { account_id } => {
                self.byte(MUTATION_CLOSE_ACCOUNT)?;
                self.u64(account_id.get())?;
            }
        }
        Ok(())
    }
    fn result(&mut self, result: &MutationResult) -> Result<(), O::Error> {
        match *result {
            MutationResult::AccountOpened => self.byte(RESULT_ACCOUNT_OPENED)?,
            MutationResult::Deposited { balance } => {
                self.byte(RESULT_DEPOSITED)?;
                self.u64(balance)?;
            }
            MutationResult::Transferred {
                from_balance,
                to_balance,
            } => {
                self.byte(RESULT_TRANSFERRED)?;
                self.u64(from_balance)?;
                self.u64(to_balance)?;
            }
            MutationResult::AccountClosed => self.byte(RESULT_ACCOUNT_CLOSED)?,
            MutationResult::Rejected(rejection) => {
                self.byte(RESULT_REJECTED)?;
                self.byte(rejection_tag(rejection))?;
            }
        }
        Ok(())
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
