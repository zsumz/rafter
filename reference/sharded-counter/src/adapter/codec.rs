use std::collections::BTreeMap;

use rafter::LogIndex;

use crate::{
    ClientId, CounterCommand, CounterRejection, CounterResult, RequestFingerprint, RequestIdentity,
    Sequence, SessionEpoch,
};

use super::state_machine::{
    Completed, CounterStateMachineError, ReplicatedCounterCommand, Session,
};

/// Maximum encoded replicated-command size.
pub const MAX_COMMAND_BYTES: usize = 39;
/// Maximum inline application-snapshot size accepted by the adapter.
pub const MAX_SNAPSHOT_BYTES: usize = 64 * 1024;
const SNAPSHOT_HEADER_BYTES: usize = 1 + 8 + 8 + 4;
const SNAPSHOT_SESSION_BYTES: usize = 4 + 8 + 8 + 1 + 8 + 1 + 8;
const SCHEMA_VERSION: u8 = 1;

pub(super) fn supports_session_bound(max_sessions: usize) -> bool {
    max_sessions != 0
        && max_sessions
            .checked_mul(SNAPSHOT_SESSION_BYTES)
            .and_then(|bytes| SNAPSHOT_HEADER_BYTES.checked_add(bytes))
            .is_some_and(|bytes| bytes <= MAX_SNAPSHOT_BYTES)
}

pub(super) fn encode_command(
    command: &ReplicatedCounterCommand,
) -> Result<Vec<u8>, CounterStateMachineError> {
    let mut bytes = Vec::with_capacity(MAX_COMMAND_BYTES);
    bytes.push(SCHEMA_VERSION);
    match command {
        ReplicatedCounterCommand::OpenSession { client_id, epoch } => {
            bytes.push(1);
            bytes.extend_from_slice(&client_id.get().to_le_bytes());
            bytes.extend_from_slice(&epoch.get().to_le_bytes());
        }
        ReplicatedCounterCommand::Counter { request, command } => {
            bytes.push(2);
            bytes.extend_from_slice(&request.client_id.get().to_le_bytes());
            bytes.extend_from_slice(&request.session_epoch.get().to_le_bytes());
            bytes.extend_from_slice(&request.sequence.get().to_le_bytes());
            bytes.extend_from_slice(&request.fingerprint.get().to_le_bytes());
            encode_counter_command(&mut bytes, *command);
        }
        ReplicatedCounterCommand::Faulty => bytes.push(3),
    }
    if bytes.len() > MAX_COMMAND_BYTES {
        return Err(CounterStateMachineError::CommandTooLarge);
    }
    Ok(bytes)
}

pub(super) fn decode_command(
    payload: &[u8],
) -> Result<ReplicatedCounterCommand, CounterStateMachineError> {
    if payload.len() > MAX_COMMAND_BYTES {
        return Err(CounterStateMachineError::CommandTooLarge);
    }
    let mut decoder = Decoder::command(payload);
    if decoder.u8()? != SCHEMA_VERSION {
        return Err(CounterStateMachineError::UnsupportedVersion);
    }
    let command = match decoder.u8()? {
        1 => ReplicatedCounterCommand::OpenSession {
            client_id: ClientId::new(decoder.u32()?),
            epoch: SessionEpoch::new(decoder.u64()?)
                .ok_or(CounterStateMachineError::MalformedCommand)?,
        },
        2 => ReplicatedCounterCommand::Counter {
            request: RequestIdentity {
                client_id: ClientId::new(decoder.u32()?),
                session_epoch: SessionEpoch::new(decoder.u64()?)
                    .ok_or(CounterStateMachineError::MalformedCommand)?,
                sequence: Sequence::new(decoder.u64()?)
                    .ok_or(CounterStateMachineError::MalformedCommand)?,
                fingerprint: RequestFingerprint::from_digest(decoder.u64()?),
            },
            command: decode_counter_command(&mut decoder)?,
        },
        3 => ReplicatedCounterCommand::Faulty,
        _ => return Err(CounterStateMachineError::MalformedCommand),
    };
    decoder.finish()?;
    Ok(command)
}

pub(super) fn encode_snapshot(
    applied_index: LogIndex,
    value: i64,
    sessions: &BTreeMap<ClientId, Session>,
) -> Result<Vec<u8>, CounterStateMachineError> {
    let size = SNAPSHOT_HEADER_BYTES + sessions.len() * SNAPSHOT_SESSION_BYTES;
    if size > MAX_SNAPSHOT_BYTES {
        return Err(CounterStateMachineError::SnapshotTooLarge);
    }
    let mut bytes = Vec::with_capacity(size);
    bytes.push(SCHEMA_VERSION);
    bytes.extend_from_slice(&applied_index.0.to_le_bytes());
    bytes.extend_from_slice(&value.to_le_bytes());
    let count =
        u32::try_from(sessions.len()).map_err(|_| CounterStateMachineError::SnapshotTooLarge)?;
    bytes.extend_from_slice(&count.to_le_bytes());
    for (client_id, session) in sessions {
        bytes.extend_from_slice(&client_id.get().to_le_bytes());
        bytes.extend_from_slice(&session.epoch.get().to_le_bytes());
        encode_completed(&mut bytes, session.completed);
    }
    Ok(bytes)
}

pub(super) fn decode_snapshot(
    payload: &[u8],
    max_sessions: usize,
) -> Result<(LogIndex, i64, BTreeMap<ClientId, Session>), CounterStateMachineError> {
    if payload.len() > MAX_SNAPSHOT_BYTES {
        return Err(CounterStateMachineError::SnapshotTooLarge);
    }
    let mut decoder = Decoder::snapshot(payload);
    if decoder.u8()? != SCHEMA_VERSION {
        return Err(CounterStateMachineError::UnsupportedVersion);
    }
    let applied_index = LogIndex(decoder.u64()?);
    let value = decoder.i64()?;
    let count =
        usize::try_from(decoder.u32()?).map_err(|_| CounterStateMachineError::MalformedSnapshot)?;
    if count > max_sessions {
        return Err(CounterStateMachineError::SessionCapacity);
    }
    let mut sessions = BTreeMap::new();
    for _ in 0..count {
        let client_id = ClientId::new(decoder.u32()?);
        let epoch =
            SessionEpoch::new(decoder.u64()?).ok_or(CounterStateMachineError::MalformedSnapshot)?;
        let completed = decode_completed(&mut decoder)?;
        if sessions
            .insert(client_id, Session { epoch, completed })
            .is_some()
        {
            return Err(CounterStateMachineError::MalformedSnapshot);
        }
    }
    decoder.finish()?;
    Ok((applied_index, value, sessions))
}

fn encode_counter_command(bytes: &mut Vec<u8>, command: CounterCommand) {
    match command {
        CounterCommand::Add { delta } => {
            bytes.push(1);
            bytes.extend_from_slice(&delta.get().to_le_bytes());
        }
        CounterCommand::Read => bytes.push(2),
    }
}

fn decode_counter_command(
    decoder: &mut Decoder<'_>,
) -> Result<CounterCommand, CounterStateMachineError> {
    match decoder.u8()? {
        1 => crate::Delta::new(decoder.i64()?)
            .map(|delta| CounterCommand::Add { delta })
            .ok_or(CounterStateMachineError::MalformedCommand),
        2 => Ok(CounterCommand::Read),
        _ => Err(CounterStateMachineError::MalformedCommand),
    }
}

fn encode_completed(bytes: &mut Vec<u8>, completed: Option<Completed>) {
    let Some(completed) = completed else {
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&0_i64.to_le_bytes());
        bytes.push(0);
        bytes.extend_from_slice(&0_i64.to_le_bytes());
        return;
    };
    bytes.extend_from_slice(&completed.sequence.get().to_le_bytes());
    match completed.command {
        CounterCommand::Add { delta } => {
            bytes.push(1);
            bytes.extend_from_slice(&delta.get().to_le_bytes());
        }
        CounterCommand::Read => {
            bytes.push(2);
            bytes.extend_from_slice(&0_i64.to_le_bytes());
        }
    }
    match completed.result {
        CounterResult::Added { value } => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        CounterResult::Value { value } => {
            bytes.push(2);
            bytes.extend_from_slice(&value.to_le_bytes());
        }
        CounterResult::Rejected(CounterRejection::CounterOverflow { current }) => {
            bytes.push(3);
            bytes.extend_from_slice(&current.to_le_bytes());
        }
    }
}

fn decode_completed(
    decoder: &mut Decoder<'_>,
) -> Result<Option<Completed>, CounterStateMachineError> {
    let sequence = decoder.u64()?;
    let command_tag = decoder.u8()?;
    let command_value = decoder.i64()?;
    let result_tag = decoder.u8()?;
    let result_value = decoder.i64()?;
    if sequence == 0 {
        if command_tag == 0 && command_value == 0 && result_tag == 0 && result_value == 0 {
            return Ok(None);
        }
        return Err(CounterStateMachineError::MalformedSnapshot);
    }
    let sequence = Sequence::new(sequence).ok_or(CounterStateMachineError::MalformedSnapshot)?;
    let command = match command_tag {
        1 => crate::Delta::new(command_value)
            .map(|delta| CounterCommand::Add { delta })
            .ok_or(CounterStateMachineError::MalformedSnapshot)?,
        2 if command_value == 0 => CounterCommand::Read,
        _ => return Err(CounterStateMachineError::MalformedSnapshot),
    };
    let result = match result_tag {
        1 => CounterResult::Added {
            value: result_value,
        },
        2 => CounterResult::Value {
            value: result_value,
        },
        3 => CounterResult::Rejected(CounterRejection::CounterOverflow {
            current: result_value,
        }),
        _ => return Err(CounterStateMachineError::MalformedSnapshot),
    };
    Ok(Some(Completed {
        sequence,
        command,
        result,
    }))
}

struct Decoder<'a> {
    remaining: &'a [u8],
    malformed: CounterStateMachineError,
}

impl<'a> Decoder<'a> {
    const fn command(bytes: &'a [u8]) -> Self {
        Self {
            remaining: bytes,
            malformed: CounterStateMachineError::MalformedCommand,
        }
    }

    const fn snapshot(bytes: &'a [u8]) -> Self {
        Self {
            remaining: bytes,
            malformed: CounterStateMachineError::MalformedSnapshot,
        }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], CounterStateMachineError> {
        if self.remaining.len() < count {
            return Err(self.malformed);
        }
        let (head, tail) = self.remaining.split_at(count);
        self.remaining = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, CounterStateMachineError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, CounterStateMachineError> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(
            bytes.try_into().map_err(|_| self.malformed)?,
        ))
    }

    fn u64(&mut self) -> Result<u64, CounterStateMachineError> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(
            bytes.try_into().map_err(|_| self.malformed)?,
        ))
    }

    fn i64(&mut self) -> Result<i64, CounterStateMachineError> {
        let bytes = self.take(8)?;
        Ok(i64::from_le_bytes(
            bytes.try_into().map_err(|_| self.malformed)?,
        ))
    }

    fn finish(self) -> Result<(), CounterStateMachineError> {
        if self.remaining.is_empty() {
            Ok(())
        } else {
            Err(self.malformed)
        }
    }
}
