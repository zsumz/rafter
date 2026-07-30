//! Durable counter application and consumer-policy record.
//!
//! Each group owns one checksummed image published by write, file sync,
//! rename, and parent-directory sync. The image keeps the application snapshot,
//! lifecycle/incarnation policy, quota, and accepted request identities
//! together. A restart therefore never reconstructs one of those facts from a
//! different publication generation.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt, fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use rafter::{LogIndex, SnapshotChunkRequest, SnapshotChunkSource};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyResult, ReadBarrier,
    ReplicatedStateMachine, SnapshotSupport,
};
use rafter_crc32::crc32;
use rafter_reference_sharded_counter::{
    adapter::{
        CounterApplyResult, CounterStateMachine, CounterStateMachineError, ReplicatedCounterCommand,
    },
    ClientId, CounterCommand, Delta, GroupIncarnation, GroupLifecycle, RequestFingerprint,
    RequestIdentity, Sequence, SessionEpoch, WorkQuota,
};
use rafter_storage::FileRaftSnapshotStore;

const MAGIC: [u8; 4] = *b"RCAP";
const VERSION: u8 = 2;
const LEGACY_VERSION: u8 = 1;
const MAX_RECORD_BYTES: usize = 128 * 1024;
const SNAPSHOT_READ_CHUNK: u32 = 16 * 1024;

/// One accepted request whose client-visible outcome may still be unknown.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OutstandingRequest {
    pub request: RequestIdentity,
    pub command: CounterCommand,
    pub phase: OutstandingPhase,
}

/// Durable proof of whether an accepted request may have entered the driver.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutstandingPhase {
    Queued,
    EnteredDriver,
}

/// One accepted request whose exact terminal refusal must survive a restart.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TerminalRequest {
    pub request: RequestIdentity,
    pub command: CounterCommand,
    pub failure: TerminalFailure,
}

/// Durable terminal outcomes for accepted work that a group can no longer run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TerminalFailure {
    GroupPoisoned,
    GroupPoisonedUnknown,
    ProcessRestarted,
}

/// Durable policy restored before a group is admitted to the process host.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StoredPolicy {
    pub incarnation: GroupIncarnation,
    pub lifecycle: GroupLifecycle,
    pub poisoned: bool,
    pub quota: WorkQuota,
    pub outstanding: BTreeMap<ClientId, OutstandingRequest>,
    pub terminal: BTreeMap<ClientId, TerminalRequest>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Record {
    policy: StoredPolicy,
    application: ApplicationSnapshot,
}

/// Result of reserving one client request in the durable outstanding table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReserveOutcome {
    Reserved,
    ExactRetry,
}

/// A durable application-record failure.
#[derive(Debug)]
pub enum StoreError {
    Io {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    Corrupt(&'static str),
    Checksum {
        expected: u32,
        actual: u32,
    },
    TooLarge {
        bytes: usize,
    },
    OutstandingCapacity {
        bound: usize,
    },
    ConflictingOutstanding {
        client_id: ClientId,
    },
    OutstandingRemain {
        count: usize,
    },
    Counter(CounterStateMachineError),
    SnapshotUnavailable,
    Missing {
        path: PathBuf,
    },
    AlreadyExists {
        path: PathBuf,
    },
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(formatter, "could not {operation} {}: {source}", path.display()),
            Self::Corrupt(detail) => write!(formatter, "counter application record is corrupt: {detail}"),
            Self::Checksum { expected, actual } => write!(
                formatter,
                "counter application record checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
            Self::TooLarge { bytes } => write!(
                formatter,
                "counter application record is {bytes} bytes, above {MAX_RECORD_BYTES}"
            ),
            Self::OutstandingCapacity { bound } => {
                write!(formatter, "outstanding request capacity {bound} is exhausted")
            }
            Self::ConflictingOutstanding { client_id } => write!(
                formatter,
                "client {} already has a different request outstanding",
                client_id.get()
            ),
            Self::OutstandingRemain { count } => {
                write!(formatter, "{count} durable requests remain outstanding")
            }
            Self::Counter(error) => write!(formatter, "counter state machine failed: {error}"),
            Self::SnapshotUnavailable => {
                formatter.write_str("promoted Raft snapshot payload is unavailable")
            }
            Self::Missing { path } => write!(
                formatter,
                "counter application record is missing at {}; first boot requires host-registry authority",
                path.display()
            ),
            Self::AlreadyExists { path } => {
                write!(formatter, "counter application record already exists at {}", path.display())
            }
        }
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Counter(error) => Some(error),
            _ => None,
        }
    }
}

impl From<CounterStateMachineError> for StoreError {
    fn from(error: CounterStateMachineError) -> Self {
        Self::Counter(error)
    }
}

/// Shared handle used by the process policy and the replicated state machine.
#[derive(Clone, Debug)]
pub struct ApplicationRecord {
    path: PathBuf,
    max_outstanding: usize,
    inner: Arc<Mutex<Record>>,
}

impl ApplicationRecord {
    /// Opens an existing record. Absence is never interpreted as first boot.
    pub fn open_existing(
        app_dir: &Path,
        max_sessions: usize,
    ) -> Result<(Self, DurableCounterStateMachine), StoreError> {
        let path = app_dir.join("state.rcap");
        if !path.exists() {
            return Err(StoreError::Missing { path });
        }
        let record = decode(&fs::read(&path).map_err(|source| io_error("read", &path, source))?)?;
        Self::from_record(app_dir, path, max_sessions, record)
    }

    /// Creates incarnation one only after the host registry has proved this is
    /// a never-before-used slot in the current first-bootstrap operation.
    pub fn bootstrap(
        app_dir: &Path,
        max_sessions: usize,
        quota: WorkQuota,
    ) -> Result<(Self, DurableCounterStateMachine), StoreError> {
        fs::create_dir_all(app_dir).map_err(|source| io_error("create", app_dir, source))?;
        let path = app_dir.join("state.rcap");
        if path.exists() {
            return Err(StoreError::AlreadyExists { path });
        }
        let mut state_machine = CounterStateMachine::new(max_sessions);
        let application = state_machine
            .build_snapshot(LogIndex::ZERO)
            .map_err(|error| snapshot_error(&error))?;
        let record = Record {
            policy: StoredPolicy {
                incarnation: GroupIncarnation::first(),
                lifecycle: GroupLifecycle::Serving,
                poisoned: false,
                quota,
                outstanding: BTreeMap::new(),
                terminal: BTreeMap::new(),
            },
            application,
        };
        persist(&path, &record)?;
        Self::from_record(app_dir, path, max_sessions, record)
    }

    fn from_record(
        app_dir: &Path,
        path: PathBuf,
        max_sessions: usize,
        record: Record,
    ) -> Result<(Self, DurableCounterStateMachine), StoreError> {
        if record.policy.outstanding.len() + record.policy.terminal.len() > max_sessions {
            return Err(StoreError::OutstandingCapacity {
                bound: max_sessions,
            });
        }
        let state_machine =
            CounterStateMachine::from_snapshot(max_sessions, record.application.clone())
                .map_err(|error| snapshot_error(&error))?;
        let shared = Self {
            path,
            max_outstanding: max_sessions,
            inner: Arc::new(Mutex::new(record)),
        };
        Ok((
            shared.clone(),
            DurableCounterStateMachine {
                inner: state_machine,
                record: shared,
                snapshot_dir: app_dir
                    .parent()
                    .expect("app directory has a group parent")
                    .join("raft/snapshots"),
            },
        ))
    }

    pub fn policy(&self) -> StoredPolicy {
        self.lock().policy.clone()
    }

    pub fn reserve(
        &self,
        request: RequestIdentity,
        command: CounterCommand,
    ) -> Result<ReserveOutcome, StoreError> {
        let mut staged = self.lock().clone();
        if let Some(existing) = staged.policy.outstanding.get(&request.client_id) {
            return if existing.request == request && existing.command == command {
                Ok(ReserveOutcome::ExactRetry)
            } else {
                Err(StoreError::ConflictingOutstanding {
                    client_id: request.client_id,
                })
            };
        }
        if staged.policy.terminal.contains_key(&request.client_id) {
            return Err(StoreError::ConflictingOutstanding {
                client_id: request.client_id,
            });
        }
        if staged.policy.outstanding.len() + staged.policy.terminal.len() >= self.max_outstanding {
            return Err(StoreError::OutstandingCapacity {
                bound: self.max_outstanding,
            });
        }
        staged.policy.outstanding.insert(
            request.client_id,
            OutstandingRequest {
                request,
                command,
                phase: OutstandingPhase::Queued,
            },
        );
        self.publish(staged)?;
        Ok(ReserveOutcome::Reserved)
    }

    pub fn mark_entered_driver(
        &self,
        request: RequestIdentity,
        command: CounterCommand,
    ) -> Result<(), StoreError> {
        let mut staged = self.lock().clone();
        let Some(outstanding) = staged.policy.outstanding.get_mut(&request.client_id) else {
            return Ok(());
        };
        if outstanding.request != request || outstanding.command != command {
            return Err(StoreError::ConflictingOutstanding {
                client_id: request.client_id,
            });
        }
        if outstanding.phase == OutstandingPhase::Queued {
            outstanding.phase = OutstandingPhase::EnteredDriver;
            self.publish(staged)?;
        }
        Ok(())
    }

    pub fn begin_draining(&self, poisoned: bool) -> Result<(), StoreError> {
        let mut staged = self.lock().clone();
        staged.policy.lifecycle = GroupLifecycle::Draining;
        staged.policy.poisoned |= poisoned;
        self.publish(staged)
    }

    pub fn mark_poisoned(&self) -> Result<(), StoreError> {
        let mut staged = self.lock().clone();
        if !staged.policy.poisoned {
            staged.policy.poisoned = true;
            self.publish(staged)?;
        }
        Ok(())
    }

    pub fn replay_terminal_failure(
        &self,
        request: RequestIdentity,
        command: CounterCommand,
    ) -> Result<Option<TerminalFailure>, StoreError> {
        let staged = self.lock();
        let Some(terminal) = staged.policy.terminal.get(&request.client_id).copied() else {
            return Ok(None);
        };
        if terminal.request == request && terminal.command == command {
            Ok(Some(terminal.failure))
        } else {
            Err(StoreError::ConflictingOutstanding {
                client_id: request.client_id,
            })
        }
    }

    pub fn fail_reservation(
        &self,
        request: RequestIdentity,
        command: CounterCommand,
        failure: TerminalFailure,
    ) -> Result<(), StoreError> {
        let mut staged = self.lock().clone();
        if let Some(terminal) = staged.policy.terminal.get(&request.client_id) {
            return if terminal.request == request
                && terminal.command == command
                && terminal.failure == failure
            {
                Ok(())
            } else {
                Err(StoreError::ConflictingOutstanding {
                    client_id: request.client_id,
                })
            };
        }
        let Some(outstanding) = staged.policy.outstanding.get(&request.client_id) else {
            return Ok(());
        };
        if outstanding.request != request || outstanding.command != command {
            return Err(StoreError::ConflictingOutstanding {
                client_id: request.client_id,
            });
        }
        staged.policy.outstanding.remove(&request.client_id);
        staged.policy.terminal.insert(
            request.client_id,
            TerminalRequest {
                request,
                command,
                failure,
            },
        );
        self.publish(staged)
    }

    pub fn fail_poisoned_outstanding(&self) -> Result<(), StoreError> {
        let mut staged = self.lock().clone();
        if staged.policy.outstanding.is_empty() {
            return Ok(());
        }
        for (client_id, outstanding) in std::mem::take(&mut staged.policy.outstanding) {
            let failure = match outstanding.phase {
                OutstandingPhase::Queued => TerminalFailure::GroupPoisoned,
                OutstandingPhase::EnteredDriver => TerminalFailure::GroupPoisonedUnknown,
            };
            staged.policy.terminal.insert(
                client_id,
                TerminalRequest {
                    request: outstanding.request,
                    command: outstanding.command,
                    failure,
                },
            );
        }
        self.publish(staged)
    }

    pub fn cancel_reservation(
        &self,
        request: RequestIdentity,
        command: CounterCommand,
    ) -> Result<(), StoreError> {
        let mut staged = self.lock().clone();
        if staged
            .policy
            .outstanding
            .get(&request.client_id)
            .is_some_and(|outstanding| {
                outstanding.request == request && outstanding.command == command
            })
        {
            staged.policy.outstanding.remove(&request.client_id);
            self.publish(staged)?;
        }
        Ok(())
    }

    pub fn retire(&self, lifecycle: GroupLifecycle) -> Result<(), StoreError> {
        debug_assert!(matches!(
            lifecycle,
            GroupLifecycle::Removed | GroupLifecycle::Tombstoned
        ));
        let mut staged = self.lock().clone();
        if !staged.policy.outstanding.is_empty() {
            return Err(StoreError::OutstandingRemain {
                count: staged.policy.outstanding.len(),
            });
        }
        let mut state_machine = CounterStateMachine::new(self.max_outstanding);
        staged.application = state_machine
            .build_snapshot(LogIndex::ZERO)
            .map_err(|error| snapshot_error(&error))?;
        staged.policy.lifecycle = lifecycle;
        staged.policy.poisoned = false;
        self.publish(staged)
    }

    pub fn reopen(&self, quota: WorkQuota, max_sessions: usize) -> Result<(), StoreError> {
        let mut staged = self.lock().clone();
        let incarnation = staged
            .policy
            .incarnation
            .successor()
            .ok_or(StoreError::Corrupt("group incarnation exhausted"))?;
        let mut state_machine = CounterStateMachine::new(max_sessions);
        staged.application = state_machine
            .build_snapshot(LogIndex::ZERO)
            .map_err(|error| snapshot_error(&error))?;
        staged.policy = StoredPolicy {
            incarnation,
            lifecycle: GroupLifecycle::Serving,
            poisoned: false,
            quota,
            outstanding: BTreeMap::new(),
            terminal: BTreeMap::new(),
        };
        self.publish(staged)
    }

    fn commit_application(
        &self,
        application: ApplicationSnapshot,
        commands: &[ReplicatedCounterCommand],
    ) -> Result<(), StoreError> {
        let mut staged = self.lock().clone();
        staged.application = application;
        for command in commands {
            if let ReplicatedCounterCommand::Counter { request, command } = command {
                if staged
                    .policy
                    .outstanding
                    .get(&request.client_id)
                    .is_some_and(|outstanding| {
                        outstanding.request == *request && outstanding.command == *command
                    })
                {
                    staged.policy.outstanding.remove(&request.client_id);
                }
            }
        }
        self.publish(staged)
    }

    fn publish(&self, staged: Record) -> Result<(), StoreError> {
        persist(&self.path, &staged)?;
        *self.lock() = staged;
        Ok(())
    }

    fn lock(&self) -> MutexGuard<'_, Record> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// Counter state machine whose successful mutations are durable before return.
#[derive(Debug)]
pub struct DurableCounterStateMachine {
    inner: CounterStateMachine,
    record: ApplicationRecord,
    snapshot_dir: PathBuf,
}

impl DurableCounterStateMachine {
    pub fn view(&self) -> rafter_reference_sharded_counter::adapter::CounterStateView {
        self.inner.view()
    }

    fn register_promoted_payload(
        &mut self,
        snapshot: &ApplicationSnapshot,
    ) -> Result<(), StoreError> {
        let Some(descriptor) = snapshot.raft_snapshot.as_ref() else {
            return Ok(());
        };
        if !snapshot.payload.is_empty() {
            return Ok(());
        }
        let source = FileRaftSnapshotStore::open(&self.snapshot_dir)
            .map_err(|_| StoreError::SnapshotUnavailable)?;
        let mut payload = Vec::with_capacity(
            usize::try_from(descriptor.application_payload_len)
                .map_err(|_| StoreError::SnapshotUnavailable)?,
        );
        let mut offset = 0_u64;
        while offset < descriptor.application_payload_len {
            let remaining = descriptor.application_payload_len - offset;
            let len = u32::try_from(remaining.min(u64::from(SNAPSHOT_READ_CHUNK)))
                .map_err(|_| StoreError::SnapshotUnavailable)?;
            let chunk = source
                .snapshot_chunk(SnapshotChunkRequest {
                    transfer_id: descriptor.transfer_id(),
                    metadata: &descriptor.metadata,
                    total_payload_len: descriptor.application_payload_len,
                    application_payload_crc32: descriptor.application_payload_crc32,
                    offset,
                    len,
                })
                .ok_or(StoreError::SnapshotUnavailable)?;
            payload.extend_from_slice(&chunk);
            offset += u64::from(len);
        }
        self.inner.register_promoted_snapshot(descriptor, payload)?;
        Ok(())
    }
}

impl ReplicatedStateMachine for DurableCounterStateMachine {
    type Command = ReplicatedCounterCommand;
    type CommandResult = CounterApplyResult;
    type Query = ();
    type QueryResult = i64;
    type Error = StoreError;

    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        self.inner.applied_index().map_err(Into::into)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        self.inner.encode_command(command).map_err(Into::into)
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        self.inner.decode_command(payload).map_err(Into::into)
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        let commands = batch
            .entries
            .iter()
            .map(|entry| entry.command)
            .collect::<Vec<_>>();
        let mut staged = self.inner.clone();
        let results = staged.apply_batch(batch)?;
        let at = staged.applied_index()?;
        let application = staged
            .build_snapshot(at)
            .map_err(|error| snapshot_error(&error))?;
        self.record.commit_application(application, &commands)?;
        self.inner = staged;
        Ok(results)
    }

    fn read(&self, query: (), barrier: ReadBarrier) -> Result<i64, Self::Error> {
        self.inner.read(query, barrier).map_err(Into::into)
    }

    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<Self::Error>> {
        self.inner
            .build_snapshot(at)
            .map_err(|error| map_snapshot_error(&error))
    }

    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
        let mut staged = self.inner.clone();
        if snapshot.payload.is_empty() {
            self.register_promoted_payload(&snapshot)
                .map_err(ApplicationSnapshotError::StateMachine)?;
            staged = self.inner.clone();
        }
        staged
            .install_snapshot(snapshot)
            .map_err(|error| map_snapshot_error(&error))?;
        let at = staged
            .applied_index()
            .map_err(StoreError::from)
            .map_err(ApplicationSnapshotError::StateMachine)?;
        let application = staged
            .build_snapshot(at)
            .map_err(|error| map_snapshot_error(&error))?;
        self.record
            .commit_application(application, &[])
            .map_err(ApplicationSnapshotError::StateMachine)?;
        self.inner = staged;
        Ok(())
    }
}

fn persist(path: &Path, record: &Record) -> Result<(), StoreError> {
    let bytes = encode(record)?;
    let temp = path.with_extension(format!("tmp.{}", std::process::id()));
    let mut file = fs::File::create(&temp).map_err(|source| io_error("create", &temp, source))?;
    file.write_all(&bytes)
        .map_err(|source| io_error("write", &temp, source))?;
    file.sync_all()
        .map_err(|source| io_error("sync", &temp, source))?;
    fs::rename(&temp, path).map_err(|source| io_error("publish", path, source))?;
    let parent = path
        .parent()
        .ok_or(StoreError::Corrupt("record path has no parent"))?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error("sync parent directory", parent, source))
}

fn encode(record: &Record) -> Result<Vec<u8>, StoreError> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&MAGIC);
    bytes.push(VERSION);
    put_u32(&mut bytes, record.policy.incarnation.get());
    bytes.push(encode_lifecycle(record.policy.lifecycle));
    bytes.push(u8::from(record.policy.poisoned));
    put_u32(&mut bytes, record.policy.quota.get());
    put_u32(
        &mut bytes,
        u32::try_from(record.policy.outstanding.len())
            .map_err(|_| StoreError::TooLarge { bytes: usize::MAX })?,
    );
    for outstanding in record.policy.outstanding.values() {
        put_request(&mut bytes, outstanding.request);
        put_command(&mut bytes, outstanding.command);
        bytes.push(encode_outstanding_phase(outstanding.phase));
    }
    put_u32(
        &mut bytes,
        u32::try_from(record.policy.terminal.len())
            .map_err(|_| StoreError::TooLarge { bytes: usize::MAX })?,
    );
    for terminal in record.policy.terminal.values() {
        put_request(&mut bytes, terminal.request);
        put_command(&mut bytes, terminal.command);
        bytes.push(encode_terminal_failure(terminal.failure));
    }
    put_u64(&mut bytes, record.application.applied_index.0);
    put_u32(
        &mut bytes,
        u32::try_from(record.application.payload.len())
            .map_err(|_| StoreError::TooLarge { bytes: usize::MAX })?,
    );
    bytes.extend_from_slice(&record.application.payload);
    let checksum = crc32(&bytes);
    put_u32(&mut bytes, checksum);
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(StoreError::TooLarge { bytes: bytes.len() });
    }
    Ok(bytes)
}

fn decode(bytes: &[u8]) -> Result<Record, StoreError> {
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(StoreError::TooLarge { bytes: bytes.len() });
    }
    if bytes.len() < 4 + 1 + 4 {
        return Err(StoreError::Corrupt("truncated envelope"));
    }
    let body_len = bytes.len() - 4;
    let expected = u32::from_le_bytes(
        bytes[body_len..]
            .try_into()
            .map_err(|_| StoreError::Corrupt("truncated checksum"))?,
    );
    let actual = crc32(&bytes[..body_len]);
    if expected != actual {
        return Err(StoreError::Checksum { expected, actual });
    }
    let mut cursor = Cursor::new(&bytes[..body_len]);
    if cursor.take(4)? != MAGIC {
        return Err(StoreError::Corrupt("wrong magic"));
    }
    let version = cursor.u8()?;
    if !(LEGACY_VERSION..=VERSION).contains(&version) {
        return Err(StoreError::Corrupt("unsupported version"));
    }
    let incarnation =
        GroupIncarnation::new(cursor.u32()?).ok_or(StoreError::Corrupt("zero incarnation"))?;
    let lifecycle = decode_lifecycle(cursor.u8()?)?;
    let poisoned = version >= VERSION && decode_bool(cursor.u8()?)?;
    let quota = WorkQuota::new(cursor.u32()?).ok_or(StoreError::Corrupt("zero quota"))?;
    let outstanding_len = cursor.u32()? as usize;
    let mut outstanding = BTreeMap::new();
    for _ in 0..outstanding_len {
        let request = cursor.request()?;
        let command = cursor.command()?;
        let phase = if version >= VERSION {
            decode_outstanding_phase(cursor.u8()?)?
        } else {
            OutstandingPhase::EnteredDriver
        };
        if outstanding
            .insert(
                request.client_id,
                OutstandingRequest {
                    request,
                    command,
                    phase,
                },
            )
            .is_some()
        {
            return Err(StoreError::Corrupt("duplicate outstanding client"));
        }
    }
    let mut terminal = BTreeMap::new();
    if version >= VERSION {
        let terminal_len = cursor.u32()? as usize;
        for _ in 0..terminal_len {
            let request = cursor.request()?;
            let command = cursor.command()?;
            let failure = decode_terminal_failure(cursor.u8()?)?;
            if outstanding.contains_key(&request.client_id)
                || terminal
                    .insert(
                        request.client_id,
                        TerminalRequest {
                            request,
                            command,
                            failure,
                        },
                    )
                    .is_some()
            {
                return Err(StoreError::Corrupt("duplicate durable client outcome"));
            }
        }
    }
    let applied_index = LogIndex(cursor.u64()?);
    let payload_len = cursor.u32()? as usize;
    let payload = cursor.take(payload_len)?.to_vec();
    if !cursor.remaining().is_empty() {
        return Err(StoreError::Corrupt("trailing bytes"));
    }
    Ok(Record {
        policy: StoredPolicy {
            incarnation,
            lifecycle,
            poisoned,
            quota,
            outstanding,
            terminal,
        },
        application: ApplicationSnapshot {
            applied_index,
            payload,
            raft_snapshot: None,
        },
    })
}

fn put_request(bytes: &mut Vec<u8>, request: RequestIdentity) {
    put_u32(bytes, request.client_id.get());
    put_u64(bytes, request.session_epoch.get());
    put_u64(bytes, request.sequence.get());
    put_u64(bytes, request.fingerprint.get());
}

fn put_command(bytes: &mut Vec<u8>, command: CounterCommand) {
    match command {
        CounterCommand::Add { delta } => {
            bytes.push(1);
            bytes.extend_from_slice(&delta.get().to_le_bytes());
        }
        CounterCommand::Read => bytes.push(2),
    }
}

const fn encode_terminal_failure(failure: TerminalFailure) -> u8 {
    match failure {
        TerminalFailure::GroupPoisoned => 1,
        TerminalFailure::ProcessRestarted => 2,
        TerminalFailure::GroupPoisonedUnknown => 3,
    }
}

fn decode_terminal_failure(value: u8) -> Result<TerminalFailure, StoreError> {
    match value {
        1 => Ok(TerminalFailure::GroupPoisoned),
        2 => Ok(TerminalFailure::ProcessRestarted),
        3 => Ok(TerminalFailure::GroupPoisonedUnknown),
        _ => Err(StoreError::Corrupt("unknown terminal failure")),
    }
}

fn decode_bool(value: u8) -> Result<bool, StoreError> {
    match value {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(StoreError::Corrupt("invalid boolean")),
    }
}

const fn encode_outstanding_phase(phase: OutstandingPhase) -> u8 {
    match phase {
        OutstandingPhase::Queued => 1,
        OutstandingPhase::EnteredDriver => 2,
    }
}

fn decode_outstanding_phase(value: u8) -> Result<OutstandingPhase, StoreError> {
    match value {
        1 => Ok(OutstandingPhase::Queued),
        2 => Ok(OutstandingPhase::EnteredDriver),
        _ => Err(StoreError::Corrupt("unknown outstanding phase")),
    }
}

fn put_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_le_bytes());
}

fn encode_lifecycle(lifecycle: GroupLifecycle) -> u8 {
    match lifecycle {
        GroupLifecycle::Creating => 1,
        GroupLifecycle::Recovering => 2,
        GroupLifecycle::Serving => 3,
        GroupLifecycle::Draining => 4,
        GroupLifecycle::Removed => 5,
        GroupLifecycle::Tombstoned => 6,
    }
}

fn decode_lifecycle(value: u8) -> Result<GroupLifecycle, StoreError> {
    match value {
        1 => Ok(GroupLifecycle::Creating),
        2 => Ok(GroupLifecycle::Recovering),
        3 => Ok(GroupLifecycle::Serving),
        4 => Ok(GroupLifecycle::Draining),
        5 => Ok(GroupLifecycle::Removed),
        6 => Ok(GroupLifecycle::Tombstoned),
        _ => Err(StoreError::Corrupt("unknown lifecycle")),
    }
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], StoreError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(StoreError::Corrupt("length overflow"))?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(StoreError::Corrupt("truncated field"))?;
        self.offset = end;
        Ok(value)
    }

    fn remaining(&self) -> &'a [u8] {
        &self.bytes[self.offset..]
    }

    fn u8(&mut self) -> Result<u8, StoreError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, StoreError> {
        Ok(u32::from_le_bytes(
            self.take(4)?
                .try_into()
                .map_err(|_| StoreError::Corrupt("invalid u32"))?,
        ))
    }

    fn u64(&mut self) -> Result<u64, StoreError> {
        Ok(u64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| StoreError::Corrupt("invalid u64"))?,
        ))
    }

    fn i64(&mut self) -> Result<i64, StoreError> {
        Ok(i64::from_le_bytes(
            self.take(8)?
                .try_into()
                .map_err(|_| StoreError::Corrupt("invalid i64"))?,
        ))
    }

    fn request(&mut self) -> Result<RequestIdentity, StoreError> {
        let client_id = ClientId::new(self.u32()?);
        let session_epoch =
            SessionEpoch::new(self.u64()?).ok_or(StoreError::Corrupt("zero session epoch"))?;
        let sequence = Sequence::new(self.u64()?).ok_or(StoreError::Corrupt("zero sequence"))?;
        let fingerprint = RequestFingerprint::from_digest(self.u64()?);
        Ok(RequestIdentity {
            client_id,
            session_epoch,
            sequence,
            fingerprint,
        })
    }

    fn command(&mut self) -> Result<CounterCommand, StoreError> {
        match self.u8()? {
            1 => {
                let delta =
                    Delta::new(self.i64()?).ok_or(StoreError::Corrupt("zero counter delta"))?;
                Ok(CounterCommand::Add { delta })
            }
            2 => Ok(CounterCommand::Read),
            _ => Err(StoreError::Corrupt("unknown counter command")),
        }
    }
}

fn snapshot_error(error: &ApplicationSnapshotError<CounterStateMachineError>) -> StoreError {
    match error {
        ApplicationSnapshotError::StateMachine(error) => StoreError::Counter(*error),
        ApplicationSnapshotError::Unsupported => {
            StoreError::Corrupt("counter snapshot unexpectedly unsupported")
        }
        _ => StoreError::Corrupt("unknown counter snapshot failure"),
    }
}

fn map_snapshot_error(
    error: &ApplicationSnapshotError<CounterStateMachineError>,
) -> ApplicationSnapshotError<StoreError> {
    match error {
        ApplicationSnapshotError::StateMachine(error) => {
            ApplicationSnapshotError::StateMachine(StoreError::Counter(*error))
        }
        ApplicationSnapshotError::Unsupported => ApplicationSnapshotError::Unsupported,
        _ => ApplicationSnapshotError::StateMachine(StoreError::Corrupt(
            "unknown counter snapshot failure",
        )),
    }
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> StoreError {
    StoreError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

#[cfg(test)]
mod tests {
    use rafter_reference_harness::process::ScratchSpace;

    use super::*;

    fn request(command: CounterCommand) -> RequestIdentity {
        RequestIdentity {
            client_id: ClientId::new(7),
            session_epoch: SessionEpoch::new(3).expect("test epoch is nonzero"),
            sequence: Sequence::new(11).expect("test sequence is nonzero"),
            fingerprint: RequestFingerprint::of(&command),
        }
    }

    #[test]
    fn terminal_poison_failure_survives_retirement_and_clears_on_reopen() {
        let scratch = ScratchSpace::create("counter-app-store", "terminal-poison")
            .expect("scratch directory is created");
        let app_dir = scratch.path().join("app");
        let quota = WorkQuota::new(4).expect("test quota is nonzero");
        let (record, state_machine) =
            ApplicationRecord::bootstrap(&app_dir, 8, quota).expect("record bootstraps");
        drop(state_machine);
        let command = CounterCommand::Add {
            delta: Delta::new(5).expect("test delta is nonzero"),
        };
        let request = request(command);

        assert_eq!(
            record.reserve(request, command).expect("request reserves"),
            ReserveOutcome::Reserved
        );
        record.begin_draining(true).expect("draining publishes");
        assert!(matches!(
            record.retire(GroupLifecycle::Removed),
            Err(StoreError::OutstandingRemain { count: 1 })
        ));

        record
            .fail_poisoned_outstanding()
            .expect("terminal refusal publishes");
        record
            .retire(GroupLifecycle::Removed)
            .expect("resolved record retires");
        drop(record);

        let (record, state_machine) =
            ApplicationRecord::open_existing(&app_dir, 8).expect("retired record reopens");
        drop(state_machine);
        assert!(!record.policy().poisoned);
        assert_eq!(
            record
                .replay_terminal_failure(request, command)
                .expect("terminal lookup succeeds"),
            Some(TerminalFailure::GroupPoisoned)
        );
        record.reopen(quota, 8).expect("new incarnation opens");
        assert_eq!(
            record
                .replay_terminal_failure(request, command)
                .expect("terminal lookup succeeds"),
            None
        );
    }
}
