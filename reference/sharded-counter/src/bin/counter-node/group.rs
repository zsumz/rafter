//! One durable counter replica and its managed-driver handle.

use std::{
    error::Error,
    fmt, fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeConfig, NodeId, RaftSnapshotMetadata, ReadId, SnapshotGroupId,
};
use rafter_app::{
    error::ErrorCause,
    group::{GroupInput, GroupStepReport, RaftGroup},
    state_machine::ReplicatedStateMachine,
};
use rafter_multiraft::{
    driver::{DriverError, DriverErrorKind},
    typed::TypedGroupDriver,
};
use rafter_reference_sharded_counter::{
    adapter::{CounterAdmissionDecision, CounterApplyResult, ReplicatedCounterCommand},
    GroupId,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
    PersistedRaftSnapshot,
};

use super::app_store::{ApplicationRecord, DurableCounterStateMachine, StoreError};

type Runtime = DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;
type Group = RaftGroup<GroupId, DurableCounterStateMachine, Runtime>;
pub type Report = GroupStepReport<GroupId, CounterApplyResult>;

/// Durable group-open failure.
#[derive(Debug)]
pub enum OpenError {
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    Application(StoreError),
    RaftStore(String),
    Runtime(String),
    Config(String),
    Recovery(String),
    PoisonedRecovery(String),
}

impl fmt::Display for OpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} {}: {source}",
                path.display()
            ),
            Self::Application(error) => write!(formatter, "application record failed: {error}"),
            Self::RaftStore(detail) => write!(formatter, "Raft store failed: {detail}"),
            Self::Runtime(detail) => write!(formatter, "Raft runtime failed: {detail}"),
            Self::Config(detail) => write!(formatter, "Raft configuration failed: {detail}"),
            Self::Recovery(detail) => write!(formatter, "Raft recovery failed: {detail}"),
            Self::PoisonedRecovery(detail) => {
                write!(formatter, "Raft recovery poisoned the group: {detail}")
            }
        }
    }
}

impl Error for OpenError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Application(error) => Some(error),
            _ => None,
        }
    }
}

/// A physical group plus its recovery outputs.
pub struct OpenedGroup {
    pub driver: SharedGroup,
    pub record: ApplicationRecord,
    pub recovery: Report,
}

#[derive(Debug)]
struct GroupSlot {
    group: Option<Group>,
}

/// Cloneable driver stored by the managed host and retained for maintenance.
#[derive(Clone, Debug)]
pub struct SharedGroup {
    inner: Arc<Mutex<GroupSlot>>,
    record: ApplicationRecord,
}

impl SharedGroup {
    pub fn open(
        group_dir: &Path,
        group_id: GroupId,
        node_id: NodeId,
        members: &[NodeId],
        election_timeout_ticks: u64,
        max_sessions: usize,
    ) -> Result<OpenedGroup, OpenError> {
        let raft_dir = group_dir.join("raft");
        let app_dir = group_dir.join("app");
        for directory in [group_dir, raft_dir.as_path(), app_dir.as_path()] {
            fs::create_dir_all(directory).map_err(|source| OpenError::Io {
                operation: "create group directory",
                path: directory.to_path_buf(),
                source,
            })?;
        }
        let stores = FileRaftNodeStores::open(&raft_dir)
            .map_err(|error| OpenError::RaftStore(error.to_string()))?;
        let (hard_state, log, snapshots) = stores.into_parts();
        let (record, state_machine) = ApplicationRecord::open_existing(&app_dir, max_sessions)
            .map_err(OpenError::Application)?;
        let applied = state_machine
            .applied_index()
            .map_err(OpenError::Application)?;
        let peers = members
            .iter()
            .copied()
            .filter(|member| *member != node_id)
            .collect();
        let config = NodeConfig::new(node_id, peers, election_timeout_ticks)
            .map_err(|error| OpenError::Config(format!("{error:?}")))?;
        let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
            config, hard_state, log, snapshots, applied,
        )
        .map_err(|error| OpenError::Runtime(format!("{error:?}")))?;
        let (runtime, outputs) = recovered.into_parts();
        let mut group =
            RaftGroup::with_applied_index(group_id, node_id, runtime, state_machine, applied);
        let recovery = match group.apply_raft_outputs(outputs) {
            Ok(recovery) => recovery,
            Err(error) => {
                if matches!(
                    group.fatal_state(),
                    rafter_app::group::GroupFatalState::Poisoned { .. }
                ) {
                    if application_durability_failed(&group) {
                        return Err(OpenError::Recovery(error.to_string()));
                    }
                    record.mark_poisoned().map_err(OpenError::Application)?;
                    crate::directed_failpoint("after_poison_publication_before_driver_error");
                    return Err(OpenError::PoisonedRecovery(error.to_string()));
                }
                return Err(OpenError::Recovery(error.to_string()));
            }
        };
        Ok(OpenedGroup {
            driver: Self {
                inner: Arc::new(Mutex::new(GroupSlot { group: Some(group) })),
                record: record.clone(),
            },
            record,
            recovery,
        })
    }

    pub fn is_ready(&self) -> bool {
        self.with_group(|group| {
            let applied = group
                .state_machine()
                .applied_index()
                .unwrap_or(LogIndex::ZERO);
            applied >= group.committed_application_index()
                && matches!(
                    group.fatal_state(),
                    rafter_app::group::GroupFatalState::Healthy
                )
        })
    }

    pub fn view(&self) -> rafter_reference_sharded_counter::adapter::CounterStateView {
        self.with_group(|group| group.state_machine().view())
    }

    pub fn admission_decision(
        &self,
        command: ReplicatedCounterCommand,
    ) -> CounterAdmissionDecision {
        self.with_group(|group| group.state_machine().admission_decision(command))
    }

    pub fn step_direct(
        &self,
        input: GroupInput<GroupId, ReplicatedCounterCommand>,
    ) -> Result<Report, DriverError> {
        let mut slot = self.lock();
        let group = slot
            .group
            .as_mut()
            .expect("managed group is present outside maintenance");
        match group.step(input) {
            Ok(report) => Ok(report),
            Err(error) => {
                let kind = match group.fatal_state() {
                    rafter_app::group::GroupFatalState::Poisoned { .. } => {
                        if application_durability_failed(group) {
                            let cause = group
                                .poison_cause()
                                .cloned()
                                .expect("classified durability failure retains its cause");
                            return Err(DriverError::new(DriverErrorKind::Transient, cause));
                        }
                        if !self.record.policy().poisoned {
                            if let Err(poison_error) = self.record.mark_poisoned() {
                                return Err(DriverError::new(
                                    DriverErrorKind::Poisoned,
                                    ErrorCause::new(poison_error),
                                ));
                            }
                            crate::directed_failpoint(
                                "after_poison_publication_before_driver_error",
                            );
                        }
                        DriverErrorKind::Poisoned
                    }
                    rafter_app::group::GroupFatalState::Healthy => DriverErrorKind::Transient,
                };
                Err(DriverError::new(kind, ErrorCause::new(error)))
            }
        }
    }

    pub fn cancel_read(&self, read_id: ReadId) {
        let mut slot = self.lock();
        if let Some(group) = slot.group.as_mut() {
            group.cancel_read(read_id);
        }
    }

    pub fn metrics(&self) -> rafter_app::metrics::RaftGroupMetrics<GroupId> {
        self.with_group(RaftGroup::metrics)
    }

    pub fn compact(&self) -> Result<LogIndex, String> {
        let mut slot = self.lock();
        let group = slot
            .group
            .take()
            .ok_or_else(|| "group is already under maintenance".to_string())?;
        let prior_applied = group.metrics().applied_index;
        let mut parts = group.into_parts();
        let outcome = (|| {
            let boundary = parts
                .state_machine
                .applied_index()
                .map_err(|error| error.to_string())?;
            if boundary == LogIndex::ZERO {
                return Err("group has no applied application entry to compact".to_string());
            }
            let application = parts
                .state_machine
                .build_snapshot(boundary)
                .map_err(|error| error.to_string())?;
            let boundary_term = parts
                .runtime
                .term_at_index(boundary)
                .ok_or_else(|| format!("Raft log does not retain snapshot boundary {boundary}"))?;
            let metadata = RaftSnapshotMetadata::new(
                SnapshotGroupId::new(format!("counter-{}", parts.group_id.get()))
                    .map_err(|error| error.to_string())?,
                parts.node_id,
                boundary,
                boundary_term,
                parts.runtime.current_term(),
                ApplicationSnapshotMetadata::new(
                    ApplicationSnapshotKind::new("sharded_counter")
                        .map_err(|error| error.to_string())?,
                    ApplicationSnapshotVersion::new(1).map_err(|error| error.to_string())?,
                ),
            )
            .map_err(|error| error.to_string())?;
            parts
                .runtime
                .compact_log_with_snapshot(PersistedRaftSnapshot {
                    metadata,
                    application_payload: application.payload,
                })
                .map_err(|error| error.to_string())?;
            Ok(boundary)
        })();
        let rebuilt_applied = outcome.as_ref().copied().unwrap_or(prior_applied);
        slot.group = Some(RaftGroup::from_parts(parts, rebuilt_applied));
        outcome
    }

    fn with_group<T>(&self, operation: impl FnOnce(&Group) -> T) -> T {
        let slot = self.lock();
        operation(
            slot.group
                .as_ref()
                .expect("managed group is present outside maintenance"),
        )
    }

    fn lock(&self) -> MutexGuard<'_, GroupSlot> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn application_durability_failed(group: &Group) -> bool {
    group
        .poison_cause()
        .and_then(|cause| cause.downcast_ref::<StoreError>())
        .is_some_and(StoreError::is_application_durability_failure)
}

impl TypedGroupDriver<GroupId> for SharedGroup {
    type Command = ReplicatedCounterCommand;
    type CommandResult = CounterApplyResult;

    fn step(&mut self, input: GroupInput<GroupId, Self::Command>) -> Result<Report, DriverError> {
        self.step_direct(input)
    }

    fn metrics(&self) -> rafter_app::metrics::RaftGroupMetrics<GroupId> {
        SharedGroup::metrics(self)
    }
}
