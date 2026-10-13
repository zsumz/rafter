//! Periodic application snapshots and retention through the existing durable owner.
use crate::{counter::Counter, disk, Driver, Result, Runtime};
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId,
};
use rafter_app::state_machine::ReplicatedStateMachine;
use rafter_runtime::service::{ServiceAdapterError, ServiceError, ServiceState};
use rafter_storage::{
    FileRaftSnapshotStore, InMemoryRaftSnapshotStore, PersistedRaftSnapshot, RaftSnapshotStore,
    SnapshotRetention,
};
use std::{
    num::NonZeroU64,
    path::{Path, PathBuf},
};

const MAX_PAYLOAD: u64 = 65_536;

pub struct Snapshots {
    every: NonZeroU64,
    directory: PathBuf,
    pruned: Option<LogIndex>,
}
impl Snapshots {
    pub fn new(directory: &Path) -> Result<Self> {
        let every = match std::env::var("COUNTER_SNAPSHOT_ENTRIES") {
            Ok(value) => value.parse::<NonZeroU64>()?,
            Err(std::env::VarError::NotPresent) => NonZeroU64::new(1024).unwrap(),
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            every,
            directory: directory.join("raft/snapshots"),
            pruned: None,
        })
    }

    pub fn maintain(&mut self, driver: &mut Driver) -> Result<()> {
        if driver.state() != ServiceState::Ready {
            return Ok(());
        }
        let node = driver.ready_node().ok_or("ready owner has no node")?;
        let floor = driver
            .durable_through()
            .ok_or("ready owner has no application floor")?;
        if floor.0.saturating_sub(node.snapshot_index().0) >= self.every.get() {
            match driver.compact_snapshot_with(build) {
                // Counter entries contain a scalar command. Disposal is synchronous,
                // like publication; the threshold is not a disk or latency ceiling.
                Ok(retired) => drop(retired),
                Err(
                    ServiceError::NotQuiescent
                    | ServiceError::NotReady(
                        ServiceState::Busy | ServiceState::Draining | ServiceState::Stopped,
                    ),
                ) => return Ok(()),
                Err(error) => return Err(error.into()),
            }
        }
        let Some(node) = driver.ready_node() else {
            return Ok(());
        };
        let Some(snapshot) = node.snapshot() else {
            return Ok(());
        };
        let at = snapshot.metadata.last_included_index;
        if self.pruned != Some(at) {
            // No runtime persistence job is in flight, and this thread owns the
            // Raft directory. The runtime already materializes bounded snapshot
            // chunks for TLS; queued messages own their bytes across pruning.
            FileRaftSnapshotStore::open(&self.directory)?
                .prune_snapshots(SnapshotRetention::CurrentOnly)?;
            self.pruned = Some(at);
        }
        Ok(())
    }
}

fn build(
    app: &mut Counter,
    node: &Runtime,
) -> std::result::Result<(RaftSnapshot, InMemoryRaftSnapshotStore), ServiceAdapterError> {
    let at = app.applied_index()?;
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("counter")?,
        node.id(),
        at,
        node.term_at_index(at)
            .ok_or_else(|| disk::invalid("snapshot boundary has no term"))?,
        node.current_term(),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("counter-v1")?,
            ApplicationSnapshotVersion::new(1)?,
        ),
    )?;
    let payload = app
        .build_snapshot(at)
        .map_err(|error| disk::invalid(&error.to_string()))?
        .payload;
    if payload.len() > MAX_PAYLOAD as usize {
        return Err(disk::invalid("counter snapshot is too large").into());
    }
    let store = InMemoryRaftSnapshotStore::with_snapshot(PersistedRaftSnapshot {
        metadata,
        application_payload: payload,
    });
    Ok((
        store
            .current_snapshot()
            .ok_or_else(|| disk::invalid("missing built snapshot"))?,
        store,
    ))
}
