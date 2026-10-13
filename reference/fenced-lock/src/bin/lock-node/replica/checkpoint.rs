//! Caller-owned snapshot maintenance through the public decomposition APIs.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    RaftSnapshotMetadata, SnapshotGroupId,
};
use rafter_app::{group::RaftGroup, state_machine::ReplicatedStateMachine};
use rafter_storage::PersistedRaftSnapshot;

use super::{LockRuntime, Replica};

impl Replica {
    /// Snapshots the durable application floor and compacts its matching log prefix.
    ///
    /// The owner loop cannot accept another request during this synchronous
    /// operation. Refusing outstanding waiters avoids turning maintenance into
    /// client cancellation. Re-adoption retains local ID watermarks and the
    /// complete peer-policy checkpoint, including retired identities.
    #[allow(
        dead_code,
        reason = "only the production process exposes checkpoint maintenance"
    )]
    pub fn checkpoint(&mut self) -> Result<LogIndex, String> {
        if !self.driver.pending_writes().is_empty() || !self.driver.pending_reads().is_empty() {
            return Err(String::from(
                "checkpoint requires no outstanding client waiters",
            ));
        }
        let applied = self
            .driver
            .with_group(|group| group.state_machine().applied_index())
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string())?;
        let checkpoint = self.driver.control_plane_checkpoint();
        let group = self
            .driver
            .release_group()
            .map_err(|error| error.to_string())?;
        let mut parts = group.into_parts();
        let result = (|| {
            let metadata = metadata(&parts.runtime, applied)?;
            let snapshot = parts
                .state_machine
                .build_snapshot(applied)
                .map_err(|error| error.to_string())?;
            parts
                .runtime
                .compact_log_with_snapshot(PersistedRaftSnapshot {
                    metadata,
                    application_payload: snapshot.payload,
                })
                .map_err(|error| error.to_string())
        })();
        // Even a refused compaction returns the same owned stores to the driver.
        let adoption = self
            .driver
            .adopt_group_with_checkpoint(
                RaftGroup::from_parts(parts, applied),
                Vec::new(),
                checkpoint,
            )
            .map_err(|error| error.to_string());
        self.finish(adoption)?;
        self.finish(result)?;
        self.refresh_readiness();
        Ok(applied)
    }
}

fn metadata(runtime: &LockRuntime, index: LogIndex) -> Result<RaftSnapshotMetadata, String> {
    let term = runtime
        .term_at_index(index)
        .ok_or_else(|| String::from("snapshot boundary term is absent"))?;
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("fenced-lock-1").map_err(|error| error.to_string())?,
        runtime.id(),
        index,
        term,
        runtime.current_term(),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("fenced-lock").map_err(|error| error.to_string())?,
            ApplicationSnapshotVersion::new(1).map_err(|error| error.to_string())?,
        ),
    )
    .map_err(|error| error.to_string())
}
