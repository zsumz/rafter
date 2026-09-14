//! A prepared local-snapshot transition for durable compositions.

use crate::{CommittedConfiguration, Output, RaftSnapshot};

use super::log::{retained_log_offset, LocalSnapshotInstallError};
use super::Node;

/// An exclusively borrowed local-snapshot transition whose boundary checks
/// have passed but whose kernel mutation has not yet been committed.
///
/// Durable compositions use this two-phase boundary to validate before any
/// storage write, keep the node unavailable for intervening mutation while
/// persistence runs, and commit the infallible kernel transition only after
/// the snapshot and covered-log compaction are durable. Dropping the value
/// changes nothing.
#[must_use = "dropping a prepared local snapshot leaves the node unchanged"]
#[derive(Debug)]
pub struct PreparedLocalSnapshotInstall<'a> {
    node: &'a mut Node,
    snapshot: RaftSnapshot,
    committed_configuration: Option<CommittedConfiguration>,
}

impl PreparedLocalSnapshotInstall<'_> {
    /// Commits the already-validated kernel transition.
    ///
    /// No validation or I/O occurs here. The exclusive borrow held by this
    /// value proves the node could not change after preparation.
    #[must_use]
    pub fn commit(self) -> Vec<Output> {
        self.node
            .install_local_snapshot_state_with_committed_configuration(
                self.snapshot,
                self.committed_configuration,
            )
    }
}

impl Node {
    /// Validates a local snapshot and exclusively reserves its kernel
    /// transition without cloning the node or mutating it.
    ///
    /// This is the durable-composition form of [`Self::install_local_snapshot`].
    /// The returned value holds an exclusive borrow of this node: callers may
    /// persist the snapshot and covered-log compaction through independent
    /// storage handles, then call
    /// [`PreparedLocalSnapshotInstall::commit`]. Dropping it on a persistence
    /// failure leaves the kernel unchanged.
    ///
    /// # Errors
    ///
    /// Returns the same [`LocalSnapshotInstallError`] as
    /// [`Self::install_local_snapshot`], before any kernel state changes.
    pub fn prepare_local_snapshot_install(
        &mut self,
        snapshot: RaftSnapshot,
    ) -> Result<PreparedLocalSnapshotInstall<'_>, LocalSnapshotInstallError> {
        let committed_configuration = self.check_local_snapshot(&snapshot)?;
        Ok(PreparedLocalSnapshotInstall {
            node: self,
            snapshot,
            committed_configuration,
        })
    }

    /// Commits an already-validated local snapshot without cloning its retained
    /// log suffix or rebuilding derived configuration state from every entry.
    pub(in crate::node) fn install_local_snapshot_state_with_committed_configuration(
        &mut self,
        snapshot: RaftSnapshot,
        committed_configuration: Option<CommittedConfiguration>,
    ) -> Vec<Output> {
        let previous_boundary = self.snapshot_index();
        let boundary_index = snapshot.metadata.last_included_index;
        let retired_len = retained_log_offset(
            boundary_index
                .0
                .checked_sub(previous_boundary.0)
                .expect("a prepared local snapshot cannot move backward"),
        );

        drop(self.persistent.log.drain(..retired_len));
        self.derived.configuration.compact_prefix(retired_len);
        self.persistent.snapshot = Some(snapshot);
        self.persistent.committed_configuration = committed_configuration;
        self.reconcile_local_proposals()
    }
}
