//! A prepared local-snapshot transition for durable compositions.

use std::fmt;

use crate::{CommittedConfiguration, LogEntry, Output, RaftSnapshot};

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
    retired_len: usize,
}

/// Log entries retired by a committed local snapshot.
///
/// These entries are no longer part of the Raft state machine. Dropping this
/// value releases their shared application payloads, which an embedding may do
/// away from a latency-sensitive consensus owner after the snapshot commit.
#[must_use = "retired log entries continue to retain their payload allocations"]
#[derive(Default)]
pub struct RetiredLogEntries {
    entries: Vec<LogEntry>,
}

impl RetiredLogEntries {
    /// Returns the number of retired entries still owned by this value.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns whether no entries were retired by the commit.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns a conservative payload-byte count for bounded deferred drop.
    #[must_use]
    pub fn payload_bytes(&self) -> usize {
        self.entries.iter().fold(0, |total, entry| {
            let payload_bytes = match entry.application_payload() {
                Some(payload) => payload.len(),
                None => 0,
            };
            total.saturating_add(payload_bytes)
        })
    }
}

impl fmt::Debug for RetiredLogEntries {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RetiredLogEntries")
            .field("entries", &self.len())
            .field("payload_bytes", &self.payload_bytes())
            .finish()
    }
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
                self.retired_len,
            )
    }

    /// Commits the validated transition and returns ownership of retired entries.
    ///
    /// The returned entries are no longer consensus state. A bounded runtime
    /// worker may drop them away from its consensus-owner thread. Dropping them
    /// inline is equivalent to [`Self::commit`].
    pub fn commit_with_retired_entries(self) -> (Vec<Output>, RetiredLogEntries) {
        self.node
            .install_local_snapshot_state_with_committed_configuration_deferred_drop(
                self.snapshot,
                self.committed_configuration,
                self.retired_len,
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
        let boundary_index = snapshot.metadata.last_included_index;
        let installed_index = self.snapshot_index();
        let Some(retired_delta) = boundary_index.0.checked_sub(installed_index.0) else {
            return Err(LocalSnapshotInstallError::BoundaryBelowInstalledSnapshot {
                snapshot_index: boundary_index,
                installed_index,
            });
        };
        Ok(PreparedLocalSnapshotInstall {
            node: self,
            snapshot,
            committed_configuration,
            retired_len: retained_log_offset(retired_delta),
        })
    }

    /// Commits an already-validated local snapshot without cloning its retained
    /// log suffix or rebuilding derived configuration state from every entry.
    pub(in crate::node) fn install_local_snapshot_state_with_committed_configuration(
        &mut self,
        snapshot: RaftSnapshot,
        committed_configuration: Option<CommittedConfiguration>,
        retired_len: usize,
    ) -> Vec<Output> {
        drop(self.persistent.log.drain(..retired_len));
        self.finish_local_snapshot_install(snapshot, committed_configuration, retired_len)
    }

    fn install_local_snapshot_state_with_committed_configuration_deferred_drop(
        &mut self,
        snapshot: RaftSnapshot,
        committed_configuration: Option<CommittedConfiguration>,
        retired_len: usize,
    ) -> (Vec<Output>, RetiredLogEntries) {
        let retired = if retired_len == 0 {
            RetiredLogEntries::default()
        } else {
            let retained = self.persistent.log.split_off(retired_len);
            let retired = std::mem::replace(&mut self.persistent.log, retained);
            RetiredLogEntries { entries: retired }
        };
        let outputs =
            self.finish_local_snapshot_install(snapshot, committed_configuration, retired_len);
        (outputs, retired)
    }

    fn finish_local_snapshot_install(
        &mut self,
        snapshot: RaftSnapshot,
        committed_configuration: Option<CommittedConfiguration>,
        retired_len: usize,
    ) -> Vec<Output> {
        // Durable snapshot stores publish one authoritative snapshot and clear
        // any staged inbound transfer as part of that publication. Commit the
        // matching volatile transition here, after the durable composition has
        // completed its write, so a later continuation cannot retain an offset
        // for staging bytes that no longer exist.
        self.volatile.incoming_snapshot = None;
        self.derived.configuration.compact_prefix(retired_len);
        self.persistent.snapshot = Some(snapshot);
        self.persistent.committed_configuration = committed_configuration;
        self.reconcile_local_proposals()
    }
}
