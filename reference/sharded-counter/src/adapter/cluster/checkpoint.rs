use std::{collections::BTreeMap, error::Error, fmt};

use rafter_app::state_machine::ApplicationSnapshot;

use crate::{
    ClientId, CounterCommand, GroupId, GroupIncarnation, GroupLifecycle, Sequence, SessionEpoch,
    WorkQuota,
};

use super::{
    super::{
        codec,
        state_machine::{Completed, CounterStateMachine, CounterStateMachineError, Session},
    },
    ManagedCounterCluster, ProposalReceipt,
};

/// One accepted counter request retained in a consumer checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointOutstanding {
    /// Request sequence.
    pub sequence: Sequence,
    /// Exact command bound to the request.
    pub command: CounterCommand,
    /// Original managed/proposal receipt naming the accepted work.
    pub receipt: ProposalReceipt,
}

/// One session retained beside an application snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckpointSession {
    /// Client slot.
    pub client_id: ClientId,
    /// Active session generation.
    pub epoch: SessionEpoch,
    /// Accepted request not yet applied at the snapshot boundary.
    pub outstanding: Option<CheckpointOutstanding>,
    /// Highest applied request retained for exact retry.
    pub completed: Option<crate::adapter::CounterCompletedView>,
}

/// Composite consumer checkpoint for one managed group.
///
/// The application snapshot remains the exact bounded state-machine format.
/// Incarnation, lifecycle, quota, and outstanding admission state are
/// consumer policy and therefore travel beside it rather than inside Rafter.
#[derive(Debug)]
pub struct CounterGroupCheckpoint {
    /// Group slot.
    pub group_id: GroupId,
    /// Exact incarnation at the snapshot boundary.
    pub incarnation: GroupIncarnation,
    /// Consumer lifecycle state.
    pub lifecycle: GroupLifecycle,
    /// Per-turn quota for the incarnation.
    pub quota: WorkQuota,
    /// Exact Rafter application snapshot.
    pub application: ApplicationSnapshot,
    /// Consumer admission state in client order.
    pub sessions: Vec<CheckpointSession>,
}

/// Fully decoded checkpoint ready for caller-owned restart composition.
#[derive(Debug)]
pub struct RestoredCounterGroup {
    /// Group slot.
    pub group_id: GroupId,
    /// Restored incarnation.
    pub incarnation: GroupIncarnation,
    /// Restored consumer lifecycle.
    pub lifecycle: GroupLifecycle,
    /// Restored per-turn quota.
    pub quota: WorkQuota,
    /// State machine installed from the exact application snapshot.
    pub state_machine: CounterStateMachine,
    /// Restored outstanding and completed admission state.
    pub sessions: Vec<CheckpointSession>,
}

/// Why a group checkpoint could not be built or restored.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointError {
    /// The requested group slot has never existed.
    UnknownGroup,
    /// The destination cluster already retains this group slot.
    GroupAlreadyPresent,
    /// Restoring an active group requires its durable Raft runtime, not only a
    /// consumer checkpoint.
    ActiveLifecycle(GroupLifecycle),
    /// An inactive retained slot contradicted the rule that removal clears
    /// client sessions.
    InactiveSessions,
    /// The bounded application snapshot could not be encoded or installed.
    StateMachine(CounterStateMachineError),
    /// The application snapshot vocabulary contains a newer unsupported case.
    SnapshotUnsupported,
}

impl fmt::Display for CheckpointError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{self:?}")
    }
}

impl Error for CheckpointError {}

impl CounterGroupCheckpoint {
    /// Installs the application snapshot and returns every consumer-owned field.
    ///
    /// # Errors
    ///
    /// Returns the exact bounded state-machine snapshot failure.
    ///
    /// # Panics
    ///
    /// Panics when `max_sessions` is zero or exceeds the snapshot format's
    /// representable capacity, matching [`CounterStateMachine::new`].
    pub fn restore(self, max_sessions: usize) -> Result<RestoredCounterGroup, CheckpointError> {
        let state_machine = CounterStateMachine::from_snapshot(max_sessions, self.application)
            .map_err(|error| match error {
                rafter_app::state_machine::ApplicationSnapshotError::Unsupported => {
                    CheckpointError::SnapshotUnsupported
                }
                rafter_app::state_machine::ApplicationSnapshotError::StateMachine(error) => {
                    CheckpointError::StateMachine(error)
                }
                _ => CheckpointError::SnapshotUnsupported,
            })?;
        Ok(RestoredCounterGroup {
            group_id: self.group_id,
            incarnation: self.incarnation,
            lifecycle: self.lifecycle,
            quota: self.quota,
            state_machine,
            sessions: self.sessions,
        })
    }
}

impl ManagedCounterCluster {
    /// Builds one exact composite checkpoint without consuming live state.
    ///
    /// Accepted outstanding requests remain explicit in `sessions`; they are
    /// not invented as applied and are not discarded by the application
    /// snapshot boundary.
    ///
    /// # Errors
    ///
    /// Returns an unknown-group or bounded snapshot-codec failure.
    pub fn checkpoint_group(
        &self,
        group_id: GroupId,
    ) -> Result<CounterGroupCheckpoint, CheckpointError> {
        let slot = self
            .groups
            .get(&group_id)
            .ok_or(CheckpointError::UnknownGroup)?;
        let sessions = slot
            .sessions
            .iter()
            .map(|(client_id, session)| CheckpointSession {
                client_id: *client_id,
                epoch: session.epoch,
                outstanding: session
                    .outstanding
                    .map(|outstanding| CheckpointOutstanding {
                        sequence: outstanding.sequence,
                        command: outstanding.command,
                        receipt: outstanding.receipt,
                    }),
                completed: session.completed.map(|completed| {
                    crate::adapter::CounterCompletedView {
                        sequence: completed.sequence,
                        command: completed.command,
                        result: completed.result,
                    }
                }),
            })
            .collect::<Vec<_>>();
        let replicated = sessions
            .iter()
            .map(|session| {
                let completed = session.completed.map(|completed| Completed {
                    sequence: completed.sequence,
                    command: completed.command,
                    result: completed.result,
                });
                (
                    session.client_id,
                    Session {
                        epoch: session.epoch,
                        completed,
                    },
                )
            })
            .collect::<BTreeMap<_, _>>();
        let payload = codec::encode_snapshot(slot.applied_index, slot.value, &replicated)
            .map_err(CheckpointError::StateMachine)?;
        Ok(CounterGroupCheckpoint {
            group_id,
            incarnation: slot.incarnation,
            lifecycle: slot.lifecycle,
            quota: slot.quota,
            application: ApplicationSnapshot {
                applied_index: slot.applied_index,
                raft_snapshot: None,
                payload,
            },
            sessions,
        })
    }

    /// Restores one inactive retained identity into an otherwise live cluster.
    ///
    /// Removed and tombstoned slots have no physical Raft group, but their
    /// incarnation must survive a local restart so late traffic stays fenced
    /// and a removed slot cannot wrap. Active group recovery additionally
    /// requires its durable Raft runtime and is deliberately refused here.
    ///
    /// # Errors
    ///
    /// Returns a duplicate-slot, active-lifecycle, inactive-session, or
    /// bounded snapshot error.
    ///
    /// # Panics
    ///
    /// Panics when `max_sessions` is zero or exceeds the snapshot format's
    /// representable capacity, matching [`CounterStateMachine::new`].
    pub fn restore_inactive_checkpoint(
        &mut self,
        checkpoint: CounterGroupCheckpoint,
        max_sessions: usize,
    ) -> Result<(), CheckpointError> {
        if self.groups.contains_key(&checkpoint.group_id) {
            return Err(CheckpointError::GroupAlreadyPresent);
        }
        if !matches!(
            checkpoint.lifecycle,
            GroupLifecycle::Removed | GroupLifecycle::Tombstoned
        ) {
            return Err(CheckpointError::ActiveLifecycle(checkpoint.lifecycle));
        }
        let restored = checkpoint.restore(max_sessions)?;
        if !restored.sessions.is_empty() {
            return Err(CheckpointError::InactiveSessions);
        }
        let view = restored.state_machine.view();
        self.groups.insert(
            restored.group_id,
            super::GroupSlot {
                incarnation: restored.incarnation,
                lifecycle: restored.lifecycle,
                quota: restored.quota,
                applied_index: view.applied_index,
                value: view.value,
                sessions: BTreeMap::new(),
            },
        );
        Ok(())
    }
}
