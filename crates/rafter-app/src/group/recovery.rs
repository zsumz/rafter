//! Restart recovery: install the snapshot, then apply the retained suffix.
//!
//! The two halves of a restart arrive from opposite sides of the composition.
//! The runtime knows a snapshot boundary and the committed entries above it;
//! the state machine knows how far its own durable state reached. Neither can
//! see the other, and the kernel says so — it raises a declared floor to its
//! boundary because it retains no covered entry and holds a descriptor rather
//! than payload bytes. This module is where the group, which holds both halves,
//! pays the obligation that raise creates.

use super::{
    ApplyEntry, Debug, GroupError, GroupResult, LogIndex, PersistedRaftRuntime, RaftGroup,
    RaftOutput, RaftSnapshot, ReplicatedStateMachine, StepReportOptions, StepReportResult,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    /// Restores the state machine to the runtime's snapshot boundary when it
    /// opened below it, then applies the recovery outputs — in that order, in
    /// one operation.
    ///
    /// **The only supported way to drain a restart's committed suffix**, and
    /// the reason is that the two steps are one transaction rather than two
    /// calls a caller is trusted to order. A replica that promoted an inbound
    /// snapshot and crashed before installing it reopens with a state machine
    /// short of a boundary its Raft state already carries. The kernel raises
    /// the declared floor to that boundary, so the suffix it hands back starts
    /// *above* the entries the state machine is missing — and applying that
    /// suffix directly writes a prefix, a hole, and a suffix into durable
    /// application state. Nothing downstream can find the hole: the applied
    /// index, the readiness predicate, and every metrics snapshot compare
    /// numbers, and all three numbers are right.
    ///
    /// So the install goes first, and it goes through the same path a
    /// leader-driven install takes — [`crate::snapshot::SnapshotEvent::Apply`]
    /// lands in the returned report, the state machine's snapshot-support
    /// declaration is checked, and a failed install poisons exactly as it does
    /// mid-flight. The suffix is applied only after the state machine reports
    /// the boundary. It earns its way past
    /// [`GroupError::SnapshotRestoreRequired`] rather than being exempted from
    /// it: the guard on the apply path is unconditional, and by the time the
    /// suffix reaches it the state machine is no longer behind.
    ///
    /// # What it does not do
    ///
    /// It restores exactly what it is about to endanger, and nothing else. A
    /// recovery that carries no committed application entry — the ordinary
    /// shape for a fully compacted replica, whose boundary *is* its last
    /// commit — installs nothing, because there is no suffix to lay over the
    /// gap. Such a replica is still below its boundary and still broken, and
    /// the verdict for it is unchanged and permanent:
    /// [`GroupError::AppliedIndexBelowSnapshotBoundary`] on the first step or
    /// read, which is the first moment its state machine would answer for the
    /// replica. Repairing it silently here would instead take a decision about
    /// discarded application state at a moment no operator is watching.
    ///
    /// It also installs nothing when `outputs` already carries an
    /// [`RaftOutput::ApplySnapshot`] at or above the boundary. That vector
    /// already orders the install ahead of its own applies.
    ///
    /// # Errors
    ///
    /// Returns [`GroupError::SnapshotRestoreRequired`] when a restore is owed
    /// and the runtime cannot produce the descriptor to perform it, with the
    /// application untouched. Otherwise as
    /// [`RaftGroup::apply_raft_outputs`], plus the install failures
    /// [`GroupError::SnapshotsUnsupported`],
    /// [`GroupError::SnapshotSupportMisdeclared`], and a poisoning
    /// [`GroupError::StateMachine`] — each of which ends the operation before
    /// a single entry of the suffix is applied.
    pub fn apply_recovery_outputs(
        &mut self,
        outputs: Vec<RaftOutput>,
    ) -> StepReportResult<G, A, R> {
        self.reject_if_poisoned()?;
        let outputs = match self.snapshot_restore_owed_by(&outputs)? {
            None => outputs,
            Some(snapshot) => install_ahead_of(snapshot, outputs),
        };
        self.apply_outputs(outputs, false, StepReportOptions::default())
    }

    /// The snapshot this recovery must install before it applies `outputs`, if
    /// any.
    ///
    /// A restore that is owed but cannot be performed is refused here rather
    /// than left to the guard on the apply path, which would reach the same
    /// verdict. The difference is what happens on the way: the pump walks the
    /// whole output vector before it applies anything, queueing committed
    /// configurations, recording granted read indexes, and collecting peer
    /// messages into a report the refusal then discards. Refusing up front
    /// leaves the group's own bookkeeping exactly as it was, which is what lets
    /// a caller repair the snapshot store and hand the same vector back.
    fn snapshot_restore_owed_by(
        &mut self,
        outputs: &[RaftOutput],
    ) -> GroupResult<A, R, Option<RaftSnapshot>> {
        let Some(entry_index) = lowest_application_entry(outputs) else {
            return Ok(None);
        };
        let snapshot_index = self.raft.snapshot_index();
        let app_applied_index = self.app_applied_index()?;
        if app_applied_index >= snapshot_index || installs_through(outputs, snapshot_index) {
            return Ok(None);
        }
        let Some(snapshot) = self.raft.snapshot() else {
            return Err(GroupError::SnapshotRestoreRequired {
                app_applied_index,
                snapshot_index,
                entry_index,
            });
        };
        Ok(Some(snapshot))
    }

    /// Refuses a committed application entry aimed at a state machine that has
    /// not installed the snapshot underneath it.
    ///
    /// **The structural half of the fix, and it is unconditional on purpose.**
    /// This runs on every path that applies committed entries — the raw output
    /// pump, an ordinary step, and the recovery operation alike — so there is
    /// no argument, flag, or call order that reaches `apply_batch` while the
    /// gap is open. [`RaftGroup::apply_recovery_outputs`] does not skip it; it
    /// closes the gap first and then passes.
    ///
    /// It is checked after the group's own floor comparison and before the
    /// already-applied comparison, which is a statement about what each of the
    /// three means. The two around it are verdicts on a state machine that
    /// contradicts itself, and they poison. This one is a repair that has not
    /// happened yet, so it refuses without poisoning — and it is unreachable
    /// together with either of them in a real restart, where the group's floor
    /// is read from the state machine and the suffix starts above the boundary.
    ///
    /// Nothing here reads or writes the application beyond the applied index
    /// the caller already read, so a refusal leaves durable application state
    /// exactly as the process found it.
    pub(super) fn reject_if_snapshot_restore_required(
        &mut self,
        app_applied_index: LogIndex,
        entries: &[ApplyEntry<A::Command>],
    ) -> GroupResult<A, R, ()> {
        let snapshot_index = self.raft.snapshot_index();
        if app_applied_index >= snapshot_index {
            return Ok(());
        }
        let Some(entry) = entries.iter().min_by_key(|entry| entry.index) else {
            return Ok(());
        };
        Err(GroupError::SnapshotRestoreRequired {
            app_applied_index,
            snapshot_index,
            entry_index: entry.index,
        })
    }
}

/// The lowest committed application entry `outputs` would apply.
///
/// Named rather than "whether any exists" because the refusal reports it: an
/// operator reading the error learns which entry the replica stopped in front
/// of, and can compare it against the boundary to see how wide the gap was.
fn lowest_application_entry(outputs: &[RaftOutput]) -> Option<LogIndex> {
    outputs
        .iter()
        .filter_map(|output| match output {
            RaftOutput::Apply { index, .. } => Some(*index),
            _ => None,
        })
        .min()
}

/// Whether `outputs` already carries an install that clears `boundary`.
fn installs_through(outputs: &[RaftOutput], boundary: LogIndex) -> bool {
    outputs.iter().any(|output| match output {
        RaftOutput::ApplySnapshot { snapshot } => snapshot.metadata.last_included_index >= boundary,
        _ => false,
    })
}

/// Puts the install in front of the suffix.
///
/// The pump would install first from anywhere in the vector — it handles
/// [`RaftOutput::ApplySnapshot`] as it walks and holds
/// [`RaftOutput::Apply`] back until the walk finishes — so this position is
/// the intent made visible rather than the mechanism. A reader of the routed
/// report sees the same order the operation promises.
fn install_ahead_of(snapshot: RaftSnapshot, outputs: Vec<RaftOutput>) -> Vec<RaftOutput> {
    let mut restored = Vec::with_capacity(outputs.len() + 1);
    restored.push(RaftOutput::ApplySnapshot { snapshot });
    restored.extend(outputs);
    restored
}
