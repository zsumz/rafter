use super::{
    ApplyBatch, ApplyEntry, ApplyEntryResult, Debug, GroupError, GroupResult, GroupStepReport,
    LocalProposalId, LogIndex, PersistedRaftRuntime, ProposalEvent, RaftGroup,
    ReplicatedStateMachine, SharedPayload, StateMachineOperation, Term,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    pub(super) fn decode_apply_output(
        &mut self,
        index: LogIndex,
        term: Term,
        payload: &SharedPayload,
        local_proposal_id: Option<LocalProposalId>,
    ) -> ApplyEntryResult<A, R> {
        let command = self
            .app
            .decode_command(payload.as_ref())
            .map_err(|source| {
                self.poison_with_state_machine_error(StateMachineOperation::DecodeCommand, source)
            })?;
        Ok(ApplyEntry {
            index,
            term,
            command,
            local_proposal_id,
        })
    }
    pub(super) fn apply_entries(
        &mut self,
        entries: Vec<ApplyEntry<A::Command>>,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) -> GroupResult<A, R, ()> {
        if entries.is_empty() {
            return Ok(());
        }

        self.validate_apply_floor(&entries)?;

        let expected_metadata = entries
            .iter()
            .map(|entry| (entry.index, entry.term, entry.local_proposal_id))
            .collect::<Vec<_>>();
        let expected_count = expected_metadata.len();
        let results = self
            .app
            .apply_batch(ApplyBatch { entries })
            .map_err(|source| {
                self.poison_with_state_machine_error(StateMachineOperation::ApplyBatch, source)
            })?;
        if results.len() != expected_count {
            self.enter_poisoned(
                format!(
                    "state machine returned {} apply results for {expected_count} committed entries",
                    results.len()
                ),
                None,
            );
            return Err(GroupError::ApplyResultCountMismatch {
                expected: expected_count,
                actual: results.len(),
            });
        }

        for ((expected_index, expected_term, expected_local_proposal_id), result) in
            expected_metadata.iter().zip(results.iter())
        {
            if result.index != *expected_index
                || result.term != *expected_term
                || result.local_proposal_id != *expected_local_proposal_id
            {
                self.enter_poisoned(
                    "state machine returned mismatched apply metadata".to_owned(),
                    None,
                );
                return Err(GroupError::ApplyResultMetadataMismatch {
                    expected_index: *expected_index,
                    actual_index: result.index,
                    expected_term: *expected_term,
                    actual_term: result.term,
                    expected_local_proposal_id: *expected_local_proposal_id,
                    actual_local_proposal_id: result.local_proposal_id,
                });
            }
        }

        let Some(required_applied_index) =
            expected_metadata.iter().map(|(index, _, _)| *index).max()
        else {
            return Ok(());
        };
        self.verify_app_applied_index_at_least(required_applied_index)?;

        for result in results {
            self.last_applied_index = self.last_applied_index.max(result.index);
            if let Some(local_proposal_id) = result.local_proposal_id {
                if self.pending_proposals.remove(&local_proposal_id).is_some() {
                    report.proposal_events.push(ProposalEvent::Applied {
                        local_proposal_id,
                        index: result.index,
                        term: result.term,
                        result: result.result.clone(),
                    });
                }
            }
            report.applied.push(result);
        }
        Ok(())
    }

    pub(super) fn validate_apply_floor(
        &mut self,
        entries: &[ApplyEntry<A::Command>],
    ) -> GroupResult<A, R, ()> {
        let app_applied_index = self.app_applied_index()?;
        let group_applied_index = self.last_applied_index;
        if app_applied_index < group_applied_index {
            self.enter_poisoned(
                format!(
                    "state machine reported applied index {app_applied_index} below group applied floor {group_applied_index}"
                ),
                None,
            );
            return Err(GroupError::AppliedIndexBehind {
                required: group_applied_index,
                actual: app_applied_index,
            });
        }

        self.reject_if_snapshot_restore_required(app_applied_index, entries)?;

        if let Some(entry) = entries
            .iter()
            .find(|entry| entry.index <= app_applied_index)
        {
            self.enter_poisoned(
                format!(
                    "refusing to replay committed entry {} because the state machine already reports applied index {app_applied_index}",
                    entry.index
                ),
                None,
            );
            return Err(GroupError::ApplyEntryAlreadyApplied {
                entry_index: entry.index,
                app_applied_index,
                group_applied_index,
            });
        }

        self.last_applied_index = self.last_applied_index.max(app_applied_index);
        Ok(())
    }

    pub(super) fn verify_app_applied_index_at_least(
        &mut self,
        required: LogIndex,
    ) -> GroupResult<A, R, LogIndex> {
        let actual = self.app_applied_index()?;
        if actual < required {
            self.enter_poisoned(
                format!("state machine reported applied index {actual} below required {required}"),
                None,
            );
            return Err(GroupError::AppliedIndexBehind { required, actual });
        }
        Ok(actual)
    }

    /// Refuses to run a group whose state machine sits below the runtime's
    /// snapshot boundary.
    ///
    /// This group is the only object that holds both halves of the
    /// composition, so it is the only one that can check the invariant they
    /// share: **at every moment the group would let the state machine answer
    /// for this replica, that state machine is at or above its own Raft
    /// snapshot boundary.** The kernel cannot check it — it raises a low
    /// declared floor to the boundary and says so, because it retains no
    /// covered entry and holds a snapshot descriptor rather than payload
    /// bytes. Neither can a state machine, which cannot see the Raft log.
    ///
    /// Below the boundary the missing entries do not exist in any form this
    /// composition can reach. The group's own log will not replay them, and
    /// the leader will not send a snapshot to a follower whose log already
    /// matches its own.
    ///
    /// # Where the verdict is taken, and where it is not
    ///
    /// "The state machine is below the boundary" is a fault at the moments
    /// listed here and a legitimate transient everywhere else, so the scope is
    /// stated in the direction the code uses it — the public methods that step
    /// the runtime or serve a state-machine read, each of which takes the
    /// verdict on entry, before it touches either half:
    ///
    /// * [`RaftGroup::step_with_options`], and therefore [`RaftGroup::step`]
    ///   and [`RaftGroup::begin_read_barrier`]. A step advances the protocol
    ///   on this replica's behalf — it votes, acknowledges replication, and
    ///   grants read indexes — for state the replica does not hold.
    /// * [`RaftGroup::begin_proposal`] and [`RaftGroup::begin_proposal_batch`],
    ///   by name. They step the runtime without routing through
    ///   `step_with_options`, so they do not inherit its verdict, and a
    ///   proposal is how a state machine's contents are extended.
    /// * [`RaftGroup::read`], at every consistency and on every branch,
    ///   including the two that never step the runtime: a retry that consumes
    ///   an already completed proof, and every [`crate::read::ReadRequest::Local`].
    ///   Serving a query hands the caller the contents of a state machine
    ///   short of acknowledged entries, which is the damage itself.
    ///
    /// That is every public method of this type that steps the runtime or
    /// serves a state-machine read. The two below reach neither and take no
    /// verdict, each for a reason of its own. [`RaftGroup::state_machine`] and
    /// [`RaftGroup::state_machine_mut`] hand out the state machine itself; a
    /// caller that reads through them is outside anything this group can
    /// mediate, and restoring through them is the documented repair.
    ///
    /// [`RaftGroup::apply_raft_outputs`] takes no *permanent* verdict, and that
    /// is the direction a correct recovery uses. An inbound snapshot is
    /// promoted durably before the application installs it, so a replica that
    /// crashed between those two writes opens legitimately below its own
    /// boundary; the repair is
    /// [`RaftGroup::apply_recovery_outputs`], which installs the snapshot and
    /// then applies the suffix as one operation. Poisoning the raw pump would
    /// refuse that recovery rather than the fault, and would depend on how the
    /// caller chunked one runtime step's outputs into calls, which is not a
    /// fact about the replica.
    ///
    /// Tolerating the transient is not the same as letting anything through
    /// it. A committed application entry aimed at a state machine still short
    /// of the boundary is refused on every apply path, with
    /// [`crate::error::GroupError::SnapshotRestoreRequired`] and without
    /// poison — see `reject_if_snapshot_restore_required`. Nothing escapes in
    /// the meantime either: a replica that never restores cannot step,
    /// propose, or read.
    ///
    /// [`RaftGroup::metrics`] takes no verdict either. It reports
    /// `applied_index` and `snapshot_index` side by side, which is the
    /// supported way to see that a declaration was raised, and an
    /// observability call that poisoned the group it reports on would destroy
    /// the evidence an operator called it for.
    ///
    /// The comparison is against the state machine's *current* applied index
    /// rather than the floor this group was constructed with, so a caller that
    /// restores the state machine from the snapshot after opening the runtime
    /// passes.
    pub(super) fn reject_if_below_snapshot_boundary(&mut self) -> GroupResult<A, R, ()> {
        let snapshot_index = self.raft.snapshot_index();
        if snapshot_index <= self.last_applied_index {
            return Ok(());
        }
        let app_applied_index = self.app_applied_index()?;
        self.last_applied_index = self.last_applied_index.max(app_applied_index);
        if app_applied_index >= snapshot_index {
            return Ok(());
        }
        self.enter_poisoned(
            format!(
                "state machine reported applied index {app_applied_index} below the snapshot boundary {snapshot_index}, whose entries are compacted and can never be applied"
            ),
            None,
        );
        Err(GroupError::AppliedIndexBelowSnapshotBoundary {
            app_applied_index,
            snapshot_index,
        })
    }

    pub(super) fn app_applied_index(&mut self) -> GroupResult<A, R, LogIndex> {
        self.app.applied_index().map_err(|source| {
            self.poison_with_state_machine_error(StateMachineOperation::AppliedIndex, source)
        })
    }
}
