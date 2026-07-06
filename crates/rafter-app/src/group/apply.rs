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
            self.enter_poisoned(format!(
                "state machine returned {} apply results for {expected_count} committed entries",
                results.len()
            ));
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
                self.enter_poisoned("state machine returned mismatched apply metadata".to_owned());
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
            self.enter_poisoned(format!(
                "state machine reported applied index {app_applied_index} below group applied floor {group_applied_index}"
            ));
            return Err(GroupError::AppliedIndexBehind {
                required: group_applied_index,
                actual: app_applied_index,
            });
        }

        if let Some(entry) = entries
            .iter()
            .find(|entry| entry.index <= app_applied_index)
        {
            self.enter_poisoned(format!(
                "refusing to replay committed entry {} because the state machine already reports applied index {app_applied_index}",
                entry.index
            ));
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
            self.enter_poisoned(format!(
                "state machine reported applied index {actual} below required {required}"
            ));
            return Err(GroupError::AppliedIndexBehind { required, actual });
        }
        Ok(actual)
    }

    pub(super) fn app_applied_index(&mut self) -> GroupResult<A, R, LogIndex> {
        self.app.applied_index().map_err(|source| {
            self.poison_with_state_machine_error(StateMachineOperation::AppliedIndex, source)
        })
    }
}
