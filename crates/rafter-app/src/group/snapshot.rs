use super::{
    ApplicationSnapshot, Debug, GroupResult, GroupStepReport, LogIndex, PersistedRaftRuntime,
    RaftGroup, RaftSnapshot, ReplicatedStateMachine, SnapshotEvent, StateMachineOperation,
};

impl<G, A, R> RaftGroup<G, A, R>
where
    G: Clone + Ord + Debug + Send + Sync + 'static,
    A: ReplicatedStateMachine,
    A::CommandResult: Clone,
    R: PersistedRaftRuntime,
{
    pub(super) fn apply_snapshot_output(
        &mut self,
        snapshot: RaftSnapshot,
        report: &mut GroupStepReport<G, A::CommandResult>,
    ) -> GroupResult<A, R, ()> {
        self.validate_snapshot_output(&snapshot)?;
        let application_snapshot = ApplicationSnapshot {
            applied_index: snapshot.metadata.last_included_index,
            payload: Vec::new(),
            raft_snapshot: Some(snapshot.clone()),
        };
        self.app
            .install_snapshot(application_snapshot)
            .map_err(|source| {
                self.poison_with_state_machine_error(StateMachineOperation::InstallSnapshot, source)
            })?;
        self.verify_app_applied_index_at_least(snapshot.metadata.last_included_index)?;
        self.last_applied_index = self
            .last_applied_index
            .max(snapshot.metadata.last_included_index);
        report.snapshot_events.push(SnapshotEvent::Apply {
            group_id: self.group_id.clone(),
            snapshot,
        });
        Ok(())
    }

    pub(super) fn validate_snapshot_output(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> GroupResult<A, R, ()> {
        if snapshot.metadata.last_included_index == LogIndex::ZERO {
            return Err(self.poison_with_malformed_snapshot(
                "snapshot last included index is zero".to_owned(),
            ));
        }
        if snapshot.metadata.last_included_term.is_zero() {
            return Err(self
                .poison_with_malformed_snapshot("snapshot last included term is zero".to_owned()));
        }
        Ok(())
    }
}
