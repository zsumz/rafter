use super::{
    ApplicationSnapshot, ApplicationSnapshotError, Debug, GroupError, GroupResult, GroupStepReport,
    LogIndex, PersistedRaftRuntime, RaftGroup, RaftSnapshot, ReplicatedStateMachine, SnapshotEvent,
    SnapshotSupport, StateMachineOperation,
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
        let snapshot_index = snapshot.metadata.last_included_index;
        // The declaration is checked before the payload is built, so a state
        // machine that cannot interpret a snapshot never sees one. An
        // unrecognized future variant is treated as unsupported, which fails
        // closed.
        if !matches!(A::SNAPSHOT_SUPPORT, SnapshotSupport::Supported) {
            self.enter_poisoned(
                format!(
                    "state machine declares no application snapshot support; refused a Raft-driven install at index {snapshot_index}"
                ),
                None,
            );
            return Err(GroupError::SnapshotsUnsupported { snapshot_index });
        }
        let application_snapshot = ApplicationSnapshot {
            applied_index: snapshot_index,
            payload: Vec::new(),
            raft_snapshot: Some(snapshot.clone()),
        };
        match self.app.install_snapshot(application_snapshot) {
            Ok(()) => {}
            // The declaration said `Supported`, so this can only be an
            // inherited provided body. Naming the mistake beats reporting a
            // generic install failure.
            Err(ApplicationSnapshotError::Unsupported) => {
                self.enter_poisoned(
                    format!(
                        "state machine declares application snapshot support but refused the install at index {snapshot_index} as unsupported"
                    ),
                    None,
                );
                return Err(GroupError::SnapshotSupportMisdeclared { snapshot_index });
            }
            Err(ApplicationSnapshotError::StateMachine(source)) => {
                return Err(self.poison_with_state_machine_error(
                    StateMachineOperation::InstallSnapshot,
                    source,
                ));
            }
        }
        self.verify_app_applied_index_at_least(snapshot_index)?;
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
