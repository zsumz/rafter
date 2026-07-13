use super::*;

#[test]
fn leader_proposal_log_entry_is_persisted_before_apply_output_escapes() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader elected")
        .is_empty());

    let outputs = runtime
        .step(RaftInput::ClientProposal {
            payload: b"create".to_vec(),
        })
        .expect("log entry persists");

    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            PersistedRaftLogEntry::noop(LogIndex(1), Term(1)),
            PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"create".to_vec(),)
        ]
    );
    assert_eq!(
        outputs,
        vec![RaftOutput::Apply {
            index: LogIndex(2),
            term: Term(1),
            payload: b"create".to_vec().into(),
            local_proposal_id: None,
        }]
    );
}

#[test]
fn follower_append_entries_are_persisted_before_success_response_escapes() {
    let mut runtime = durable_node_with_log(
        2,
        &[1, 3],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );

    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(2),
                leader_id: RaftNodeId(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: vec![LogEntry::application(Term(2), b"append".to_vec())].into(),
                leader_commit: LogIndex::ZERO,
            }),
        })
        .expect("log entry persists");

    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![PersistedRaftLogEntry::application(
            LogIndex(1),
            Term(2),
            b"append".to_vec(),
        )]
    );
    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::Send {
            message: Message::AppendEntriesResponse(response),
            ..
        }] if response.success && response.match_index == LogIndex(1)
    ));
}

#[test]
fn configuration_proposal_log_entry_is_persisted_before_outputs_escape() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader elected")
        .is_empty());
    let configuration = learner_configuration_entry(ConfigurationId(1));

    let outputs = runtime
        .step(RaftInput::AddLearner {
            learner_id: RaftNodeId(2),
        })
        .expect("configuration entry persists");

    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            PersistedRaftLogEntry::noop(LogIndex(1), Term(1)),
            PersistedRaftLogEntry::configuration(LogIndex(2), Term(1), configuration.clone(),)
        ]
    );
    assert_eq!(
        runtime.effective_configuration_entry(),
        Some(configuration.clone())
    );
    assert_eq!(runtime.committed_configuration_entry(), Some(configuration));
    assert!(!outputs
        .iter()
        .any(|output| matches!(output, RaftOutput::Apply { .. })));
}

#[test]
fn committed_configuration_write_failure_suppresses_dependent_membership_output() {
    let mut control = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    assert!(control
        .step(RaftInput::Tick)
        .expect("control leader elected")
        .is_empty());
    let control_outputs = control
        .step(RaftInput::AddLearner {
            learner_id: RaftNodeId(2),
        })
        .expect("control configuration commits");
    assert!(control_outputs.iter().any(|output| matches!(
        output,
        RaftOutput::Send {
            to: RaftNodeId(2),
            message: Message::AppendEntries(request),
        } if request.leader_commit == LogIndex(2)
    )));

    let mut runtime = durable_node_with_log(
        1,
        &[],
        FailingCommittedConfigurationHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader elected before failure is armed")
        .is_empty());
    let configuration = learner_configuration_entry(ConfigurationId(1));
    let committed_configuration = CommittedConfiguration {
        index: LogIndex(2),
        config_id: ConfigurationId(1),
    };

    let error = runtime
        .step(RaftInput::AddLearner {
            learner_id: RaftNodeId(2),
        })
        .expect_err("committed configuration write fails before outputs escape");

    assert!(matches!(
        error,
        RaftRuntimeError::HardStateWrite(RaftHardStateStoreWriteError::Io {
            operation: "write committed configuration test hard state",
            ..
        })
    ));
    assert_eq!(
        runtime.hard_state_store.rejected,
        Some(RaftHardState {
            current_term: Term(1),
            voted_for: Some(RaftNodeId(1)),
            commit_index: LogIndex(2),
            committed_configuration: Some(committed_configuration),
        })
    );
    assert_eq!(
        runtime.hard_state_store.current(),
        RaftHardState {
            current_term: Term(1),
            voted_for: Some(RaftNodeId(1)),
            commit_index: LogIndex(1),
            committed_configuration: None,
        }
    );
    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            PersistedRaftLogEntry::noop(LogIndex(1), Term(1)),
            PersistedRaftLogEntry::configuration(LogIndex(2), Term(1), configuration.clone(),),
        ]
    );

    let reopened = durable_node_with_log(
        1,
        &[],
        runtime.hard_state_store.durable.clone(),
        runtime.log_segment.clone(),
    );
    assert_eq!(reopened.commit_index(), LogIndex(1));
    assert_eq!(reopened.committed_configuration_entry(), None);
    assert_eq!(
        reopened.effective_configuration_entry(),
        Some(configuration)
    );
    assert_eq!(
        reopened.committed_membership(),
        MembershipConfig::stable(membership_set(&[1]))
    );
}

#[test]
fn log_append_failure_suppresses_apply_outputs() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        FailingAfterElectionNoopLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader elected")
        .is_empty());

    let error = runtime
        .step(RaftInput::ClientProposal {
            payload: b"create".to_vec(),
        })
        .expect_err("log append fails");

    assert!(matches!(
        error,
        RaftRuntimeError::LogAppend(RaftLogSegmentAppendError::Io {
            operation: "append test raft log entries",
            ..
        })
    ));
    // Poisoned accessors may run ahead of durability; the contract is that
    // nothing durable recorded the entry and no output ever will.
    let error = runtime
        .step(RaftInput::Tick)
        .expect_err("a poisoned runtime refuses further inputs");
    assert!(matches!(error, RaftRuntimeError::Poisoned { .. }));
}

#[test]
fn log_append_failure_poisons_runtime_until_restart() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        FailingAfterElectionNoopLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader elected")
        .is_empty());

    let error = runtime
        .step(RaftInput::ClientProposal {
            payload: b"create".to_vec(),
        })
        .expect_err("log append fails");
    assert!(matches!(error, RaftRuntimeError::LogAppend(_)));

    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(cause, RaftRuntimeFatalError::LogAppend(_))
    });
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct FailingCommittedConfigurationHardStateStore {
    durable: InMemoryRaftHardStateStore,
    rejected: Option<RaftHardState>,
}

impl FailingCommittedConfigurationHardStateStore {
    fn new() -> Self {
        Self::default()
    }
}

impl RaftHardStateStore for FailingCommittedConfigurationHardStateStore {
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        if state.committed_configuration != self.durable.current().committed_configuration {
            self.rejected = Some(state);
            return Err(RaftHardStateStoreWriteError::Io {
                operation: "write committed configuration test hard state",
                path: PathBuf::from("test-committed-configuration-hard-state"),
                message: "injected failure".to_string(),
            });
        }
        self.durable.write_hard_state(state)
    }

    fn current(&self) -> RaftHardState {
        self.durable.current()
    }
}
