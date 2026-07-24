#![allow(clippy::wildcard_imports)]

mod support;

use std::sync::{Arc, Mutex};

use rafter_runtime::RaftRuntimeError;
use rafter_runtime_api::PersistedRaftRuntime;

use support::*;

#[test]
fn in_memory_driver_rejects_waiters_after_shutdown() {
    let driver = elected_driver();
    let handle = driver.handle();

    block_on(handle.shutdown()).expect("shutdown succeeds");

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::ShuttingDown)
    );
    assert_eq!(
        block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable)),
        Err(ReadError::ShuttingDown)
    );
    assert_eq!(
        block_on(handle.transfer_leadership(NodeId(2))),
        Err(TransferLeadershipError::ShuttingDown)
    );
}

#[test]
fn in_memory_driver_resolves_waiters_when_apply_poisons_group() {
    let driver = KvDriver::new_elected(
        NodeId(1),
        vec![group_with_app(
            1,
            &[],
            3,
            KvStateMachine {
                fail_apply: true,
                ..KvStateMachine::default()
            },
        )],
    )
    .expect("single-node primary elects");
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::GroupPoisoned,
        })
    );
    assert!(matches!(
        block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable)),
        Err(ReadError::Poisoned { .. })
    ));
}

#[test]
fn in_memory_driver_write_batch_submits_one_runtime_batch_and_preserves_order() {
    let stats = Arc::new(Mutex::new(BatchRuntimeStats::default()));
    let group = RaftGroup::new(
        (),
        NodeId(1),
        BatchRecordingRuntime {
            stats: stats.clone(),
            next_index: LogIndex(1),
        },
        KvStateMachine::default(),
    );
    let driver = InMemoryRaftDriver::new(NodeId(1), vec![group]).expect("driver builds");

    let outcomes = block_on(driver.write_batch(
        (),
        vec![
            WriteBatchEntry::new(("alpha".to_owned(), "one".to_owned())),
            WriteBatchEntry::new(("beta".to_owned(), "two".to_owned())),
        ],
    ));

    assert_eq!(
        outcomes,
        vec![
            Ok(WriteReceipt {
                index: LogIndex(1),
                term: Term(1),
                result: None,
            }),
            Ok(WriteReceipt {
                index: LogIndex(2),
                term: Term(1),
                result: None,
            }),
        ]
    );
    let stats = stats.lock().expect("stats lock");
    assert!(stats.step_batches.is_empty());
    assert_eq!(stats.proposal_batches.len(), 1);
    assert_eq!(
        stats.proposal_batches[0],
        vec![
            ClientProposalInput {
                proposal_id: Some(LocalProposalId(1)),
                payload: b"alpha\none".to_vec(),
            },
            ClientProposalInput {
                proposal_id: Some(LocalProposalId(2)),
                payload: b"beta\ntwo".to_vec(),
            },
        ]
    );
}

#[test]
fn in_memory_driver_write_batch_reserves_local_ids_all_or_nothing() {
    let stats = Arc::new(Mutex::new(BatchRuntimeStats::default()));
    let mut group = RaftGroup::new(
        (),
        NodeId(1),
        BatchRecordingRuntime {
            stats: stats.clone(),
            next_index: LogIndex(1),
        },
        KvStateMachine::default(),
    );
    group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: LocalProposalId(u64::MAX - 1),
            client_request_id: None,
            command: ("manual".to_owned(), "one".to_owned()),
        })
        .expect("manual proposal seeds adopted watermark");
    let batches_before_driver = stats.lock().expect("stats lock").step_batches.len();
    let driver = InMemoryRaftDriver::new(NodeId(1), vec![group]).expect("driver builds");

    let exhausted = block_on(driver.write_batch(
        (),
        vec![
            WriteBatchEntry::new(("alpha".to_owned(), "one".to_owned())),
            WriteBatchEntry::new(("beta".to_owned(), "two".to_owned())),
        ],
    ));

    assert_eq!(
        exhausted,
        vec![
            Err(WriteError::LocalProposalIdExhausted),
            Err(WriteError::LocalProposalIdExhausted),
        ]
    );
    assert_eq!(
        stats.lock().expect("stats lock").step_batches.len(),
        batches_before_driver,
        "failed reservation must not submit a runtime batch"
    );
    assert!(
        stats
            .lock()
            .expect("stats lock")
            .proposal_batches
            .is_empty(),
        "failed reservation must not submit a proposal batch"
    );

    let last_id_outcome = block_on(driver.write_batch(
        (),
        vec![WriteBatchEntry::new(("last".to_owned(), "ok".to_owned()))],
    ));

    assert!(matches!(last_id_outcome.as_slice(), [Ok(_)]));
    let stats = stats.lock().expect("stats lock");
    assert_eq!(stats.step_batches.len(), batches_before_driver);
    assert_eq!(stats.proposal_batches.len(), 1);
    assert_eq!(
        stats.proposal_batches[0],
        vec![ClientProposalInput {
            proposal_id: Some(LocalProposalId(u64::MAX)),
            payload: b"last\nok".to_vec(),
        }]
    );
}

#[test]
fn in_memory_driver_reports_not_leader_before_election() {
    let driver = KvDriver::new(NodeId(1), groups()).expect("driver builds");
    let handle = driver.handle();

    let error = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("follower rejects writes");

    assert!(matches!(
        error,
        WriteError::NotLeader {
            leader_hint: None,
            ..
        }
    ));
}

#[derive(Debug, Default)]
struct BatchRuntimeStats {
    step_batches: Vec<Vec<RaftInput>>,
    proposal_batches: Vec<Vec<ClientProposalInput>>,
}

struct BatchRecordingRuntime {
    stats: Arc<Mutex<BatchRuntimeStats>>,
    next_index: LogIndex,
}

impl PersistedRaftRuntime for BatchRecordingRuntime {
    type Error = RaftRuntimeError;

    fn id(&self) -> NodeId {
        NodeId(1)
    }

    fn leader_hint(&self) -> Option<NodeId> {
        Some(NodeId(1))
    }

    fn role(&self) -> Role {
        Role::Leader
    }

    fn current_term(&self) -> Term {
        Term(1)
    }

    fn commit_index(&self) -> LogIndex {
        self.last_recorded_index()
    }

    fn last_log_index(&self) -> LogIndex {
        self.last_recorded_index()
    }

    fn snapshot_index(&self) -> LogIndex {
        LogIndex::ZERO
    }

    /// Every index this fake records is an application proposal, and it
    /// commits each one as it records it.
    fn committed_application_index(&self) -> LogIndex {
        self.last_recorded_index()
    }

    fn membership(&self) -> MembershipConfig {
        MembershipConfig::stable(
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("scripted membership is valid"),
        )
    }

    fn replication(&self) -> Vec<ReplicationProgress> {
        Vec::new()
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        self.step_batch(vec![input])
    }

    fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        self.stats
            .lock()
            .expect("stats lock")
            .proposal_batches
            .push(proposals.clone());

        let mut outputs = Vec::new();
        for proposal in proposals {
            if let Some(proposal_id) = proposal.proposal_id {
                let index = self.next_index;
                self.next_index = self.next_index.next();
                outputs.push(RaftOutput::LocalProposalAppended {
                    proposal_id,
                    index,
                    term: Term(1),
                });
                outputs.push(RaftOutput::Apply {
                    index,
                    term: Term(1),
                    payload: SharedPayload::from(proposal.payload),
                    local_proposal_id: Some(proposal_id),
                });
            }
        }
        Ok(outputs)
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, RaftRuntimeError> {
        self.stats
            .lock()
            .expect("stats lock")
            .step_batches
            .push(inputs.clone());

        let mut outputs = Vec::new();
        for input in inputs {
            if let RaftInput::TrackedClientProposal {
                proposal_id,
                payload,
            } = input
            {
                let index = self.next_index;
                self.next_index = self.next_index.next();
                outputs.push(RaftOutput::LocalProposalAppended {
                    proposal_id,
                    index,
                    term: Term(1),
                });
                outputs.push(RaftOutput::Apply {
                    index,
                    term: Term(1),
                    payload: SharedPayload::from(payload),
                    local_proposal_id: Some(proposal_id),
                });
            }
        }
        Ok(outputs)
    }

    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        (index <= self.last_log_index()).then_some(Term(1))
    }
}

impl BatchRecordingRuntime {
    const fn last_recorded_index(&self) -> LogIndex {
        LogIndex(self.next_index.0.saturating_sub(1))
    }
}

#[test]
fn in_memory_driver_reports_proposal_id_exhaustion_after_max() {
    let mut adopted = group(1, &[], 3);
    adopted
        .begin_proposal_outcome(Proposal {
            local_proposal_id: LocalProposalId(u64::MAX - 1),
            client_request_id: None,
            command: ("manual".to_owned(), "one".to_owned()),
        })
        .expect("manual proposal consumes the penultimate local proposal id");
    let driver = KvDriver::new_elected(NodeId(1), vec![adopted])
        .expect("quiescent manually driven group is adoptable");
    let handle = driver.handle();

    block_on(handle.write(("last".to_owned(), "ok".to_owned())))
        .expect("last proposal ID may be used once");
    assert_eq!(
        block_on(handle.write(("again".to_owned(), "no".to_owned()))),
        Err(WriteError::LocalProposalIdExhausted)
    );
}

#[test]
fn in_memory_driver_bounds_pending_writes_and_publishes_metrics() {
    let driver = scripted_write_driver(ScriptedWriteMode::AppendThenCycle);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::DriveBoundReached,
        })
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        1,
        "bounded unknown outcome publishes pending proposal metrics"
    );
}

#[test]
fn in_memory_driver_publishes_metrics_for_empty_network_unknown_outcome() {
    let driver = scripted_write_driver(ScriptedWriteMode::AppendThenIdle);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::EmptyNetwork,
        })
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        1,
        "empty-network unknown outcome publishes pending proposal metrics"
    );
}

#[test]
fn in_memory_driver_maps_post_append_dispatch_error_to_unknown_outcome() {
    let driver = scripted_write_driver(ScriptedWriteMode::AppendThenMissingNode);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::PostAppendDriverError,
        })
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        1,
        "post-append unknown outcome publishes pending proposal metrics"
    );
}

#[test]
fn in_memory_driver_preserves_pre_append_runtime_error() {
    let driver = scripted_write_driver(ScriptedWriteMode::PreAppendRuntimeError);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::Storage {
            message: "persisted Raft log diverges from committed state at index 1".to_owned(),
        })
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        0,
        "pre-append runtime error does not leak pending proposal state"
    );
}

#[test]
fn in_memory_driver_maps_no_lifecycle_proposal_output_to_unknown_outcome() {
    let driver = scripted_write_driver(ScriptedWriteMode::PreAppendNoLifecycleMessage);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.write(("alpha".to_owned(), "one".to_owned()))),
        Err(WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::RuntimeDroppedProposal,
        })
    );
    assert_eq!(
        handle
            .metrics()
            .expect("metrics")
            .current()
            .pending_proposals,
        0,
        "malformed no-lifecycle output is cleaned up before returning"
    );
}
