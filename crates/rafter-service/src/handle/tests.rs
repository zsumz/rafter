use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};

use rafter::{
    LocalProposalId, LogIndex, MembershipConfig, MembershipSet, NodeId, ProposalRejection, ReadId,
    ReplicationProgress, Role, Term,
};
use rafter_app::{
    group::GroupFatalState,
    metrics::RaftGroupMetrics,
    proposal::ClientRequestId,
    read::{ReadConsistency, ReadProof},
};

use super::*;
use crate::{
    driver::{DriverFuture, QueryReceipt, ReadOptions, WriteOptions, WriteReceipt},
    error::{MetricsError, ReadError, ShutdownError, TransferLeadershipError, WriteError},
    watch::MetricsWatch,
};

type TestHandle = RaftHandle<u64, String, String, String, String, RecordingSender>;

#[derive(Clone, Debug, Default)]
struct RecordingSender {
    inner: Arc<Mutex<RecordingState>>,
}

#[derive(Debug)]
struct RecordingState {
    writes: Vec<(u64, String, WriteOptions)>,
    reads: Vec<(u64, String, ReadConsistency, ReadOptions)>,
    transfers: Vec<(u64, NodeId)>,
    metrics_requests: Vec<u64>,
    shutdowns: Vec<u64>,
    shutting_down: bool,
    next_writes: VecDeque<Result<WriteReceipt<String>, WriteError>>,
    next_reads: VecDeque<Result<QueryReceipt<u64, String>, ReadError>>,
    next_transfers: VecDeque<Result<(), TransferLeadershipError>>,
    metrics: RaftGroupMetrics<u64>,
}

impl Default for RecordingState {
    fn default() -> Self {
        Self {
            writes: Vec::new(),
            reads: Vec::new(),
            transfers: Vec::new(),
            metrics_requests: Vec::new(),
            shutdowns: Vec::new(),
            shutting_down: false,
            next_writes: VecDeque::new(),
            next_reads: VecDeque::new(),
            next_transfers: VecDeque::new(),
            metrics: metrics(7),
        }
    }
}

impl RecordingSender {
    fn push_write(&self, result: Result<WriteReceipt<String>, WriteError>) {
        self.inner
            .lock()
            .expect("lock state")
            .next_writes
            .push_back(result);
    }

    fn push_read(&self, result: Result<QueryReceipt<u64, String>, ReadError>) {
        self.inner
            .lock()
            .expect("lock state")
            .next_reads
            .push_back(result);
    }

    fn push_transfer(&self, result: Result<(), TransferLeadershipError>) {
        self.inner
            .lock()
            .expect("lock state")
            .next_transfers
            .push_back(result);
    }
}

impl DriverCommandSender<u64, String, String, String, String> for RecordingSender {
    fn write(
        &self,
        group_id: u64,
        command: String,
        options: WriteOptions,
    ) -> DriverFuture<Result<WriteReceipt<String>, WriteError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = inner.lock().expect("lock state");
            if state.shutting_down {
                return Err(WriteError::ShuttingDown);
            }
            state.writes.push((group_id, command, options));
            state.next_writes.pop_front().unwrap_or_else(|| {
                Ok(WriteReceipt {
                    index: LogIndex(3),
                    term: Term(2),
                    result: "applied".to_owned(),
                })
            })
        })
    }

    fn read(
        &self,
        group_id: u64,
        query: String,
        consistency: ReadConsistency,
        options: ReadOptions,
    ) -> DriverFuture<Result<QueryReceipt<u64, String>, ReadError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = inner.lock().expect("lock state");
            if state.shutting_down {
                return Err(ReadError::ShuttingDown);
            }
            state.reads.push((group_id, query, consistency, options));
            state.next_reads.pop_front().unwrap_or_else(|| {
                Ok(QueryReceipt {
                    result: "value".to_owned(),
                    proof: None,
                })
            })
        })
    }

    fn transfer_leadership(
        &self,
        group_id: u64,
        target: NodeId,
    ) -> DriverFuture<Result<(), TransferLeadershipError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = inner.lock().expect("lock state");
            if state.shutting_down {
                return Err(TransferLeadershipError::ShuttingDown);
            }
            state.transfers.push((group_id, target));
            state.next_transfers.pop_front().unwrap_or(Ok(()))
        })
    }

    fn metrics(&self, group_id: u64) -> Result<MetricsWatch<u64>, MetricsError> {
        let mut state = self.inner.lock().expect("lock state");
        state.metrics_requests.push(group_id);
        Ok(MetricsWatch::new(state.metrics.clone()))
    }

    fn shutdown(&self, group_id: u64) -> DriverFuture<Result<(), ShutdownError>> {
        let inner = self.inner.clone();
        Box::pin(async move {
            let mut state = inner.lock().expect("lock state");
            if state.shutting_down {
                return Err(ShutdownError::AlreadyShutDown);
            }
            state.shutting_down = true;
            state.shutdowns.push(group_id);
            Ok(())
        })
    }
}

#[test]
fn write_returns_receipt_after_driver_reports_apply() {
    let sender = RecordingSender::default();
    sender.push_write(Ok(WriteReceipt {
        index: LogIndex(9),
        term: Term(4),
        result: "stored".to_owned(),
    }));
    let handle = TestHandle::new(7, sender.clone());

    let receipt = block_on(handle.write("put a=1".to_owned())).expect("write succeeds");

    assert_eq!(
        receipt,
        WriteReceipt {
            index: LogIndex(9),
            term: Term(4),
            result: "stored".to_owned(),
        }
    );
    assert_eq!(
        sender.inner.lock().expect("lock state").writes,
        vec![(7, "put a=1".to_owned(), WriteOptions::default())]
    );
}

#[test]
fn write_options_and_unknown_outcome_are_preserved() {
    let sender = RecordingSender::default();
    let client_request_id = ClientRequestId {
        client_id: 10,
        sequence: 99,
    };
    sender.push_write(Err(WriteError::UnknownOutcome {
        local_proposal_id: LocalProposalId(42),
        client_request_id: Some(client_request_id),
        reason: crate::error::UnknownOutcomeReason::RuntimeDroppedProposal,
    }));
    let handle = TestHandle::new(7, sender.clone());

    let error = block_on(handle.write_with_options(
        "put a=1".to_owned(),
        WriteOptions {
            client_request_id: Some(client_request_id),
        },
    ))
    .expect_err("unknown outcome is surfaced");

    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                local_proposal_id: LocalProposalId(42),
                client_request_id: Some(actual),
                reason: crate::error::UnknownOutcomeReason::RuntimeDroppedProposal,
            } if actual == client_request_id
        ),
        "got {error:?}"
    );
    assert_eq!(
        sender.inner.lock().expect("lock state").writes,
        vec![(
            7,
            "put a=1".to_owned(),
            WriteOptions {
                client_request_id: Some(client_request_id),
            },
        )]
    );
}

#[test]
fn not_leader_error_carries_leader_hint() {
    let sender = RecordingSender::default();
    sender.push_write(Err(WriteError::NotLeader {
        leader_hint: Some(NodeId(2)),
        term: Term(8),
    }));
    let handle = TestHandle::new(7, sender);

    let error = block_on(handle.write("put".to_owned())).expect_err("write rejects");

    assert!(
        matches!(
            error,
            WriteError::NotLeader {
                leader_hint: Some(NodeId(2)),
                term: Term(8),
            }
        ),
        "got {error:?}"
    );
}

#[test]
fn read_passes_consistency_and_returns_query_receipt() {
    let sender = RecordingSender::default();
    let proof = ReadProof {
        group_id: 7,
        issued_by: NodeId(1),
        term: Term(3),
        read_index: LogIndex(11),
        required_applied_index: LogIndex(11),
        local_applied_index: LogIndex(12),
    };
    sender.push_read(Ok(QueryReceipt {
        result: "1".to_owned(),
        proof: Some(proof.clone()),
    }));
    let handle = TestHandle::new(7, sender.clone());

    let receipt = block_on(handle.read("get a".to_owned(), ReadConsistency::Linearizable))
        .expect("read succeeds");

    assert_eq!(
        receipt,
        QueryReceipt {
            result: "1".to_owned(),
            proof: Some(proof),
        }
    );
    assert_eq!(
        sender.inner.lock().expect("lock state").reads,
        vec![(
            7,
            "get a".to_owned(),
            ReadConsistency::Linearizable,
            ReadOptions::default()
        )]
    );
}

/// The floor a caller supplies must arrive at the driver as the caller wrote
/// it. A handle that dropped it would turn a read-your-writes request into an
/// ordinary read with no way for the caller to tell.
#[test]
fn read_with_options_hands_the_callers_floor_to_the_driver() {
    let sender = RecordingSender::default();
    sender.push_read(Ok(QueryReceipt {
        result: "1".to_owned(),
        proof: None,
    }));
    let handle = TestHandle::new(7, sender.clone());

    block_on(handle.read_with_options(
        "get a".to_owned(),
        ReadConsistency::Linearizable,
        ReadOptions::default().with_min_applied_index(LogIndex(9)),
    ))
    .expect("read succeeds");

    assert_eq!(
        sender.inner.lock().expect("lock state").reads,
        vec![(
            7,
            "get a".to_owned(),
            ReadConsistency::Linearizable,
            ReadOptions::default().with_min_applied_index(LogIndex(9))
        )]
    );
}

#[test]
fn transfer_request_metrics_membership_and_shutdown_use_group_identity() {
    let sender = RecordingSender::default();
    sender.push_transfer(Ok(()));
    let handle = TestHandle::new(7, sender.clone());

    block_on(handle.transfer_leadership(NodeId(3))).expect("transfer request accepted");
    assert_eq!(
        sender.inner.lock().expect("lock state").transfers,
        vec![(7, NodeId(3))]
    );
    assert_eq!(handle.membership().group_id(), &7);
    assert_eq!(handle.metrics().expect("metrics").current().group_id, 7);
    assert_eq!(
        sender.inner.lock().expect("lock state").metrics_requests,
        vec![7]
    );

    block_on(handle.shutdown()).expect("shutdown succeeds");
    assert_eq!(sender.inner.lock().expect("lock state").shutdowns, vec![7]);
}

#[test]
fn shutdown_rejects_later_waiters() {
    let sender = RecordingSender::default();
    let handle = TestHandle::new(7, sender);

    block_on(handle.shutdown()).expect("shutdown succeeds");

    let write = block_on(handle.write("put".to_owned())).expect_err("the handle is closed");
    let read = block_on(handle.read("get".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the handle is closed");
    let transfer =
        block_on(handle.transfer_leadership(NodeId(2))).expect_err("the handle is closed");
    let shutdown = block_on(handle.shutdown()).expect_err("the handle is already closed");

    assert!(matches!(write, WriteError::ShuttingDown), "got {write:?}");
    assert!(matches!(read, ReadError::ShuttingDown), "got {read:?}");
    assert!(
        matches!(transfer, TransferLeadershipError::ShuttingDown),
        "got {transfer:?}"
    );
    assert!(
        matches!(shutdown, ShutdownError::AlreadyShutDown),
        "got {shutdown:?}"
    );
}

#[test]
fn payload_rejection_is_represented_without_success_receipt() {
    let sender = RecordingSender::default();
    sender.push_write(Err(WriteError::Rejected {
        reason: ProposalRejection::PayloadTooLarge {
            payload_len: 1024,
            max_payload_len: 512,
        },
    }));
    let handle = TestHandle::new(7, sender);

    let error = block_on(handle.write("oversized".to_owned())).expect_err("write rejects");

    assert!(
        matches!(
            error,
            WriteError::Rejected {
                reason: ProposalRejection::PayloadTooLarge {
                    payload_len: 1024,
                    max_payload_len: 512,
                },
            }
        ),
        "got {error:?}"
    );
}

fn metrics(group_id: u64) -> RaftGroupMetrics<u64> {
    RaftGroupMetrics {
        group_id,
        node_id: NodeId(1),
        role: Role::Leader,
        term: Term(2),
        leader_hint: Some(NodeId(1)),
        commit_index: LogIndex(3),
        applied_index: LogIndex(3),
        last_log_index: LogIndex(3),
        snapshot_index: LogIndex::ZERO,
        membership: MembershipConfig::Stable(
            MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("valid membership"),
        ),
        replication: Vec::<ReplicationProgress>::new(),
        pending_proposals: 0,
        pending_reads: 0,
        pending_read_barriers: 0,
        pending_query_reads: 0,
        completed_query_reads: 0,
        reserved_reads: 0,
        fatal_state: GroupFatalState::Healthy,
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}

#[test]
fn read_rejection_can_include_leader_hint() {
    let sender = RecordingSender::default();
    sender.push_read(Err(ReadError::Rejected {
        read_id: Some(ReadId(5)),
        reason: rafter::ReadIndexRejection::NotLeader {
            role: Role::Follower,
            term: Term(6),
        },
        leader_hint: Some(NodeId(4)),
    }));
    let handle = TestHandle::new(7, sender);

    let error = block_on(handle.read("get".to_owned(), ReadConsistency::Linearizable))
        .expect_err("read rejects");

    assert!(
        matches!(
            error,
            ReadError::Rejected {
                read_id: Some(ReadId(5)),
                reason: rafter::ReadIndexRejection::NotLeader {
                    role: Role::Follower,
                    term: Term(6),
                },
                leader_hint: Some(NodeId(4)),
            }
        ),
        "got {error:?}"
    );
}
