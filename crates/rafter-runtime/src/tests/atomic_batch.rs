//! Atomic batches retain the runtime's output fence and recovery contract.
use super::file_backed_fixture::TestDirectory;
use super::*;
use rafter_storage::durable_batch::{
    DurableReceipt, PersistenceDomain, RaftPersistenceBatch, RaftPersistenceBatchError,
    WalRaftLogSegment, WalRaftNodeStores,
};

fn committed_append() -> RaftInput {
    RaftInput::Message {
        from: RaftNodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(2),
            leader_id: RaftNodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: vec![LogEntry::application(Term(2), b"committed".to_vec())].into(),
            leader_commit: LogIndex(1),
        }),
    }
}
#[test]
fn follower_append_and_commit_share_one_sync_then_recover_applied_entry() {
    let dir = TestDirectory::new("atomic-append");
    let (mut hard, log, snapshots) = WalRaftNodeStores::open(dir.path()).unwrap().into_parts();
    hard.write_hard_state(RaftHardState {
        current_term: Term(2),
        ..RaftHardState::default()
    })
    .unwrap();
    let mut node = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard,
        log,
        snapshots,
    )
    .unwrap();
    let before = node.log_segment.sync_count();
    let outputs = node.step(committed_append()).unwrap();
    assert_eq!(node.log_segment.sync_count() - before, 1);
    assert_eq!(node.hard_state_store.current().commit_index, LogIndex(1));
    assert!(outputs.iter().any(|o| matches!(
        o,
        RaftOutput::Apply {
            index: LogIndex(1),
            ..
        }
    )));
    assert!(outputs.iter().any(|o| matches!(
        o,
        RaftOutput::Send {
            message: Message::AppendEntriesResponse(AppendEntriesResponse { success: true, .. }),
            ..
        }
    )));
    drop(node);
    let (hard, log, snapshots) = WalRaftNodeStores::open(dir.path()).unwrap().into_parts();
    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard,
        log,
        snapshots,
    )
    .unwrap();
    let (node, outputs) = recovered.into_parts();
    assert_eq!(node.commit_index(), LogIndex(1));
    assert!(outputs.iter().any(|o| matches!(
        o,
        RaftOutput::Apply {
            index: LogIndex(1),
            ..
        }
    )));
}

#[derive(Debug)]
struct RefusingBatchLog {
    inner: WalRaftLogSegment,
    bad_receipt: bool,
}
impl RaftLogSegment for RefusingBatchLog {
    fn persist_batch(
        &mut self,
        domain: &PersistenceDomain,
        batch: RaftPersistenceBatch<'_>,
    ) -> Result<Option<DurableReceipt>, RaftPersistenceBatchError> {
        if self.bad_receipt {
            let mut receipt = self.inner.persist_batch(domain, batch)?.unwrap();
            receipt.domain = PersistenceDomain::new();
            Ok(Some(receipt))
        } else {
            Err(RaftPersistenceBatchError::Io {
                operation: "injected batch failure",
                source: std::io::Error::other("failed persistence").into(),
            })
        }
    }
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        self.inner.append_entries(entries)
    }
    fn truncate_suffix(&mut self, index: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        self.inner.truncate_suffix(index)
    }
    fn compact_prefix_through(
        &mut self,
        index: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        self.inner.compact_prefix_through(index)
    }
    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.inner.replay_entries()
    }
    fn next_index(&self) -> LogIndex {
        self.inner.next_index()
    }
    fn compacted_through(&self) -> LogIndex {
        self.inner.compacted_through()
    }
}
#[test]
fn failed_atomic_publication_suppresses_outputs_and_poisons_runtime() {
    failure(false);
}
#[test]
fn receipt_from_another_domain_suppresses_outputs_and_poisons_runtime() {
    failure(true);
}
fn failure(bad_receipt: bool) {
    let dir = TestDirectory::new("atomic-failure");
    let (mut hard, log, snapshots) = WalRaftNodeStores::open(dir.path()).unwrap().into_parts();
    hard.write_hard_state(RaftHardState {
        current_term: Term(2),
        ..RaftHardState::default()
    })
    .unwrap();
    let mut node = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard,
        RefusingBatchLog {
            inner: log,
            bad_receipt,
        },
        snapshots,
    )
    .unwrap();
    assert!(matches!(
        node.step(committed_append()),
        Err(RaftRuntimeError::PersistenceBatch(_))
    ));
    assert_poisoned_after_failure(&mut node, |cause| {
        matches!(cause, RaftRuntimeFatalError::PersistenceBatch(_))
    });
    if !bad_receipt {
        assert_eq!(node.hard_state_store.current().commit_index, LogIndex::ZERO);
        assert_eq!(node.log_segment.next_index(), LogIndex(1));
    }
}
#[test]
fn changed_term_retains_separate_term_log_and_commit_fences() {
    let dir = TestDirectory::new("atomic-term");
    let (hard, log, snapshots) = WalRaftNodeStores::open(dir.path()).unwrap().into_parts();
    let mut node = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard,
        log,
        snapshots,
    )
    .unwrap();
    let before = node.log_segment.sync_count();
    node.step(committed_append()).unwrap();
    assert_eq!(node.log_segment.sync_count() - before, 3);
    assert_eq!(node.hard_state_store.current().current_term, Term(2));
}
