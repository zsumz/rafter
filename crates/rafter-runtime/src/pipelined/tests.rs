//! A follower can finish replication while local persistence is held, without a commit.
use super::*;
use crate::{DurableRaftNode, RaftRuntimeError};
use rafter::{
    AppendEntries, AppendEntriesResponse, ClientProposalInput, Input, LogIndex, Message,
    NodeConfig, NodeId, Output, PreVoteResponse, RequestVoteResponse, Term,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftLogEntry, RaftLogSegment, RaftLogSegmentAppendError, RaftLogSegmentCompactError,
    RaftLogSegmentTruncateError,
};
use std::sync::mpsc;
use std::time::Duration;

#[derive(Debug, Default)]
pub(super) struct ControlledLog {
    inner: InMemoryRaftLogSegment,
    delay: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    fail: bool,
}
impl RaftLogSegment for ControlledLog {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        if let Some((started, release)) = self.delay.take() {
            started.send(()).unwrap();
            release.recv().unwrap();
        }
        if self.fail {
            return Err(RaftLogSegmentAppendError::Io {
                operation: "injected local write",
                source: std::io::Error::other("failed sync").into(),
            });
        }
        self.inner.append_entries(entries)
    }
    fn truncate_suffix(&mut self, i: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        self.inner.truncate_suffix(i)
    }
    fn compact_prefix_through(&mut self, i: LogIndex) -> Result<(), RaftLogSegmentCompactError> {
        self.inner.compact_prefix_through(i)
    }
    fn next_index(&self) -> LogIndex {
        self.inner.next_index()
    }
    fn compacted_through(&self) -> LogIndex {
        self.inner.compacted_through()
    }
    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.inner.replay_entries()
    }
}
pub(super) type Node =
    DurableRaftNode<InMemoryRaftHardStateStore, ControlledLog, InMemoryRaftSnapshotStore>;
pub(super) type Pipeline =
    PipelinedRaftNode<InMemoryRaftHardStateStore, ControlledLog, InMemoryRaftSnapshotStore>;
pub(super) type Work =
    Box<PersistenceWork<InMemoryRaftHardStateStore, ControlledLog, InMemoryRaftSnapshotStore>>;
fn node(id: u64) -> Node {
    DurableRaftNode::with_storage(
        NodeConfig::new(
            NodeId(id),
            (1..=3).filter(|i| *i != id).map(NodeId).collect(),
            1,
        )
        .unwrap(),
        InMemoryRaftHardStateStore::new(),
        ControlledLog::default(),
    )
    .unwrap()
}
pub(super) fn fixture() -> (Node, Node) {
    let mut leader = node(1);
    let mut follower = node(2);
    let outputs = leader.step(Input::Tick).unwrap();
    let term = outputs
        .iter()
        .find_map(|o| match o {
            Output::Send {
                message: Message::PreVote(r),
                ..
            } => Some(r.term),
            _ => None,
        })
        .unwrap();
    let outputs = leader
        .step(Input::Message {
            from: NodeId(2),
            message: Message::PreVoteResponse(PreVoteResponse {
                term,
                voter_id: NodeId(2),
                vote_granted: true,
            }),
        })
        .unwrap();
    let term = outputs
        .iter()
        .find_map(|o| match o {
            Output::Send {
                message: Message::RequestVote(r),
                ..
            } => Some(r.term),
            _ => None,
        })
        .unwrap();
    let outputs = leader
        .step(Input::Message {
            from: NodeId(2),
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term,
                voter_id: NodeId(2),
                vote_granted: true,
            }),
        })
        .unwrap();
    let request = append_to_two(&outputs);
    for response in follower
        .step(Input::Message {
            from: NodeId(1),
            message: Message::AppendEntries(request),
        })
        .unwrap()
    {
        if let Output::Send {
            to: NodeId(1),
            message,
        } = response
        {
            leader
                .step(Input::Message {
                    from: NodeId(2),
                    message,
                })
                .unwrap();
        }
    }
    assert_eq!(leader.commit_index(), LogIndex(1));
    (leader, follower)
}
pub(super) fn proposals() -> Vec<ClientProposalInput> {
    vec![ClientProposalInput {
        proposal_id: None,
        payload: b"two".to_vec(),
    }]
}
fn append_to_two(outputs: &[Output]) -> AppendEntries {
    outputs
        .iter()
        .find_map(|o| match o {
            Output::Send {
                to: NodeId(2),
                message: Message::AppendEntries(r),
            } => Some(r.clone()),
            _ => None,
        })
        .unwrap()
}
fn prepare(node: &mut Pipeline) -> (Vec<Output>, Work) {
    match node.prepare_proposals(proposals()).unwrap() {
        PreparedProposals::Pending { replication, work } => (replication, work),
        PreparedProposals::Durable(_) => panic!("expected overlap"),
    }
}

#[test]
fn follower_persistence_overlaps_local_write_but_quorum_waits_for_its_receipt() {
    let (mut leader, mut follower) = fixture();
    let (started_tx, started_rx) = mpsc::sync_channel(1);
    let (release_tx, release_rx) = mpsc::sync_channel(1);
    leader.log_segment.delay = Some((started_tx, release_rx));
    let mut pipeline = Pipeline::new(leader);
    let (replication, work) = prepare(&mut pipeline);
    assert!(replication.iter().all(|o| matches!(
        o,
        Output::Send {
            message: Message::AppendEntries(_),
            ..
        }
    )));
    let handle = std::thread::spawn(move || work.persist());
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    let response = follower
        .step(Input::Message {
            from: NodeId(1),
            message: Message::AppendEntries(append_to_two(&replication)),
        })
        .unwrap()
        .into_iter()
        .find_map(|o| match o {
            Output::Send {
                to: NodeId(1),
                message: Message::AppendEntriesResponse(response),
            } => Some(response),
            _ => None,
        })
        .unwrap();
    assert!(response.success);
    assert_eq!(follower.log_segment.next_index(), LogIndex(3));
    assert_eq!(
        pipeline.progress(),
        PipelineProgress {
            accepted: LogIndex(2),
            submitted: LogIndex(2),
            durable: LogIndex(1),
            committed: LogIndex(1)
        }
    );
    let ack = Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(response),
    };
    assert_eq!(
        pipeline.step_batch(vec![ack.clone()]),
        Err(PipelineError::PersistencePending)
    );
    assert!(pipeline.ready_node().is_none());
    assert!(!handle.is_finished());
    release_tx.send(()).unwrap();
    assert!(pipeline
        .complete(handle.join().unwrap())
        .unwrap()
        .iter()
        .all(|o| !matches!(o, Output::Apply { .. })));
    assert_eq!(pipeline.progress().durable, LogIndex(2));
    let outputs = pipeline.step_batch(vec![ack]).unwrap();
    assert_eq!(
        outputs
            .iter()
            .filter(|o| matches!(
                o,
                Output::Apply {
                    index: LogIndex(2),
                    ..
                }
            ))
            .count(),
        1
    );
    assert_eq!(pipeline.progress().committed, LogIndex(2));
}

#[test]
fn failed_local_write_never_releases_apply_and_poison_survives_completion() {
    let (mut node, _) = fixture();
    node.log_segment.fail = true;
    let mut pipeline = Pipeline::new(node);
    let (_, work) = prepare(&mut pipeline);
    assert!(matches!(
        pipeline.complete(work.persist()),
        Err(PipelineError::Runtime(RaftRuntimeError::LogAppend(_)))
    ));
    assert_eq!(pipeline.progress().durable, LogIndex(1));
    assert_eq!(pipeline.progress().committed, LogIndex(1));
    assert!(matches!(
        pipeline.step_batch(vec![Input::Tick]),
        Err(PipelineError::Runtime(RaftRuntimeError::Poisoned { .. }))
    ));
}
#[test]
fn another_generation_with_the_same_index_and_sequence_cannot_complete_this_write() {
    let mut first = Pipeline::new(fixture().0);
    let mut second = Pipeline::new(fixture().0);
    let (_, first_work) = prepare(&mut first);
    let (_, second_work) = prepare(&mut second);
    assert_eq!(
        first_work.operation().sequence(),
        second_work.operation().sequence()
    );
    assert!(!first_work
        .operation()
        .same_generation(second_work.operation()));
    assert_eq!(
        first.complete(second_work.persist()),
        Err(PipelineError::UnexpectedCompletion)
    );
    assert!(first.pending_operation().is_some());
    assert_eq!(first.progress().durable, LogIndex(1));
    first.complete(first_work.persist()).unwrap();
    assert_eq!(first.progress().durable, LogIndex(2));
}
#[test]
fn ordinary_follower_and_single_voter_proposals_keep_synchronous_fences() {
    let mut follower = Pipeline::new(node(2));
    assert!(matches!(
        follower.prepare_proposals(proposals()).unwrap(),
        PreparedProposals::Durable(_)
    ));
    let mut single = DurableRaftNode::with_storage(
        NodeConfig::new(NodeId(1), vec![], 1).unwrap(),
        InMemoryRaftHardStateStore::new(),
        ControlledLog::default(),
    )
    .unwrap();
    single.step(Input::Tick).unwrap();
    let mut pipeline = Pipeline::new(single);
    let PreparedProposals::Durable(outputs) = pipeline.prepare_proposals(proposals()).unwrap()
    else {
        panic!("single voter cannot speculate")
    };
    assert!(outputs.iter().any(|o| matches!(
        o,
        Output::Apply {
            index: LogIndex(2),
            ..
        }
    )));
    assert_eq!(pipeline.progress().durable, LogIndex(2));
}
#[test]
fn higher_term_input_is_fenced_after_the_outstanding_generation_completes() {
    let mut pipeline = Pipeline::new(fixture().0);
    let (_, work) = prepare(&mut pipeline);
    let higher = Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(9),
            follower_id: NodeId(2),
            success: false,
            match_index: LogIndex(0),
            sequence: 0,
        }),
    };
    assert_eq!(
        pipeline.step_batch(vec![higher.clone()]),
        Err(PipelineError::PersistencePending)
    );
    pipeline.complete(work.persist()).unwrap();
    pipeline.step_batch(vec![higher]).unwrap();
    assert_eq!(pipeline.ready_node().unwrap().current_term(), Term(9));
    assert_eq!(
        pipeline.ready_node().unwrap().role(),
        rafter::Role::Follower
    );
}
