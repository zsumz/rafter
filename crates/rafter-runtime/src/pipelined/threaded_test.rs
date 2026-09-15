//! Ownership, fallback, and shutdown checks for the threaded composition.

use super::{tests::*, *};
use rafter::{Input, LogIndex, Message, NodeId, Output};
use std::time::{Duration, Instant};

fn append_to(outputs: &[Output], node_id: u64) -> rafter::AppendEntries {
    outputs
        .iter()
        .find_map(|output| match output {
            Output::Send {
                to,
                message: Message::AppendEntries(request),
            } if *to == NodeId(node_id) => Some(request.clone()),
            _ => None,
        })
        .expect("expected append")
}

#[test]
fn threaded_composition_fences_consensus_until_exact_completion() {
    let (leader, mut follower) = fixture();
    let mut pipeline = ThreadedPipelinedRaftNode::start(
        leader,
        PersistenceWorkerOptions::new().with_storage_telemetry(true),
    )
    .map_err(|error| error.into_parts().0)
    .unwrap();

    let prepared = pipeline.step_proposal_batch(proposals()).unwrap();
    assert_eq!(
        prepared.persistence(),
        ThreadedPersistenceDisposition::Pending
    );
    assert!(prepared
        .outputs()
        .iter()
        .all(|output| matches!(output, Output::Send { .. })));
    assert!(pipeline.ready_node().is_none());
    assert_eq!(
        pipeline.step_batch(vec![Input::Tick]),
        Err(ThreadedPipelineError::Pipeline(
            PipelineError::PersistencePending
        ))
    );

    let response = follower
        .step(Input::Message {
            from: NodeId(1),
            message: Message::AppendEntries(append_to(prepared.outputs(), 2)),
        })
        .unwrap()
        .into_iter()
        .find_map(|output| match output {
            Output::Send {
                to: NodeId(1),
                message,
            } => Some(message),
            _ => None,
        })
        .unwrap();
    let completed = pipeline.complete().unwrap().unwrap();
    assert!(completed
        .outputs()
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));
    let _worker_telemetry = completed.storage_telemetry();
    assert_eq!(pipeline.progress().durable, LogIndex(2));

    let outputs = pipeline
        .step_batch(vec![Input::Message {
            from: NodeId(2),
            message: response,
        }])
        .unwrap();
    assert!(outputs
        .iter()
        .any(|output| matches!(output, Output::Apply { index, .. } if *index == LogIndex(2))));
}

#[test]
fn stopped_idle_worker_falls_back_to_the_same_inline_fence() {
    let (leader, _) = fixture();
    let mut pipeline = ThreadedPipelinedRaftNode::start(leader, PersistenceWorkerOptions::new())
        .map_err(|error| error.into_parts().0)
        .unwrap();
    pipeline.shutdown_worker().unwrap();

    let result = pipeline.step_proposal_batch(proposals()).unwrap();
    assert_eq!(
        result.persistence(),
        ThreadedPersistenceDisposition::InlineFallback
    );
    assert!(result
        .outputs()
        .iter()
        .any(|output| matches!(output, Output::Send { .. })));
    assert!(pipeline.ready_node().is_some());
    assert!(!pipeline.persistence_pending());
    assert_eq!(pipeline.progress().durable, LogIndex(2));
}

#[test]
fn busy_worker_refuses_shutdown_and_nonblocking_completion_preserves_credit() {
    let (mut leader, _) = fixture();
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    leader.log_segment.delay = Some((started_tx, release_rx));
    let mut pipeline = ThreadedPipelinedRaftNode::start(leader, PersistenceWorkerOptions::new())
        .map_err(|error| error.into_parts().0)
        .unwrap();
    assert_eq!(
        pipeline
            .step_proposal_batch(proposals())
            .unwrap()
            .persistence(),
        ThreadedPersistenceDisposition::Pending
    );
    started_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    assert_eq!(
        pipeline.shutdown_worker(),
        Err(PersistenceWorkerShutdownError::Busy)
    );
    assert!(pipeline.try_complete().unwrap().is_none());
    assert!(pipeline.persistence_pending());

    release_tx.send(()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    while pipeline.try_complete().unwrap().is_none() {
        assert!(Instant::now() < deadline, "worker completion timed out");
        std::thread::yield_now();
    }
    assert!(!pipeline.persistence_pending());
    pipeline.shutdown_worker().unwrap();
}

#[test]
fn polling_observes_worker_panic_and_keeps_the_originating_node_fenced() {
    let (mut leader, _) = fixture();
    leader.log_segment.panic_on_append = true;
    let mut pipeline = ThreadedPipelinedRaftNode::start(leader, PersistenceWorkerOptions::new())
        .map_err(|error| error.into_parts().0)
        .unwrap();
    assert_eq!(
        pipeline
            .step_proposal_batch(proposals())
            .unwrap()
            .persistence(),
        ThreadedPersistenceDisposition::Pending
    );

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        match pipeline.try_complete() {
            Err(ThreadedPipelineError::WorkerStopped(_)) => break,
            Ok(None) => {
                assert!(Instant::now() < deadline, "worker termination timed out");
                std::thread::yield_now();
            }
            other => panic!("unexpected persistence result: {other:?}"),
        }
    }

    assert!(pipeline.persistence_pending());
    assert!(pipeline.ready_node().is_none());
    assert_eq!(
        pipeline.step_batch(vec![Input::Tick]),
        Err(ThreadedPipelineError::Pipeline(
            PipelineError::PersistencePending
        ))
    );
    assert_eq!(
        pipeline.shutdown_worker(),
        Err(PersistenceWorkerShutdownError::Panicked)
    );
    assert!(pipeline.persistence_pending());
}
