//! Safe ownership composition for one pipelined durable node and worker.

use rafter::{ClientProposalInput, Input, Output};
use rafter_runtime::pipelined::{
    PersistenceWorker, PersistenceWorkerOptions, PipelinedRaftNode, PreparedProposals,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::durable_batch::{WalRaftHardStateStore, WalRaftLogSegment};
use rafter_storage::FileRaftSnapshotStore;
use std::sync::mpsc::TrySendError;

pub(crate) type DurableNode =
    DurableRaftNode<WalRaftHardStateStore, WalRaftLogSegment, FileRaftSnapshotStore>;
type Pipeline = PipelinedRaftNode<WalRaftHardStateStore, WalRaftLogSegment, FileRaftSnapshotStore>;
type Worker = PersistenceWorker<WalRaftHardStateStore, WalRaftLogSegment, FileRaftSnapshotStore>;

#[derive(Debug)]
pub(crate) struct PipelinedNode {
    node: Pipeline,
    worker: Worker,
    submitted: u64,
    synchronous_fallbacks: u64,
}

impl PipelinedNode {
    pub(crate) fn start(node: DurableNode) -> Self {
        Self {
            node: Pipeline::new(node),
            worker: Worker::start(PersistenceWorkerOptions::new())
                .expect("start one-credit persistence worker"),
            submitted: 0,
            synchronous_fallbacks: 0,
        }
    }

    pub(crate) fn step(&mut self, input: Input) -> Vec<Output> {
        let mut outputs = self.complete_pending();
        let mut stepped = match input {
            Input::ClientProposal { payload } => self.prepare(ClientProposalInput {
                proposal_id: None,
                payload,
            }),
            Input::TrackedClientProposal {
                proposal_id,
                payload,
            } => self.prepare(ClientProposalInput {
                proposal_id: Some(proposal_id),
                payload,
            }),
            other => self
                .node
                .step_batch(vec![other])
                .expect("synchronous durable step succeeds"),
        };
        outputs.append(&mut stepped);
        outputs
    }

    fn prepare(&mut self, proposal: ClientProposalInput) -> Vec<Output> {
        match self
            .node
            .prepare_proposals(vec![proposal])
            .expect("proposal preparation succeeds")
        {
            PreparedProposals::Durable(outputs) => outputs,
            PreparedProposals::Pending {
                mut replication,
                work,
            } => match self.worker.try_submit(work) {
                Ok(()) => {
                    self.submitted += 1;
                    replication
                }
                Err(error) => {
                    // The exact owned work comes back. Persisting it here turns
                    // an executor failure into a visible performance fallback,
                    // never a lost node or a weakened output fence.
                    self.synchronous_fallbacks += 1;
                    let work = match error {
                        TrySendError::Full(work) | TrySendError::Disconnected(work) => work,
                    };
                    let completion = work.persist();
                    let mut deferred = self
                        .node
                        .complete(completion)
                        .expect("synchronous fallback restores the node");
                    replication.append(&mut deferred);
                    replication
                }
            },
            _ => panic!("unsupported proposal preparation result"),
        }
    }

    pub(crate) fn complete_pending(&mut self) -> Vec<Output> {
        if self.node.pending_operation().is_none() {
            return Vec::new();
        }
        let completion = self
            .worker
            .complete()
            .expect("persistence worker returns the outstanding node")
            .into_parts()
            .0;
        self.node
            .complete(completion)
            .expect("originating pipeline accepts its completion")
    }

    pub(crate) fn ready(&self) -> &DurableNode {
        self.node
            .ready_node()
            .expect("maintenance runs only after persistence completion")
    }

    pub(crate) fn ready_mut(&mut self) -> &mut DurableNode {
        self.node
            .ready_node_mut()
            .expect("maintenance runs only after persistence completion")
    }

    pub(crate) fn finish(mut self) -> (u64, u64, Vec<Output>) {
        let outputs = self.complete_pending();
        self.worker
            .shutdown()
            .expect("idle persistence worker shuts down");
        (self.submitted, self.synchronous_fallbacks, outputs)
    }
}
