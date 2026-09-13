//! Safe ownership composition for one pipelined durable node and worker.

use rafter::{ClientProposalInput, Input, Output};
use rafter_runtime::pipelined::{
    PersistenceWorkerOptions, ThreadedPersistenceDisposition, ThreadedPipelinedRaftNode,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::durable_batch::{WalRaftHardStateStore, WalRaftLogSegment};
use rafter_storage::FileRaftSnapshotStore;

pub(crate) type DurableNode =
    DurableRaftNode<WalRaftHardStateStore, WalRaftLogSegment, FileRaftSnapshotStore>;
type Pipeline =
    ThreadedPipelinedRaftNode<WalRaftHardStateStore, WalRaftLogSegment, FileRaftSnapshotStore>;

#[derive(Debug)]
pub(crate) struct PipelinedNode {
    node: Pipeline,
    submitted: u64,
    synchronous_fallbacks: u64,
}

impl PipelinedNode {
    pub(crate) fn start(node: DurableNode) -> Self {
        Self {
            node: Pipeline::start(node, PersistenceWorkerOptions::new())
                .map_err(|error| error.into_parts().0)
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
        let result = self
            .node
            .step_proposal_batch(vec![proposal])
            .expect("proposal preparation succeeds");
        match result.persistence() {
            ThreadedPersistenceDisposition::Pending => self.submitted += 1,
            ThreadedPersistenceDisposition::InlineFallback => self.synchronous_fallbacks += 1,
            ThreadedPersistenceDisposition::Durable => {}
            _ => panic!("unsupported persistence disposition"),
        }
        result.into_outputs()
    }

    pub(crate) fn complete_pending(&mut self) -> Vec<Output> {
        if !self.node.persistence_pending() {
            return Vec::new();
        }
        self.node
            .complete()
            .expect("persistence worker returns the outstanding node")
            .expect("pending pipeline returns one completion")
            .into_outputs()
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
        self.node
            .shutdown_worker()
            .expect("idle persistence worker shuts down");
        (self.submitted, self.synchronous_fallbacks, outputs)
    }
}
