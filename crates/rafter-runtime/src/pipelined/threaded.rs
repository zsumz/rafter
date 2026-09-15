//! Bounded composition of a pipelined node and its dedicated persistence worker.

mod types;

pub use types::{
    ThreadedPersistenceCompletion, ThreadedPersistenceDisposition, ThreadedPipelineError,
    ThreadedPipelineStartError, ThreadedProposalOutputs,
};

use super::{
    PersistenceWorker, PersistenceWorkerOptions, PersistenceWorkerShutdownError, PipelineProgress,
    PipelinedRaftNode, PreparedProposals,
};
use rafter::{ClientProposalInput, Input, Output, SnapshotChunkSource};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};
use std::fmt;

/// One durable node and one dedicated, end-to-end one-credit persistence worker.
///
/// This is the reusable composition used by an owner loop. Proposal preparation
/// can return eligible peer sends while the worker owns the node, but every
/// other consensus input is refused until [`Self::complete`] or
/// [`Self::try_complete`] accepts the exact generation-bound completion.
/// Applications must still durably apply `Output::Apply` before acknowledging a
/// client.
///
/// If the worker refuses a newly prepared operation, this type runs that exact
/// persistence work inline and reports
/// [`ThreadedPersistenceDisposition::InlineFallback`]. The durability contract
/// therefore remains intact even after an idle worker has been shut down.
pub struct ThreadedPipelinedRaftNode<H, L, S> {
    node: PipelinedRaftNode<H, L, S>,
    worker: PersistenceWorker<H, L, S>,
}

impl<H, L, S> fmt::Debug for ThreadedPipelinedRaftNode<H, L, S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ThreadedPipelinedRaftNode")
            .field("worker", &self.worker)
            .finish_non_exhaustive()
    }
}

impl<H, L, S> ThreadedPipelinedRaftNode<H, L, S>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
{
    /// Starts a dedicated persistence worker around an already recovered node.
    ///
    /// # Errors
    ///
    /// Returns the operating-system thread creation error without consuming the
    /// node when the worker cannot be started.
    pub fn start(
        node: crate::DurableRaftNode<H, L, S>,
        options: PersistenceWorkerOptions,
    ) -> Result<Self, ThreadedPipelineStartError<H, L, S>> {
        let worker = match PersistenceWorker::start(options) {
            Ok(worker) => worker,
            Err(source) => {
                return Err(ThreadedPipelineStartError {
                    source,
                    node: Box::new(node),
                });
            }
        };
        Ok(Self {
            node: PipelinedRaftNode::new(node),
            worker,
        })
    }

    /// Returns the node only when no persistence operation owns it.
    #[must_use]
    pub fn ready_node(&self) -> Option<&crate::DurableRaftNode<H, L, S>> {
        self.node.ready_node()
    }

    /// Returns mutable node access only while no persistence operation owns it.
    #[must_use]
    pub fn ready_node_mut(&mut self) -> Option<&mut crate::DurableRaftNode<H, L, S>> {
        self.node.ready_node_mut()
    }

    /// Returns accepted, submitted, durable, and durably committed positions.
    #[must_use]
    pub const fn progress(&self) -> PipelineProgress {
        self.node.progress()
    }

    /// Returns whether the worker currently owns the node.
    #[must_use]
    pub fn persistence_pending(&self) -> bool {
        self.node.pending_operation().is_some()
    }

    /// Drives inputs through the synchronous fence.
    ///
    /// # Errors
    ///
    /// Refuses all input while persistence is pending, and propagates fatal
    /// runtime failures.
    pub fn step_batch(&mut self, inputs: Vec<Input>) -> Result<Vec<Output>, ThreadedPipelineError> {
        self.node.step_batch(inputs).map_err(Into::into)
    }

    /// Prepares proposals and submits eligible persistence work to the worker.
    ///
    /// The caller may execute every returned output immediately. A `Pending`
    /// disposition contains only dependency-safe replication sends; dependent
    /// outputs remain fenced until completion.
    ///
    /// # Errors
    ///
    /// Refuses while another operation owns the node, on identity exhaustion,
    /// or when the synchronous or inline durability fence fails.
    pub fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<ThreadedProposalOutputs, ThreadedPipelineError> {
        match self.node.prepare_proposals(proposals)? {
            PreparedProposals::Durable(outputs) => Ok(ThreadedProposalOutputs {
                outputs,
                persistence: ThreadedPersistenceDisposition::Durable,
            }),
            PreparedProposals::Pending { replication, work } => {
                match self.worker.try_submit(work) {
                    Ok(()) => Ok(ThreadedProposalOutputs {
                        outputs: replication,
                        persistence: ThreadedPersistenceDisposition::Pending,
                    }),
                    Err(error) => {
                        let work = match error {
                            std::sync::mpsc::TrySendError::Full(work)
                            | std::sync::mpsc::TrySendError::Disconnected(work) => work,
                        };
                        let completion = work.persist();
                        let mut outputs = replication;
                        outputs.extend(self.node.complete(completion)?);
                        Ok(ThreadedProposalOutputs {
                            outputs,
                            persistence: ThreadedPersistenceDisposition::InlineFallback,
                        })
                    }
                }
            }
        }
    }

    /// Waits for and accepts the outstanding worker completion.
    ///
    /// Returns `Ok(None)` when no persistence operation is pending.
    ///
    /// # Errors
    ///
    /// Returns [`ThreadedPipelineError::WorkerStopped`] if the worker ended
    /// without returning the node, or the runtime failure retained by a
    /// completed operation.
    pub fn complete(
        &mut self,
    ) -> Result<Option<ThreadedPersistenceCompletion>, ThreadedPipelineError> {
        if !self.persistence_pending() {
            return Ok(None);
        }
        let (completion, storage_telemetry) = self.worker.complete()?.into_parts();
        let outputs = self.node.complete(completion)?;
        Ok(Some(ThreadedPersistenceCompletion {
            outputs,
            storage_telemetry,
        }))
    }

    /// Polls once for the outstanding worker completion.
    ///
    /// Returns `Ok(None)` both when no operation is pending and while the worker
    /// is still executing it. Use [`Self::persistence_pending`] to distinguish
    /// those states.
    ///
    /// # Errors
    ///
    /// Returns [`ThreadedPipelineError::WorkerStopped`] if the worker ended
    /// without returning the node, or the runtime failure retained by a
    /// completed operation.
    pub fn try_complete(
        &mut self,
    ) -> Result<Option<ThreadedPersistenceCompletion>, ThreadedPipelineError> {
        if !self.persistence_pending() {
            return Ok(None);
        }
        let Some(completed) = self.worker.try_complete()? else {
            return Ok(None);
        };
        let (completion, storage_telemetry) = completed.into_parts();
        let outputs = self.node.complete(completion)?;
        Ok(Some(ThreadedPersistenceCompletion {
            outputs,
            storage_telemetry,
        }))
    }

    /// Stops and joins an idle persistence worker.
    ///
    /// After successful shutdown, proposal work safely uses the documented
    /// inline fallback. This permits a service to quiesce its helper thread
    /// without changing the durability contract.
    ///
    /// # Errors
    ///
    /// Returns `Busy` while the worker owns an operation, or `Panicked` when
    /// joining the thread observes a panic.
    pub fn shutdown_worker(&mut self) -> Result<(), PersistenceWorkerShutdownError> {
        self.worker.shutdown()
    }
}
