//! Admission without a hidden input mailbox.

use super::driver::Terminal;
use super::{DurableService, ServiceError, ServiceInput, ServiceState, ServiceStepError};
use crate::application::{ApplicationEntry, DurableApplication};
use crate::pipelined::ThreadedProposalOutputs;
use rafter::{Input, SnapshotChunkSource};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};

impl<H, L, S, T, A> DurableService<H, L, S, T, A>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    /// Admits one input when previous outputs have been dispatched.
    ///
    /// This never waits for application credits or an outstanding persistence
    /// completion. Ordinary consensus inputs and ineligible proposal batches
    /// may perform synchronous storage I/O. Successful admission is not a
    /// client acknowledgement: consume `Applied` events for that boundary.
    ///
    /// # Errors
    /// Returns the unchanged input when busy, closed, or beyond proposal limits.
    /// A fatal error consumes the input and fences the service; its durable
    /// disposition must be recovered before retrying any write.
    pub fn try_step(&mut self, input: ServiceInput) -> Result<(), ServiceStepError> {
        let state = self.state();
        if state != ServiceState::Ready {
            return Err(ServiceStepError::Unavailable { state, input });
        }
        if !self.within_proposal_limits(&input) {
            return Err(ServiceStepError::ProposalLimit { input });
        }
        // A caught storage panic must not allow reuse of partially stepped state.
        self.terminal = Some(Terminal::Failed);
        let result = match input {
            ServiceInput::Consensus(input) => self.node.step_batch(vec![*input]),
            ServiceInput::Proposals(proposals) => self
                .node
                .step_proposal_batch(proposals)
                .map(ThreadedProposalOutputs::into_outputs),
        };
        match result {
            Ok(outputs) => {
                self.terminal = None;
                self.outputs = outputs.into();
                Ok(())
            }
            Err(error) => {
                self.terminal = Some(Terminal::Failed);
                Err(ServiceStepError::Fatal(ServiceError::Persistence(error)))
            }
        }
    }

    fn within_proposal_limits(&self, input: &ServiceInput) -> bool {
        match input {
            ServiceInput::Proposals(proposals) => {
                !proposals.is_empty()
                    && proposals.len() <= self.options.max_proposals
                    && proposals
                        .iter()
                        .try_fold(0_usize, |sum, p| sum.checked_add(p.payload.len()))
                        .is_some_and(|bytes| bytes <= self.options.max_proposal_bytes)
            }
            ServiceInput::Consensus(input) => match input.as_ref() {
                Input::ClientProposal { payload }
                | Input::TrackedClientProposal { payload, .. } => {
                    payload.len() <= self.options.max_proposal_bytes
                }
                _ => true,
            },
        }
    }
}
