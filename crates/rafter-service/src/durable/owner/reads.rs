//! Correlate real quorum outcomes before evaluating an application query.

use super::{
    ApplicationEntry, DurableApplication, DurableClientOwner, Input, RaftHardStateStore,
    RaftLogSegment, RaftSnapshotStore, ServiceError, ServiceStepError, SnapshotChunkSource,
};
use crate::{durable::read::Phase, ReadError};
use rafter::Output;
use rafter_app::read::ReadProof;
use rafter_runtime::service::ServiceEvent;

mod query;

impl<H, L, S, T, A, R: Send + 'static> DurableClientOwner<H, L, S, T, A, R>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    pub(super) fn observe_reads(&self, event: &ServiceEvent<T, A>) -> Result<(), ServiceError> {
        let Some(reads) = &self.reads else {
            return Ok(());
        };
        let ServiceEvent::Output(output) = event else {
            return Ok(());
        };
        match output {
            Output::ReadIndexGranted {
                read_id,
                read_index,
            } => {
                let node = self.ready_node().ok_or(ServiceError::OutputOrder)?;
                // Input admission stayed closed while this output waited for
                // application durability. Capture term and translated floor
                // now, before the embedding can admit another consensus input.
                reads.shared.phase(
                    *read_id,
                    Phase::Ready(Some(ReadProof {
                        group_id: (),
                        issued_by: node.id(),
                        term: node.current_term(),
                        read_index: *read_index,
                        required_applied_index: node
                            .committed_application_index_through(*read_index),
                        local_applied_index: rafter::LogIndex::ZERO,
                    })),
                );
            }
            Output::ReadIndexRejected { read_id, reason } => {
                reads.shared.finish(
                    *read_id,
                    Err(ReadError::Rejected {
                        read_id: Some(*read_id),
                        reason: *reason,
                        leader_hint: self
                            .ready_node()
                            .and_then(rafter_runtime::DurableRaftNode::leader_hint),
                    }),
                );
            }
            Output::ReadIndexCanceled { read_id, reason } => {
                reads.shared.finish(
                    *read_id,
                    Err(ReadError::Canceled {
                        read_id: *read_id,
                        reason: *reason,
                        leader_hint: self
                            .ready_node()
                            .and_then(rafter_runtime::DurableRaftNode::leader_hint),
                    }),
                );
            }
            _ => {}
        }
        Ok(())
    }

    pub(super) fn drive_read(&mut self) -> Result<bool, ServiceError> {
        let Some(work) = self.reads.as_ref().and_then(|r| r.shared.next_work()) else {
            return Ok(false);
        };
        self.failed = true;
        let mut fence = self.callback_fence();
        let result = match work.phase {
            Phase::Queued => {
                let reads = self.reads.as_ref().ok_or(ServiceError::Failed)?;
                reads.shared.phase(work.id, Phase::Barrier);
                match self
                    .service
                    .as_mut()
                    .ok_or(ServiceError::Failed)?
                    .try_step(Input::ReadIndex { read_id: work.id }.into())
                {
                    Ok(()) => Ok(true),
                    Err(ServiceStepError::Unavailable { .. }) => {
                        reads.shared.phase(work.id, Phase::Queued);
                        Ok(false)
                    }
                    Err(ServiceStepError::Fatal(error)) => Err(error),
                    Err(_) => Err(ServiceError::OutputOrder),
                }
            }
            Phase::Ready(_) => self.evaluate_query(work),
            Phase::Barrier => Err(ServiceError::OutputOrder),
        };
        if result.is_ok() {
            self.failed = false;
            fence.armed = false;
        }
        result
    }
}
