//! Query one immutable durable view without holding the admission mutex.

use super::{
    ApplicationEntry, DurableApplication, DurableClientOwner, Phase, RaftHardStateStore,
    RaftLogSegment, RaftSnapshotStore, ReadError, ServiceError, SnapshotChunkSource,
};
use crate::{durable::read::Work, DriverUnavailableReason, QueryReceipt};
use rafter::LogIndex;
use rafter_runtime::{application::ApplicationReadRejection, service::ServiceReadRejection};

impl<H, L, S, T, A, R: Send + 'static> DurableClientOwner<H, L, S, T, A, R>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    pub(super) fn evaluate_query(&mut self, work: Work) -> Result<bool, ServiceError> {
        let reads = self.reads.as_ref().ok_or(ServiceError::Failed)?;
        let node = self.ready_node().ok_or(ServiceError::OutputOrder)?;
        if !node.committed_membership().contains_replica(node.id())
            && !node.effective_membership().contains_replica(node.id())
        {
            drop(work.payload);
            reads.shared.finish(
                work.id,
                Err(ReadError::Unavailable {
                    reason: DriverUnavailableReason::NotMember,
                }),
            );
            return Ok(true);
        }
        let Phase::Ready(mut proof) = work.phase else {
            return Err(ServiceError::OutputOrder);
        };
        let required = work
            .options
            .min_applied_index
            .unwrap_or(LogIndex::ZERO)
            .max(
                proof
                    .as_ref()
                    .map_or(LogIndex::ZERO, |p| p.required_applied_index),
            );
        let service = self.service.as_mut().ok_or(ServiceError::Failed)?;
        let outcome =
            match service.try_read_application(required, |app| (reads.query)(app, &work.payload)) {
                Ok(view) => {
                    if let Some(proof) = &mut proof {
                        proof.required_applied_index = required;
                        proof.local_applied_index = view.durable_through();
                    }
                    Ok(QueryReceipt {
                        result: view.into_output(),
                        proof,
                    })
                }
                Err(error) => {
                    let rejection = error.rejection();
                    match rejection {
                        ServiceReadRejection::Application(ApplicationReadRejection::Busy) => {
                            return Ok(false)
                        }
                        ServiceReadRejection::Application(ApplicationReadRejection::Behind {
                            durable_through,
                            ..
                        }) => Err(ReadError::FreshnessUnavailable {
                            read_id: proof.as_ref().map(|_| work.id),
                            required_applied_index: required,
                            local_applied_index: durable_through,
                        }),
                        ServiceReadRejection::Application(ApplicationReadRejection::Stopped) => {
                            return Err(ServiceError::ApplicationStopped)
                        }
                        _ => return Err(ServiceError::Failed),
                    }
                }
            };
        // Neither the extra Arc nor the queue's payload may outlive publication
        // and collection of the result that holds their byte credit.
        drop(work.payload);
        reads.shared.finish(work.id, outcome);
        Ok(true)
    }
}
