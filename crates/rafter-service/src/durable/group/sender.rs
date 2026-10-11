//! Group validation happens before admission to the durable queues.
use super::{
    lock,
    shared::{Shared, Transfer},
};
use crate::{
    DriverCommandSender, DriverFuture, MetricsError, MetricsWatch, QueryReceipt, ReadError,
    ReadOptions, ShutdownError, TransferLeadershipError, WriteError, WriteOptions, WriteReceipt,
};
use rafter::NodeId;
use rafter_app::read::{ReadConsistency, ReadProof};
use std::sync::Arc;

/// Cloneable managed endpoint; it owns no worker, storage, or application.
///
/// Commands and queries are independently encoded byte vectors. The original
/// client's count/capacity limits and durable completion semantics are preserved.
/// At most one transfer remains retained, including its uncollected result.
pub struct DurableCommandSender<G, O, R> {
    pub(super) shared: Arc<Shared<G, O, R>>,
}
impl<G, O, R> Clone for DurableCommandSender<G, O, R> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}
impl<G, O, R> std::fmt::Debug for DurableCommandSender<G, O, R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableCommandSender")
            .finish_non_exhaustive()
    }
}
impl<G, O, R> DriverCommandSender<G, Vec<u8>, Vec<u8>, O, R> for DurableCommandSender<G, O, R>
where
    G: Clone + Eq + Send + Sync + 'static,
    O: Send + 'static,
    R: Send + 'static,
{
    fn write(
        &self,
        group: G,
        command: Vec<u8>,
        options: WriteOptions,
    ) -> DriverFuture<Result<WriteReceipt<O>, WriteError>> {
        if group != self.shared.group {
            return Box::pin(std::future::ready(Err(WriteError::WrongGroup)));
        }
        self.shared.writes.write(command, options)
    }
    fn read(
        &self,
        group: G,
        query: Vec<u8>,
        consistency: ReadConsistency,
        options: ReadOptions,
    ) -> DriverFuture<Result<QueryReceipt<G, R>, ReadError>> {
        if group != self.shared.group {
            return Box::pin(std::future::ready(Err(ReadError::WrongGroup)));
        }
        if lock(&self.shared.state).closed {
            return Box::pin(std::future::ready(Err(ReadError::ShuttingDown)));
        }
        let Some(reads) = &self.shared.reads else {
            return Box::pin(std::future::ready(Err(ReadError::UnsupportedConsistency {
                consistency,
            })));
        };
        let result = reads.read(query, consistency, options);
        Box::pin(async move {
            let receipt = result.await?;
            Ok(QueryReceipt {
                result: receipt.result,
                proof: receipt.proof.map(|p| ReadProof {
                    group_id: group,
                    issued_by: p.issued_by,
                    term: p.term,
                    read_index: p.read_index,
                    required_applied_index: p.required_applied_index,
                    local_applied_index: p.local_applied_index,
                }),
            })
        })
    }
    fn transfer_leadership(
        &self,
        group: G,
        target: NodeId,
    ) -> DriverFuture<Result<(), TransferLeadershipError>> {
        if group != self.shared.group {
            return Box::pin(std::future::ready(Err(TransferLeadershipError::WrongGroup)));
        }
        let reply = {
            let mut state = lock(&self.shared.state);
            let error = if state.closed {
                Some(TransferLeadershipError::ShuttingDown)
            } else {
                None
            };
            if let Some(error) = error {
                return Box::pin(std::future::ready(Err(error)));
            }
            let Some(credit) = self.shared.budget.reserve(0, 1, 0) else {
                return Box::pin(std::future::ready(Err(
                    TransferLeadershipError::Backpressure,
                )));
            };
            let reply = crate::durable::reply::Reply::new(credit, Arc::clone(&self.shared.wake));
            state.transfer = Some(Transfer {
                target,
                submitted: false,
                rejection: None,
                reply: Arc::clone(&reply),
            });
            reply
        };
        let future = reply.future();
        (self.shared.wake)();
        future
    }
    fn metrics(&self, group: G) -> Result<MetricsWatch<G>, MetricsError> {
        if group != self.shared.group {
            return Err(MetricsError::WrongGroup);
        }
        Ok(self.shared.metrics.watch())
    }
    fn shutdown(&self, group: G) -> DriverFuture<Result<(), ShutdownError>> {
        if group != self.shared.group {
            return Box::pin(std::future::ready(Err(ShutdownError::WrongGroup)));
        }
        let first = {
            let mut state = lock(&self.shared.state);
            let first = !state.shutdown && !state.stopped;
            state.shutdown = true;
            state.closed = true;
            first
        };
        self.shared.close();
        let mut watch = self.shared.metrics.watch();
        (self.shared.wake)();
        // Duplicate requests also wait for actual backend release. Dropping
        // this future only unregisters its wakeup; shutdown remains requested.
        Box::pin(async move {
            while watch.changed().await.is_some() {}
            if first {
                Ok(())
            } else {
                Err(ShutdownError::AlreadyShutDown)
            }
        })
    }
}
