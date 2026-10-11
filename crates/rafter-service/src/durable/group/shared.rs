//! Client endpoints and one retained transfer request; never a backend owner.
use super::{lock, DurableReadSender, DurableWriteSender};
use crate::{
    durable::{budget::Budget, reply::Reply},
    MetricsPublisher, TransferLeadershipError,
};
use rafter::{LeadershipTransferRejection, NodeId};
use std::sync::{Arc, Mutex};

pub(super) struct Shared<G, O, R> {
    pub group: G,
    pub writes: DurableWriteSender<O>,
    pub reads: Option<DurableReadSender<R>>,
    pub metrics: MetricsPublisher<G>,
    pub state: Mutex<State>,
    pub budget: Arc<Budget>,
    pub wake: Arc<dyn Fn() + Send + Sync>,
}
#[derive(Default)]
pub(super) struct State {
    pub closed: bool,
    pub shutdown: bool,
    pub stopped: bool,
    pub transfer: Option<Transfer>,
}
pub(super) struct Transfer {
    pub target: NodeId,
    pub submitted: bool,
    pub rejection: Option<LeadershipTransferRejection>,
    pub reply: Arc<Reply<Result<(), TransferLeadershipError>>>,
}
impl<G, O, R> Shared<G, O, R> {
    pub(super) fn close(&self) {
        lock(&self.state).closed = true;
        self.writes.shared.close();
        if let Some(reads) = &self.reads {
            reads.shared.close();
        }
    }
    pub(super) fn finish(&self) {
        self.close();
        let transfer = {
            let mut state = lock(&self.state);
            state.stopped = true;
            state.transfer.take()
        };
        // Publish all terminal states before either subscriber class runs.
        let waker = transfer.and_then(|t| {
            t.reply.complete(Err(if t.submitted {
                TransferLeadershipError::Unconfirmed
            } else {
                TransferLeadershipError::ShuttingDown
            }))
        });
        let closed =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.metrics.close()));
        let notified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if let Some(waker) = waker {
                waker.wake();
            }
        }));
        if let Err(error) = closed.and(notified) {
            std::panic::resume_unwind(error);
        }
    }
}
