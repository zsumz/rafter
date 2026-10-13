//! Client endpoints and one retained control request; never a backend owner.
use super::{
    lock, membership, DurableMembershipError, DurableMembershipLimits, DurableReadSender,
    DurableWriteSender,
};
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
    pub membership_limits: Option<DurableMembershipLimits>,
    pub wake: Arc<dyn Fn() + Send + Sync>,
}
#[derive(Default)]
pub(super) struct State {
    pub closed: bool,
    pub shutdown: bool,
    pub stopped: bool,
    pub transfer: Option<Transfer>,
    pub membership: Option<membership::Pending>,
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
        let (transfer, membership) = {
            let mut state = lock(&self.state);
            state.stopped = true;
            (state.transfer.take(), state.membership.take())
        };
        // Publish all terminal states before either subscriber class runs.
        let transfer_waker = transfer.and_then(|t| {
            t.reply.complete(Err(if t.submitted {
                TransferLeadershipError::Unconfirmed
            } else {
                TransferLeadershipError::ShuttingDown
            }))
        });
        let membership_waker = membership.and_then(|m| {
            m.reply.complete(Err(if m.submitted {
                DurableMembershipError::Unconfirmed
            } else {
                DurableMembershipError::ShuttingDown
            }))
        });
        let closed =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.metrics.close()));
        let notified = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            crate::durable::reply::wake_all(
                transfer_waker.into_iter().chain(membership_waker).collect(),
            );
        }));
        if let Err(error) = closed.and(notified) {
            std::panic::resume_unwind(error);
        }
    }
}
