//! One result, retaining admission credit until every owner has released it.

use super::{lock, shared::Credit};
use crate::{DriverFuture, WriteError, WriteReceipt};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll, Waker},
};

type Outcome<O> = Result<WriteReceipt<O>, WriteError>;

pub(super) struct Reply<O> {
    state: Mutex<State<O>>,
    pub canceled: AtomicBool,
    wake: Arc<dyn Fn() + Send + Sync>,
    // Drop the outcome and its waker before releasing admission credit.
    _credit: Credit,
}

struct State<O> {
    outcome: Option<Outcome<O>>,
    waker: Option<Waker>,
}

impl<O> Reply<O> {
    pub(super) fn new(credit: Credit, wake: Arc<dyn Fn() + Send + Sync>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                outcome: None,
                waker: None,
            }),
            canceled: AtomicBool::new(false),
            wake,
            _credit: credit,
        })
    }

    pub(super) fn finish(&self, outcome: Outcome<O>) {
        if let Some(waker) = self.complete(outcome) {
            waker.wake();
        }
    }

    pub(super) fn complete(&self, outcome: Outcome<O>) -> Option<Waker> {
        let mut outcome = Some(outcome);
        let waker = {
            let mut state = lock(&self.state);
            if !self.canceled.load(Ordering::Acquire) {
                debug_assert!(state.outcome.is_none());
                state.outcome = outcome.take();
            }
            state.waker.take()
        };
        drop(outcome);
        waker
    }
}

impl<O: Send + 'static> Reply<O> {
    pub(super) fn future(self: &Arc<Self>) -> DriverFuture<Outcome<O>> {
        Box::pin(ReplyFuture {
            reply: Some(Arc::clone(self)),
        })
    }
}

struct ReplyFuture<O> {
    reply: Option<Arc<Reply<O>>>,
}

impl<O> Future for ReplyFuture<O> {
    type Output = Outcome<O>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let Some(reply) = this.reply.as_ref() else {
            return Poll::Pending;
        };
        let replacement = cx.waker().clone();
        let (outcome, old) = {
            let mut state = lock(&reply.state);
            let outcome = state.outcome.take();
            let old = state.waker.replace(replacement);
            (outcome, old)
        };
        drop(old);
        if let Some(outcome) = outcome {
            this.reply.take();
            Poll::Ready(outcome)
        } else {
            Poll::Pending
        }
    }
}

impl<O> Drop for ReplyFuture<O> {
    fn drop(&mut self) {
        if let Some(reply) = self.reply.take() {
            reply.canceled.store(true, Ordering::Release);
            let old = lock(&reply.state).waker.take();
            drop(old);
            (reply.wake)();
        }
    }
}
