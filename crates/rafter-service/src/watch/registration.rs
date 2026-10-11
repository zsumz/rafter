//! One registration per pending `changed` future, including cancellation.
use super::{lock_state, MetricsState};
use std::{
    sync::{Arc, Mutex},
    task::Waker,
};

#[derive(Debug, Default)]
pub(super) struct Waiter {
    pub(super) waker: Mutex<Option<Waker>>,
}

#[derive(Debug)]
pub(super) struct Registration<G> {
    shared: Arc<Mutex<MetricsState<G>>>,
    pub(super) waiter: Arc<Waiter>,
}
impl<G> Registration<G> {
    pub(super) fn new(shared: &Arc<Mutex<MetricsState<G>>>) -> Self {
        Self {
            shared: Arc::clone(shared),
            waiter: Arc::new(Waiter::default()),
        }
    }
}
impl<G> Drop for Registration<G> {
    fn drop(&mut self) {
        {
            let mut state = lock_state(&self.shared);
            state
                .waiters
                .retain(|waiter| !Arc::ptr_eq(waiter, &self.waiter));
            // No callback can run from these Arc drops: this registration still
            // owns its waiter. Release a vanished subscription burst's buffer.
            if state.waiters.is_empty() {
                state.waiters.shrink_to_fit();
            }
        }
        let waker = lock_state(&self.waiter.waker).take();
        // A custom task destructor may reenter the publisher or cancel another
        // wait. It must run outside both the registry and the waiter locks.
        drop(waker);
    }
}

pub(super) fn wake_all(waiters: Vec<Arc<Waiter>>) {
    for waiter in waiters {
        let waker = lock_state(&waiter.waker).take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
