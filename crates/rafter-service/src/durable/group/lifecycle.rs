//! Shutdown observation follows backend disposal, including panicking tasks.
use super::{shared::Shared, ApplicationEntry, Arc, DurableApplication, DurableRaftDriver};
use std::panic::{catch_unwind, AssertUnwindSafe};

pub(super) struct Fence<G, O, R> {
    shared: Arc<Shared<G, O, R>>,
    pub armed: bool,
}
impl<G, O, R> Fence<G, O, R> {
    pub(super) fn new(shared: &Arc<Shared<G, O, R>>) -> Self {
        Self {
            shared: Arc::clone(shared),
            armed: true,
        }
    }
}
impl<G, O, R> Drop for Fence<G, O, R> {
    fn drop(&mut self) {
        if self.armed {
            self.shared.close();
        }
    }
}
// Run each disposal/notification outside another callback's unwinding. A guard
// destructor calling another panicking waker would abort on the second panic.
pub(super) fn release<G, O, R, B, A>(shared: &Shared<G, O, R>, backend: B, application: A) {
    let app = catch_unwind(AssertUnwindSafe(|| drop(application)));
    let owner = catch_unwind(AssertUnwindSafe(|| drop(backend)));
    let finish = catch_unwind(AssertUnwindSafe(|| shared.finish()));
    if let Err(error) = app.and(owner).and(finish) {
        std::panic::resume_unwind(error);
    }
}
impl<G, H, L, S, T: ApplicationEntry, A: DurableApplication<T>, Tr, R: Send + 'static> Drop
    for DurableRaftDriver<G, H, L, S, T, A, Tr, R>
{
    fn drop(&mut self) {
        self.shared.close();
        release(&self.shared, self.owner.take(), ());
    }
}
