//! Fence every client queue together and publish only after backend release.
use super::{ApplicationEntry, Arc, DurableApplication, DurableClientOwner, ReadShared, Shared};
use crate::durable::reply::wake_all;
use std::fmt;

impl<H, L, S, T: ApplicationEntry, A: DurableApplication<T>, R: Send + 'static>
    DurableClientOwner<H, L, S, T, A, R>
{
    pub(super) fn close_clients(&self) {
        self.shared.close();
        if let Some(reads) = &self.reads {
            reads.shared.close();
        }
    }
    pub(super) fn callback_fence(&self) -> CallbackFence<A::Outcome, R> {
        CallbackFence {
            writes: Arc::clone(&self.shared),
            reads: self.reads.as_ref().map(|r| Arc::clone(&r.shared)),
            armed: true,
        }
    }
    pub(super) fn terminate_clients(&self) {
        let mut wakers = self.shared.terminate();
        if let Some(reads) = &self.reads {
            wakers.extend(reads.shared.terminate());
        }
        wake_all(wakers);
    }
}
impl<H, L, S, T: ApplicationEntry, A: DurableApplication<T>, R: Send + 'static> Drop
    for DurableClientOwner<H, L, S, T, A, R>
{
    fn drop(&mut self) {
        self.close_clients();
        let finish = Finish {
            writes: Arc::clone(&self.shared),
            reads: self.reads.as_ref().map(|r| Arc::clone(&r.shared)),
        };
        drop(self.service.take());
        drop(finish);
    }
}
struct Finish<O, R> {
    writes: Arc<Shared<O>>,
    reads: Option<Arc<ReadShared<R>>>,
}
impl<O, R> Drop for Finish<O, R> {
    fn drop(&mut self) {
        let mut wakers = self.writes.terminate();
        if let Some(reads) = &self.reads {
            wakers.extend(reads.terminate());
        }
        wake_all(wakers);
    }
}

pub(super) struct CallbackFence<O, R> {
    writes: Arc<Shared<O>>,
    reads: Option<Arc<ReadShared<R>>>,
    pub(super) armed: bool,
}
impl<O, R> Drop for CallbackFence<O, R> {
    fn drop(&mut self) {
        if self.armed {
            self.writes.close();
            if let Some(reads) = &self.reads {
                reads.close();
            }
        }
    }
}
impl<H, L, S, T: ApplicationEntry, A: DurableApplication<T>, R: Send + 'static> fmt::Debug
    for DurableClientOwner<H, L, S, T, A, R>
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DurableClientOwner")
            .field("retained", &self.shared.stats())
            .field("reads", &self.reads.as_ref().map(|r| r.shared.stats()))
            .finish_non_exhaustive()
    }
}
