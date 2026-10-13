//! A completed result is counted until collection or final abandonment.
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

pub(super) struct Counted(Arc<AtomicUsize>);
impl Counted {
    pub(super) fn new(counter: Arc<AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self(counter)
    }
}
impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}
