//! Credits cover owner-held requests and completed, uncollected results.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Default)]
pub(super) struct Budget {
    count: AtomicUsize,
    bytes: AtomicUsize,
}

impl Budget {
    // Each queue serializes admission; concurrent releases only lower usage.
    pub(super) fn reserve(
        self: &Arc<Self>,
        bytes: usize,
        max_count: usize,
        max_bytes: usize,
    ) -> Option<Credit> {
        let (count, retained) = self.stats();
        if count >= max_count || retained.checked_add(bytes).is_none_or(|n| n > max_bytes) {
            return None;
        }
        self.count.fetch_add(1, Ordering::AcqRel);
        self.bytes.fetch_add(bytes, Ordering::AcqRel);
        Some(Credit {
            budget: Arc::clone(self),
            bytes,
        })
    }

    pub(super) fn stats(&self) -> (usize, usize) {
        (
            self.count.load(Ordering::Acquire),
            self.bytes.load(Ordering::Acquire),
        )
    }
}

pub(super) struct Credit {
    budget: Arc<Budget>,
    bytes: usize,
}
impl Drop for Credit {
    fn drop(&mut self) {
        self.budget.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.budget.count.fetch_sub(1, Ordering::AcqRel);
    }
}
