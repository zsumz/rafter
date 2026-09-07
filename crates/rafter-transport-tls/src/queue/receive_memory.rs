//! Runtime-wide memory accounting for inbound read, decode, and queue ownership.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::ReceiveMemoryLimits;

#[derive(Clone, Debug)]
pub(crate) struct ReceiveMemoryBudget {
    inner: Arc<BudgetInner>,
}

#[derive(Debug)]
struct BudgetInner {
    limits: ReceiveMemoryLimits,
    decoded_group_bytes: usize,
    used: AtomicUsize,
}

#[derive(Debug)]
pub(crate) struct ReceiveMemoryPermit {
    inner: Arc<BudgetInner>,
    charged: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ReceiveMemoryFull {
    pub(crate) required: usize,
    pub(crate) maximum: usize,
}

impl ReceiveMemoryBudget {
    pub(crate) fn new(limits: ReceiveMemoryLimits, decoded_group_bytes: usize) -> Self {
        Self {
            inner: Arc::new(BudgetInner {
                limits,
                decoded_group_bytes,
                used: AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) fn try_acquire_frame(
        &self,
        frame_bytes: usize,
    ) -> Result<ReceiveMemoryPermit, ReceiveMemoryFull> {
        let charged = self
            .inner
            .limits
            .charge(frame_bytes, self.inner.decoded_group_bytes);
        self.try_acquire_charge(charged)
    }

    pub(crate) fn try_acquire_scratch(
        &self,
        scratch_bytes: usize,
    ) -> Result<ReceiveMemoryPermit, ReceiveMemoryFull> {
        self.try_acquire_charge(scratch_bytes)
    }

    fn try_acquire_charge(&self, charged: usize) -> Result<ReceiveMemoryPermit, ReceiveMemoryFull> {
        let maximum = self.inner.limits.bytes_global();
        let mut used = self.inner.used.load(Ordering::Relaxed);
        loop {
            let Some(next) = used.checked_add(charged) else {
                return Err(ReceiveMemoryFull {
                    required: charged,
                    maximum,
                });
            };
            if next > maximum {
                return Err(ReceiveMemoryFull {
                    required: charged,
                    maximum,
                });
            }
            match self.inner.used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(ReceiveMemoryPermit {
                        inner: Arc::clone(&self.inner),
                        charged,
                    });
                }
                Err(actual) => used = actual,
            }
        }
    }

    pub(crate) fn used(&self) -> usize {
        self.inner.used.load(Ordering::Relaxed)
    }
}

impl Drop for ReceiveMemoryPermit {
    fn drop(&mut self) {
        let _ = self
            .inner
            .used
            .fetch_update(Ordering::AcqRel, Ordering::Relaxed, |used| {
                used.checked_sub(self.charged)
            });
    }
}

#[cfg(test)]
#[path = "receive_memory_test.rs"]
mod tests;
