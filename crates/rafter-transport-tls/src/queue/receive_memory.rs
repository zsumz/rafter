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
mod tests {
    use super::*;

    #[test]
    fn declared_lengths_reserve_before_allocation_and_release_exactly() {
        let limits = ReceiveMemoryLimits::new(320, 32).expect("valid memory limits");
        let budget = ReceiveMemoryBudget::new(limits, 8);
        let first = budget.try_acquire_frame(6).expect("200 weighted bytes");
        assert_eq!(budget.used(), 200);
        assert!(matches!(
            budget.try_acquire_frame(4),
            Err(ReceiveMemoryFull {
                required: 136,
                maximum: 320,
            })
        ));
        drop(first);
        assert_eq!(budget.used(), 0);
        assert!(budget.try_acquire_frame(9).is_ok());
    }

    #[test]
    fn retained_scratch_and_frames_share_one_global_budget() {
        let limits = ReceiveMemoryLimits::new(320, 32).expect("valid memory limits");
        let budget = ReceiveMemoryBudget::new(limits, 8);
        let scratch = budget.try_acquire_scratch(120).expect("connection scratch");
        let frame = budget.try_acquire_frame(6).expect("remaining frame budget");

        assert_eq!(budget.used(), 320);
        assert!(matches!(
            budget.try_acquire_scratch(1),
            Err(ReceiveMemoryFull {
                required: 1,
                maximum: 320,
            })
        ));
        drop(frame);
        drop(scratch);
        assert_eq!(budget.used(), 0);
    }
}
