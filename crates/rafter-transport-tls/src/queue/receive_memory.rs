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
    pub(crate) fn new(limits: ReceiveMemoryLimits) -> Self {
        Self {
            inner: Arc::new(BudgetInner {
                limits,
                used: AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) fn try_acquire(
        &self,
        frame_bytes: usize,
    ) -> Result<ReceiveMemoryPermit, ReceiveMemoryFull> {
        let charged = self.inner.limits.charge(frame_bytes);
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
        let limits = ReceiveMemoryLimits::new(100, 10).expect("valid memory limits");
        let budget = ReceiveMemoryBudget::new(limits);
        let first = budget.try_acquire(6).expect("60 weighted bytes");
        assert_eq!(budget.used(), 60);
        assert!(matches!(
            budget.try_acquire(5),
            Err(ReceiveMemoryFull {
                required: 50,
                maximum: 100,
            })
        ));
        drop(first);
        assert_eq!(budget.used(), 0);
        assert!(budget.try_acquire(10).is_ok());
    }
}
