//! Revocable authorization carried by accepted outbound work.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// A cheap, lock-free proof that one destination remains authorized.
#[derive(Clone, Debug)]
pub(crate) struct AuthorizationLease {
    valid: Arc<AtomicBool>,
}

impl AuthorizationLease {
    pub(crate) fn new() -> Self {
        Self {
            valid: Arc::new(AtomicBool::new(true)),
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire)
    }

    pub(super) fn revoke(&self) {
        self.valid.store(false, Ordering::Release);
    }
}
