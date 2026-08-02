//! Revocable authorization carried by accepted outbound work.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// A cheap, lock-free proof that one binding or policy grant remains valid.
#[derive(Clone, Debug)]
pub(crate) struct AuthorizationLease {
    valid: Arc<AtomicBool>,
}

/// Lock-free proof that an accepted outbound route remains valid end to end.
#[derive(Clone, Debug)]
pub(crate) struct RouteAuthorization {
    source: AuthorizationLease,
    destination: AuthorizationLease,
}

impl RouteAuthorization {
    pub(crate) fn new(source: AuthorizationLease, destination: AuthorizationLease) -> Self {
        Self {
            source,
            destination,
        }
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.source.is_valid() && self.destination.is_valid()
    }
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
