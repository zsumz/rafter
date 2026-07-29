use std::{error::Error, fmt, num::NonZeroUsize};

/// Bounds fixed for one managed scheduler.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ManagedConfig {
    workers: NonZeroUsize,
    max_group_queue: NonZeroUsize,
    max_global_queue: NonZeroUsize,
    default_quota: NonZeroUsize,
}

impl ManagedConfig {
    /// Creates a managed scheduler configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ManagedConfigError::GlobalQueueBelowGroupQueue`] when the
    /// host-wide bound is smaller than the per-group bound.
    pub const fn new(
        workers: NonZeroUsize,
        max_group_queue: NonZeroUsize,
        max_global_queue: NonZeroUsize,
        default_quota: NonZeroUsize,
    ) -> Result<Self, ManagedConfigError> {
        if max_global_queue.get() < max_group_queue.get() {
            return Err(ManagedConfigError::GlobalQueueBelowGroupQueue {
                group: max_group_queue,
                global: max_global_queue,
            });
        }
        Ok(Self {
            workers,
            max_group_queue,
            max_global_queue,
            default_quota,
        })
    }

    /// Maximum simultaneous in-flight dispatches.
    #[must_use]
    pub const fn workers(self) -> NonZeroUsize {
        self.workers
    }

    /// Maximum queued items in one group.
    #[must_use]
    pub const fn max_group_queue(self) -> NonZeroUsize {
        self.max_group_queue
    }

    /// Maximum queued items across all groups.
    #[must_use]
    pub const fn max_global_queue(self) -> NonZeroUsize {
        self.max_global_queue
    }

    /// Quota assigned to a group that does not override it.
    #[must_use]
    pub const fn default_quota(self) -> NonZeroUsize {
        self.default_quota
    }
}

/// Invalid managed scheduler bounds.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ManagedConfigError {
    /// The global queue bound would make the group bound unreachable.
    GlobalQueueBelowGroupQueue {
        /// Per-group queue bound.
        group: NonZeroUsize,
        /// Host-wide queue bound.
        global: NonZeroUsize,
    },
}

impl fmt::Display for ManagedConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::GlobalQueueBelowGroupQueue { group, global } => write!(
                formatter,
                "global queue bound {global} is below group queue bound {group}"
            ),
        }
    }
}

impl Error for ManagedConfigError {}
