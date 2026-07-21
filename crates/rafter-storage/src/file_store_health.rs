//! Shared post-error health state for file-backed store handles.
//!
//! A mutating I/O error can occur after the filesystem accepted some or all of
//! an operation. The handle must not guess which durable state won. File-backed
//! stores mark themselves as requiring reopen and reject later mutations.

/// Whether a file-backed store handle may safely perform another mutation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum FileStoreHealth {
    /// The handle agrees with the last successfully acknowledged durable state.
    #[default]
    Healthy,
    /// An earlier mutating I/O failure left the durable outcome ambiguous.
    ReopenRequired,
}

impl FileStoreHealth {
    /// Marks the handle unusable for further mutations until it is reopened.
    pub(crate) fn require_reopen(&mut self) {
        *self = Self::ReopenRequired;
    }

    /// Returns whether a fresh open is required before another mutation.
    pub(crate) const fn is_reopen_required(self) -> bool {
        matches!(self, Self::ReopenRequired)
    }
}
