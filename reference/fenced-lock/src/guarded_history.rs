use crate::{GuardedRejection, GuardedResource, GuardedWrite, OperationId, ResourceName};

/// One client-visible event from an external guarded resource.
///
/// Event position is real-time order. Each operation has one invocation and
/// one completion, correlated by [`OperationId`]. The protected resource name
/// is recorded separately from the name claimed by [`GuardedWrite`] so a
/// checker can prove that names were not conflated.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuardedHistoryEvent {
    /// A write was offered to one guarded resource.
    Invoked {
        /// Operation identity unique within this guarded history.
        operation_id: OperationId,
        /// Resource protected by the guard that received the write.
        guarded_resource: ResourceName,
        /// Exact write offered by the caller.
        write: GuardedWrite,
    },
    /// The caller observed the guarded resource's exact answer.
    Completed {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
        /// Accepted value or exact refusal.
        result: Result<u64, GuardedRejection>,
    },
}

impl GuardedHistoryEvent {
    /// Returns the operation identity this event belongs to.
    #[must_use]
    pub const fn operation_id(self) -> OperationId {
        match self {
            Self::Invoked { operation_id, .. } | Self::Completed { operation_id, .. } => {
                operation_id
            }
        }
    }
}

/// A guarded resource that records exact invocation intervals around `apply`.
///
/// The wrapper is intentionally small and synchronous: it records an
/// invocation immediately before delegating and a completion immediately
/// after the result is known. Tests should use this type instead of manually
/// assembling guarded histories.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordingGuardedResource {
    inner: GuardedResource,
    history: Vec<GuardedHistoryEvent>,
    next_operation_id: u64,
}

impl RecordingGuardedResource {
    /// Creates an empty guard and recorder for one resource.
    #[must_use]
    pub const fn new(resource: ResourceName) -> Self {
        Self {
            inner: GuardedResource::new(resource),
            history: Vec::new(),
            next_operation_id: 1,
        }
    }

    /// Offers and records one guarded write.
    ///
    /// # Errors
    ///
    /// Returns the exact rejection produced by the guarded resource.
    ///
    /// # Panics
    ///
    /// Panics after exhausting the complete nonzero `u64` operation-ID space
    /// rather than recording a duplicate identifier.
    pub fn apply(&mut self, write: GuardedWrite) -> Result<u64, GuardedRejection> {
        let operation_id = OperationId::new(self.next_operation_id);
        self.next_operation_id = self
            .next_operation_id
            .checked_add(1)
            .expect("guarded operation identifiers exhausted");
        self.history.push(GuardedHistoryEvent::Invoked {
            operation_id,
            guarded_resource: self.inner.resource(),
            write,
        });
        let result = self.inner.apply(write);
        self.history.push(GuardedHistoryEvent::Completed {
            operation_id,
            result,
        });
        result
    }

    /// Returns the protected resource name.
    #[must_use]
    pub const fn resource(&self) -> ResourceName {
        self.inner.resource()
    }

    /// Returns the highest token this guard has accepted.
    #[must_use]
    pub const fn highest_accepted(&self) -> Option<crate::FencingToken> {
        self.inner.highest_accepted()
    }

    /// Returns the stored value.
    #[must_use]
    pub const fn value(&self) -> u64 {
        self.inner.value()
    }

    /// Returns how many writes were accepted.
    #[must_use]
    pub const fn accepted_writes(&self) -> u64 {
        self.inner.accepted_writes()
    }

    /// Returns how many writes were refused.
    #[must_use]
    pub const fn refused_writes(&self) -> u64 {
        self.inner.refused_writes()
    }

    /// Returns the exact recorded history.
    #[must_use]
    pub fn history(&self) -> &[GuardedHistoryEvent] {
        &self.history
    }
}
