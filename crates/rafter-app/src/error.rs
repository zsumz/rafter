//! Error types for the embedded application/group driver.

use std::{error::Error, fmt, sync::Arc};

mod display;
mod group_error;
mod operation;

pub use group_error::GroupError;
pub use operation::StateMachineOperation;

/// A typed error preserved across a layer boundary.
///
/// A Rafter error names a stable category; the cause names what actually
/// failed. Both are needed: the category is what a caller branches on and what
/// a metric labels, and the cause is what an operator reads. Rendering the
/// cause into the category's message loses the second and does not improve the
/// first.
///
/// The cause is shared rather than owned because one failure fans out to every
/// entry of a write batch, and a `Box<dyn Error>` cannot be cloned. It is
/// type-erased rather than a type parameter because the boundary it crosses is
/// a client boundary: a driver that reaches its group over a network holds its
/// own transport error, not the leader's application error, and a client type
/// parameterized over the leader's error type would be a promise no networked
/// driver can keep.
///
/// This type is deliberately not itself a [`std::error::Error`]. It is a
/// handle, and it is transparent to `source()`: an error carrying a cause
/// returns the *inner* error from its own `source()`, so a chain printer walks
/// one link per real failure rather than one per boundary crossed.
#[derive(Clone)]
pub struct ErrorCause(Arc<dyn Error + Send + Sync + 'static>);

impl ErrorCause {
    /// Preserves `error` as the cause of a Rafter error.
    #[must_use]
    pub fn new<E>(error: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self(Arc::new(error))
    }

    /// Returns the preserved error.
    #[must_use]
    pub fn as_error(&self) -> &(dyn Error + Send + Sync + 'static) {
        self.0.as_ref()
    }

    /// Preserves an already-shared `error` as the cause of a Rafter error.
    ///
    /// This is the constructor for a failure with two owners. A group that
    /// poisons retains the state machine's error as its poison cause *and*
    /// hands the same error back to the caller inside
    /// [`GroupError::StateMachine`], and [`ReplicatedStateMachine::Error`] is
    /// deliberately not `Clone`, so one allocation is shared rather than two
    /// values produced.
    ///
    /// [`ReplicatedStateMachine::Error`]: crate::state_machine::ReplicatedStateMachine::Error
    #[must_use]
    pub fn from_shared<E>(error: Arc<E>) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self(error)
    }

    /// Returns the preserved error when it is of type `E`.
    ///
    /// An embedder whose own state machine or runtime produced the failure
    /// recovers its exact type here, which is what makes a typed recovery path
    /// writable. A caller on the far side of a transport recovers whatever
    /// *that* driver preserved, which is that driver's error and not the
    /// leader's — a cause is preserved across one boundary, not serialized
    /// across the network.
    #[must_use]
    pub fn downcast_ref<E>(&self) -> Option<&E>
    where
        E: Error + 'static,
    {
        let error: &(dyn Error + 'static) = self.0.as_ref();
        error.downcast_ref::<E>()
    }
}

impl fmt::Debug for ErrorCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self.as_error(), formatter)
    }
}

impl fmt::Display for ErrorCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self.as_error(), formatter)
    }
}

#[cfg(test)]
#[path = "error_test.rs"]
mod tests;
