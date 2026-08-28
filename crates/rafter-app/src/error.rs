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
mod tests {
    use super::*;

    #[derive(Debug)]
    struct TestError(&'static str);

    impl fmt::Display for TestError {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str(self.0)
        }
    }

    impl Error for TestError {}

    #[test]
    fn group_error_display_uses_underlying_error_messages() {
        let error = GroupError::<TestError, TestError>::StateMachine {
            operation: StateMachineOperation::ApplyBatch,
            source: Arc::new(TestError("apply failed")),
        };

        assert_eq!(
            error.to_string(),
            "state machine batch apply failed: apply failed"
        );
    }

    /// The shared source is the same object the group kept as its poison cause,
    /// and it stays reachable as a typed `source()` link.
    #[test]
    fn a_state_machine_group_error_exposes_its_shared_source() {
        let source = Arc::new(TestError("apply failed"));
        let error = GroupError::<TestError, TestError>::StateMachine {
            operation: StateMachineOperation::ApplyBatch,
            source: Arc::clone(&source),
        };
        let cause = ErrorCause::from_shared(source);

        assert_eq!(
            error
                .source()
                .expect("the state machine error is exposed")
                .to_string(),
            "apply failed"
        );
        assert!(cause.downcast_ref::<TestError>().is_some());
    }

    #[test]
    fn a_preserved_cause_downcasts_to_the_error_the_caller_kept() {
        let cause = ErrorCause::new(TestError("apply failed"));

        assert_eq!(
            cause
                .downcast_ref::<TestError>()
                .expect("the preserved error keeps its own type")
                .0,
            "apply failed"
        );
        assert!(cause.downcast_ref::<fmt::Error>().is_none());
    }

    #[test]
    fn a_preserved_cause_renders_as_the_error_it_holds() {
        let cause = ErrorCause::new(TestError("apply failed"));

        assert_eq!(cause.to_string(), "apply failed");
        assert_eq!(
            format!("{cause:?}"),
            format!("{:?}", TestError("apply failed"))
        );
        assert_eq!(cause.as_error().to_string(), "apply failed");
    }

    /// The cause is a handle rather than an error, so a chain printer walks one
    /// link per real failure: `source()` reaches the preserved error directly
    /// and not an `ErrorCause` wrapper that renders the same text twice.
    #[test]
    fn a_poisoned_group_error_exposes_the_preserved_cause_as_its_source() {
        let error = GroupError::<TestError, TestError>::Poisoned {
            reason: "ApplyBatch failed".to_owned(),
            cause: Some(ErrorCause::new(TestError("apply failed"))),
        };

        let source = error.source().expect("the poison cause is exposed");

        assert_eq!(source.to_string(), "apply failed");
        assert!(source.downcast_ref::<TestError>().is_some());
        assert!(source.source().is_none());
    }

    /// The `Option` is not decoration: a poison with no underlying error must
    /// not invent one.
    #[test]
    fn a_poisoned_group_error_without_a_cause_has_no_source() {
        let error = GroupError::<TestError, TestError>::Poisoned {
            reason: "malformed snapshot output: snapshot last included index is zero".to_owned(),
            cause: None,
        };

        assert!(error.source().is_none());
    }

    /// The category is what a caller branches on; the cause is reached through
    /// `source()`. A `Display` that interpolated the cause would print it twice
    /// in any chain-aware report.
    #[test]
    fn poisoned_display_states_the_category_without_repeating_the_cause() {
        let error = GroupError::<TestError, TestError>::Poisoned {
            reason: "ApplyBatch failed".to_owned(),
            cause: Some(ErrorCause::new(TestError("disk unavailable"))),
        };

        let rendered = error.to_string();

        assert_eq!(rendered, "Raft group is poisoned: ApplyBatch failed");
        assert!(!rendered.contains("disk unavailable"));
    }

    #[test]
    fn group_error_sources_are_available_for_underlying_errors() {
        let error = GroupError::<TestError, TestError>::Runtime(TestError("disk unavailable"));

        assert_eq!(
            error
                .source()
                .expect("runtime error is exposed as source")
                .to_string(),
            "disk unavailable"
        );
    }
}
