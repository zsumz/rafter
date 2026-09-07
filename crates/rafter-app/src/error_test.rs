//! Display text and source chains for embedded driver errors.
//!
//! A group error must render the underlying error rather than restate a
//! category, and a preserved cause must stay downcastable to the error the
//! caller kept; a poisoned group carrying no cause reports no source at all.

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
