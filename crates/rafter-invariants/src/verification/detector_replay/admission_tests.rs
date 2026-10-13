//! Unsupported hosts must never enter replay effects; Linux executes one attempt.

use super::{run, DetectorReplayAttempt};

#[test]
#[cfg(not(target_os = "linux"))]
fn unsupported_host_refuses_before_any_compilation_or_fixture_effect() {
    let attempt = run(|| panic!("unsupported host reached replay execution"));
    let DetectorReplayAttempt::CompilationFailed(failure) = attempt else {
        panic!("unsupported platform cannot report a completed replay");
    };
    assert_eq!(
        failure.message,
        "detector replay requires descriptor-bound executable launch on Linux"
    );
    assert!(failure.metadata_output.is_none());
    assert!(failure.compiler_output.is_none());
    assert!(failure.retained_diagnostics.is_none());
}

#[test]
#[cfg(target_os = "linux")]
fn linux_enters_exactly_one_attempt_and_preserves_its_outcome() {
    let mut calls = 0;
    let attempt = run(|| {
        calls += 1;
        DetectorReplayAttempt::CompilationFailed(Box::new(super::CompilationFailure {
            message: "backend outcome".to_owned(),
            metadata_output: None,
            compiler_output: None,
            retained_diagnostics: None,
        }))
    });
    assert_eq!(calls, 1);
    let DetectorReplayAttempt::CompilationFailed(failure) = attempt else {
        panic!("admission must preserve the backend outcome");
    };
    assert_eq!(failure.message, "backend outcome");
}
