//! Explicit assertion macros for tests consumed by the invariant gate.
//!
//! The gate supplies a source-bound token. A typed assertion emits one
//! observation marker on success or one violation marker on failure. Ordinary
//! panics and assertions remain harness errors instead of being mistaken for
//! protocol counterexamples.

extern crate self as rafter_invariant_test;

use std::{
    any::type_name,
    cell::RefCell,
    fmt::{Debug, Display},
    process::{ExitCode, Termination},
    sync::atomic::{AtomicBool, Ordering},
};

pub use rafter_invariant_test_macros::detector_test;

const TOKEN_ENV: &str = "RAFTER_INVARIANT_ORACLE_TOKEN";
const OBSERVED_PREFIX: &str = "RAFTER_INVARIANT_ORACLE_OBSERVED:";
const VIOLATION_PREFIX: &str = "RAFTER_INVARIANT_ORACLE_VIOLATION:";
const DETECTOR_WITNESS_PREFIX: &str = "RAFTER_INVARIANT_DETECTOR_WITNESS:";

static OBSERVED: AtomicBool = AtomicBool::new(false);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DetectorWitness {
    kind: &'static str,
    identity: &'static str,
}

#[derive(Debug, Default)]
struct DetectorTestState {
    active: bool,
    gate: DetectorGate,
    witnesses: Vec<DetectorWitness>,
}

#[derive(Debug, Default)]
enum DetectorGate {
    #[default]
    Ordinary,
    Active(String),
    InvalidToken,
}

std::thread_local! {
    static DETECTOR_TEST_STATE: RefCell<DetectorTestState> = RefCell::default();
}

/// Opaque successful return value produced by [`detector_test`].
#[derive(Debug)]
pub struct DetectorTestOutcome {
    active: bool,
    gate: DetectorGate,
    witnesses: Vec<DetectorWitness>,
}

impl Termination for DetectorTestOutcome {
    fn report(self) -> ExitCode {
        let has_rejection = self
            .witnesses
            .iter()
            .any(|witness| witness.kind == "expect-err");
        if !self.active || !has_rejection {
            eprintln!("detector test returned without an invocation-bound rejection");
            return ExitCode::FAILURE;
        }
        let token = match self.gate {
            DetectorGate::Ordinary => return ExitCode::SUCCESS,
            DetectorGate::InvalidToken => {
                eprintln!("detector test started with an invalid gate token");
                return ExitCode::FAILURE;
            }
            DetectorGate::Active(token) => match std::env::var(TOKEN_ENV) {
                Ok(current) if current == token => token,
                Ok(_) => {
                    eprintln!("detector test returned with a different gate token");
                    return ExitCode::FAILURE;
                }
                Err(_) => {
                    eprintln!("detector test returned without its gate token");
                    return ExitCode::FAILURE;
                }
            },
        };
        for witness in self.witnesses {
            eprintln!(
                "{DETECTOR_WITNESS_PREFIX}{token}:{}:{}()",
                witness.kind, witness.identity
            );
        }
        ExitCode::SUCCESS
    }
}

#[doc(hidden)]
pub fn __begin_detector_test() {
    DETECTOR_TEST_STATE.with_borrow_mut(|state| {
        assert!(!state.active, "detector test session was started twice");
        state.active = true;
        state.gate = match std::env::var(TOKEN_ENV) {
            Ok(token) => DetectorGate::Active(token),
            Err(std::env::VarError::NotPresent) => DetectorGate::Ordinary,
            Err(std::env::VarError::NotUnicode(_)) => DetectorGate::InvalidToken,
        };
        state.witnesses.clear();
    });
    OBSERVED.store(false, Ordering::Relaxed);
}

#[doc(hidden)]
#[must_use]
pub fn __detector_test_outcome() -> DetectorTestOutcome {
    DETECTOR_TEST_STATE.with_borrow_mut(|state| DetectorTestOutcome {
        active: std::mem::take(&mut state.active),
        gate: std::mem::take(&mut state.gate),
        witnesses: std::mem::take(&mut state.witnesses),
    })
}

#[doc(hidden)]
pub fn __oracle_observed() {
    if !OBSERVED.swap(true, Ordering::Relaxed) {
        if let Ok(token) = std::env::var(TOKEN_ENV) {
            eprintln!("{OBSERVED_PREFIX}{token}");
        }
    }
}

fn __oracle_detector_witness(kind: &'static str, detector: &'static str) {
    DETECTOR_TEST_STATE.with_borrow_mut(|state| {
        if state.active {
            state.witnesses.push(DetectorWitness {
                kind,
                identity: detector,
            });
        }
    });
}

#[cfg(test)]
fn __oracle_fabricated_detector_witness(kind: &str, detector: &str) {
    if let Ok(token) = std::env::var(TOKEN_ENV) {
        eprintln!("{DETECTOR_WITNESS_PREFIX}{token}:{kind}:{detector}()");
    }
}

/// Internal function-call adapter used by the invocation-bound oracle macros.
///
/// The function item itself is retained as `Self`, so its compiler-resolved
/// type name cannot be replaced with a caller-supplied detector label.
#[doc(hidden)]
pub trait __OracleCall<Arguments> {
    type Output;

    fn __oracle_call(self, arguments: Arguments) -> Self::Output;
}

macro_rules! impl_oracle_call {
    (() => ()) => {
        impl<Function, Output> __OracleCall<()> for Function
        where
            Function: FnOnce() -> Output,
        {
            type Output = Output;

            fn __oracle_call(self, (): ()) -> Self::Output {
                self()
            }
        }
    };
    (($($argument:ident),+) => ($($value:ident),+)) => {
        impl<Function, Output, $($argument),+> __OracleCall<($($argument,)+)> for Function
        where
            Function: FnOnce($($argument),+) -> Output,
        {
            type Output = Output;

            #[allow(non_snake_case)]
            fn __oracle_call(self, ($($value,)+): ($($argument,)+)) -> Self::Output {
                self($($value),+)
            }
        }
    };
}

impl_oracle_call!(() => ());
impl_oracle_call!((A0) => (a0));
impl_oracle_call!((A0, A1) => (a0, a1));
impl_oracle_call!((A0, A1, A2) => (a0, a1, a2));
impl_oracle_call!((A0, A1, A2, A3) => (a0, a1, a2, a3));
impl_oracle_call!((A0, A1, A2, A3, A4) => (a0, a1, a2, a3, a4));
impl_oracle_call!((A0, A1, A2, A3, A4, A5) => (a0, a1, a2, a3, a4, a5));
impl_oracle_call!((A0, A1, A2, A3, A4, A5, A6) => (a0, a1, a2, a3, a4, a5, a6));
impl_oracle_call!((A0, A1, A2, A3, A4, A5, A6, A7) => (a0, a1, a2, a3, a4, a5, a6, a7));

/// Invoke a detector and emit its compiler-resolved identity only after it
/// returns the expected rejection.
#[doc(hidden)]
#[track_caller]
pub fn __oracle_expect_err<Function, Arguments, Value, Error, Message>(
    detector: Function,
    arguments: Arguments,
    message: Message,
) -> Error
where
    Function: __OracleCall<Arguments, Output = Result<Value, Error>>,
    Value: Debug,
    Message: Display,
{
    let detector_identity = type_name::<Function>();
    match detector.__oracle_call(arguments) {
        Err(error) => {
            __oracle_detector_witness("expect-err", detector_identity);
            __oracle_observed();
            error
        }
        Ok(value) => __oracle_violation(format_args!("{message}: {value:?}")),
    }
}

/// Invoke a recorder and emit its compiler-resolved identity only after it
/// returns normally.
#[doc(hidden)]
pub fn __oracle_invoke_recorder<Function, Arguments>(recorder: Function, arguments: Arguments)
where
    Function: __OracleCall<Arguments, Output = ()>,
{
    let recorder_identity = type_name::<Function>();
    recorder.__oracle_call(arguments);
    __oracle_detector_witness("recorder", recorder_identity);
}

#[doc(hidden)]
#[track_caller]
pub fn __oracle_violation(message: std::fmt::Arguments<'_>) -> ! {
    match std::env::var(TOKEN_ENV) {
        Ok(token) => std::panic::panic_any(format!("{VIOLATION_PREFIX}{token}: {message}")),
        Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => {
            std::panic::panic_any(message.to_string())
        }
    }
}

#[doc(hidden)]
#[must_use]
pub fn __oracle_violation_message(message: std::fmt::Arguments<'_>) -> String {
    match std::env::var(TOKEN_ENV) {
        Ok(token) => format!("{VIOLATION_PREFIX}{token}: {message}"),
        Err(std::env::VarError::NotPresent | std::env::VarError::NotUnicode(_)) => {
            message.to_string()
        }
    }
}

/// Assert a boolean invariant and emit a typed gate observation.
#[macro_export]
macro_rules! oracle_assert {
    ($condition:expr $(,)?) => {{
        if $condition {
            $crate::__oracle_observed();
        } else {
            $crate::__oracle_violation(format_args!(
                "assertion failed: {}",
                stringify!($condition)
            ));
        }
    }};
    ($condition:expr, $($message:tt)+) => {{
        if $condition {
            $crate::__oracle_observed();
        } else {
            $crate::__oracle_violation(format_args!($($message)+));
        }
    }};
}

/// Assert equality and emit a typed gate observation.
#[macro_export]
macro_rules! oracle_assert_eq {
    ($left:expr, $right:expr $(,)?) => {{
        match (&$left, &$right) {
            (left, right) if *left == *right => $crate::__oracle_observed(),
            (left, right) => $crate::__oracle_violation(format_args!(
                "assertion `left == right` failed\n  left: {left:?}\n right: {right:?}"
            )),
        }
    }};
    ($left:expr, $right:expr, $($message:tt)+) => {{
        match (&$left, &$right) {
            (left, right) if *left == *right => $crate::__oracle_observed(),
            _ => $crate::__oracle_violation(format_args!($($message)+)),
        }
    }};
}

/// Assert inequality and emit a typed gate observation.
#[macro_export]
macro_rules! oracle_assert_ne {
    ($left:expr, $right:expr $(,)?) => {{
        match (&$left, &$right) {
            (left, right) if *left != *right => $crate::__oracle_observed(),
            (left, right) => $crate::__oracle_violation(format_args!(
                "assertion `left != right` failed\n  left: {left:?}\n right: {right:?}"
            )),
        }
    }};
    ($left:expr, $right:expr, $($message:tt)+) => {{
        match (&$left, &$right) {
            (left, right) if *left != *right => $crate::__oracle_observed(),
            _ => $crate::__oracle_violation(format_args!($($message)+)),
        }
    }};
}

/// Report an explicit invariant violation.
#[macro_export]
macro_rules! oracle_violation {
    () => {
        $crate::__oracle_violation(format_args!("explicit invariant violation"))
    };
    ($($message:tt)+) => {
        $crate::__oracle_violation(format_args!($($message)+))
    };
}

/// Invoke a named detector, require it to reject the input, and return its error.
///
/// The first argument is intentionally restricted to a direct function call so
/// the detector identity cannot be supplied independently of the invocation.
#[macro_export]
macro_rules! oracle_expect_err {
    ($detector:ident($($argument:expr),* $(,)?), $message:expr $(,)?) => {{
        $crate::__oracle_expect_err($detector, ($($argument,)*), $message)
    }};
}

/// Invoke a named recorder and emit its witness only after the call returns.
///
/// This is the unit-returning counterpart to [`oracle_expect_err!`].
#[macro_export]
macro_rules! oracle_invoke_recorder {
    ($recorder:ident($($argument:expr),* $(,)?)) => {{
        $crate::__oracle_invoke_recorder($recorder, ($($argument,)*));
    }};
}

/// Assert a proptest condition while preserving shrinking behavior.
#[macro_export]
macro_rules! oracle_prop_assert {
    ($condition:expr $(,)?) => {{
        ::proptest::prop_assert!(
            $condition,
            "{}",
            $crate::__oracle_violation_message(format_args!(
                "assertion failed: {}",
                stringify!($condition)
            ))
        );
        $crate::__oracle_observed();
    }};
    ($condition:expr, $($message:tt)+) => {{
        ::proptest::prop_assert!(
            $condition,
            "{}",
            $crate::__oracle_violation_message(format_args!($($message)+))
        );
        $crate::__oracle_observed();
    }};
}

/// Assert proptest equality while preserving shrinking behavior.
#[macro_export]
macro_rules! oracle_prop_assert_eq {
    ($left:expr, $right:expr $(,)?) => {{
        ::proptest::prop_assert_eq!(
            $left,
            $right,
            "{}",
            $crate::__oracle_violation_message(format_args!(
                "assertion `left == right` failed"
            ))
        );
        $crate::__oracle_observed();
    }};
    ($left:expr, $right:expr, $($message:tt)+) => {{
        ::proptest::prop_assert_eq!(
            $left,
            $right,
            "{}",
            $crate::__oracle_violation_message(format_args!($($message)+))
        );
        $crate::__oracle_observed();
    }};
}

#[cfg(test)]
mod tests;
