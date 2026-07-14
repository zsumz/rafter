//! Explicit assertion macros for tests consumed by the invariant gate.
//!
//! The gate supplies a source-bound token. A typed assertion emits one
//! observation marker on success or one violation marker on failure. Ordinary
//! panics and assertions remain harness errors instead of being mistaken for
//! protocol counterexamples.

use std::sync::atomic::{AtomicBool, Ordering};

const TOKEN_ENV: &str = "RAFTER_INVARIANT_ORACLE_TOKEN";
const OBSERVED_PREFIX: &str = "RAFTER_INVARIANT_ORACLE_OBSERVED:";
const VIOLATION_PREFIX: &str = "RAFTER_INVARIANT_ORACLE_VIOLATION:";

static OBSERVED: AtomicBool = AtomicBool::new(false);

#[doc(hidden)]
pub fn __oracle_observed() {
    if !OBSERVED.swap(true, Ordering::Relaxed) {
        if let Ok(token) = std::env::var(TOKEN_ENV) {
            eprintln!("{OBSERVED_PREFIX}{token}");
        }
    }
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

/// Require a detector or validator to reject an input and return its error.
#[macro_export]
macro_rules! oracle_expect_err {
    ($result:expr, $message:expr $(,)?) => {{
        match $result {
            ::core::result::Result::Err(error) => {
                $crate::__oracle_observed();
                error
            }
            ::core::result::Result::Ok(value) => {
                $crate::__oracle_violation(format_args!("{}: {value:?}", $message))
            }
        }
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
mod tests {
    #[test]
    fn ordinary_success_does_not_require_gate_environment() {
        oracle_assert!(true);
        oracle_assert_eq!(1, 1);
        oracle_assert_ne!(1, 2);
        let error = oracle_expect_err!(Result::<(), _>::Err("expected"), "must reject");
        assert_eq!(error, "expected");
    }
}
