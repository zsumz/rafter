//! Public assertion and invocation macros.

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
