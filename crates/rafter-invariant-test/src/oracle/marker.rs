//! Typed observation and violation emission for the invariant harness.

use std::fmt;

use crate::detector;

#[doc(hidden)]
pub fn observed() {
    if detector::mark_first_observation() {
        detector::emit_observed();
    }
}

#[doc(hidden)]
#[track_caller]
pub fn violation(message: fmt::Arguments<'_>) -> ! {
    std::panic::panic_any(detector::violation_message(message))
}

#[doc(hidden)]
#[must_use]
pub fn violation_message(message: fmt::Arguments<'_>) -> String {
    detector::violation_message(message)
}
