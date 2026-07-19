//! Assertion, detector-call, and recorder-call facade.

mod call;
mod macros;
mod marker;

pub use call::{expect_error, invoke_recorder, OracleCall};
pub use marker::{observed, violation, violation_message};
