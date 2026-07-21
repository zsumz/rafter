//! Structural verification policy for exact Rust test executions.

mod receipt;

pub(crate) use receipt::validate as validate_receipt;
