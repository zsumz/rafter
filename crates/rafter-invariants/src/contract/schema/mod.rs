//! Checked-in schema sources and domain-neutral JSON validation.

mod json;

pub(crate) use json::validate;

pub(crate) const RESULT_SCHEMA: &str =
    include_str!("../../../../../verification/invariant-result-schema.json");
pub(crate) const VERDICT_SCHEMA: &str =
    include_str!("../../../../../verification/invariant-verdict-schema.json");

#[cfg(test)]
mod tests;
