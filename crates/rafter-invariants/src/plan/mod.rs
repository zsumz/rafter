//! Immutable execution-plan construction, capture, and validation facade.

mod capture;
mod model;
mod validate;

#[cfg(test)]
mod tests;

pub(crate) use capture::{capture_invocation, current_source_ref};
pub(crate) use model::CapturedInvocation;
pub use model::{ExecutionPlan, PlanOptions};
pub(crate) use validate::verify_plan_input;
