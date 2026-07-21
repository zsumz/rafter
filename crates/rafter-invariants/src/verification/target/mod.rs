//! Independent Cargo target source identity and protected compiler-artifact policy.
//!
//! The exact trust anchors remain review-visible here:
//! `crates/rafter-invariant-test/src/oracle/macros.rs`,
//! `crates/rafter-invariant-test/src/oracle/call.rs`, and
//! `crates/rafter-invariant-test/src/detector/session.rs`.
//! Source-graph construction accepts `reserved_macros: &[&str]` from its policy owner.

mod cargo_target;
mod cfg;
mod oracle_source;
mod policy;
mod protected_compiler;
mod source_graph;
#[cfg(test)]
mod tests;
mod traversal;

pub(crate) use cfg::module_active_for_test;
pub(crate) use oracle_source::{verify_registered_oracle_sources, RegisteredTestBinding};
pub(crate) use protected_compiler::verify_protected_compiler_artifacts;
pub(crate) use source_graph::{target_source_graph, SourceModule, TargetSourceGraph};

pub(crate) const ORACLE_MACROS: &[&str] = policy::ORACLE_MACROS;
