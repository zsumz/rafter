//! TLA+ source, tool, and bounded runner contract facade.

mod artifacts;
mod obligation;
mod options;
mod spec;
mod tool;

pub(super) use artifacts::source_artifacts;
pub(super) use options::{validate_obligation_options, validate_runner_options};
pub(super) use obligation::validate_obligation_specs;
pub(super) use spec::validate_spec_contract;
#[cfg(test)]
pub(crate) use tool::fetch_tool_at;
pub(super) use tool::{fetch_tool, parse_timeout, required_configuration, validate_java};

#[cfg(test)]
use obligation::validate_obligation_config_sources;
#[cfg(test)]
use spec::{
    configured_invariants, validate_safety_only_boundary, validate_symmetry_contract,
    validate_trace_contract_sources, SPEC, TRACE_CONFIG, TRACE_SPEC,
};
#[cfg(test)]
use tool::{fetch_tool_with, java_major, tool_fetch_environment};

#[cfg(test)]
#[path = "contract/tests.rs"]
mod tests;
