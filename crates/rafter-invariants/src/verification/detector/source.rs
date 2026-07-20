//! Invocation-bound detector source analysis over an authenticated target graph.

mod analysis;
mod binding;
mod cache;
mod contract;
mod control_flow;
mod function_body;
mod function_collection;
mod function_index;
mod imports;
mod model;
mod policy;
mod reachability;
mod syntax;

#[cfg(test)]
pub(super) use analysis::verify_invocation_bound_detector;
pub(super) use analysis::verify_invocation_bound_detector_cached;
pub(crate) use cache::DetectorSourceCache;
pub(crate) use contract::DetectorInvocationContract;

// Compatibility aliases for existing source-analysis modules. They remain private to this
// domain while the large analyzer facade is decomposed.
use function_collection::collect_functions;
use function_index::CallTarget;
use model::{
    CallableArgument, FunctionCall, FunctionEvent, FunctionFacts, FunctionFallthrough,
    InvocationCall, SourceDefect,
};
use policy::{FORBIDDEN_CALLS, FORBIDDEN_WITNESS_HELPERS, ORACLE_MACROS, SAFE_BUILTIN_MACROS};

#[cfg(test)]
#[path = "source_tests.rs"]
pub(super) mod tests;

#[cfg(test)]
#[path = "source_adversarial_tests.rs"]
mod adversarial_tests;

#[cfg(test)]
#[path = "source_module_graph_tests.rs"]
mod module_graph_tests;
