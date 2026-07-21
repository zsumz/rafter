//! Shared source-analysis vocabulary for invariant-tooling architecture scenarios.

mod domain;
mod module_graph;
mod rust_paths;
mod source_inventory;
mod workspace;

pub(crate) use domain::{
    assert_domain_source_imports_follow_manifest, assert_forbidden_domain_imports_absent, domain,
};
pub(crate) use module_graph::{
    declared_module_graph, declared_module_graph_from_roots, declared_module_path,
    is_declared_test_module, module_is_test_only, DeclaredModuleGraph,
};
pub(crate) use rust_paths::{
    normalize_rust_path, BlockingProcessCollector, PathContext, RustPathCollector,
};
pub(crate) use source_inventory::{
    declared_macros, function_call_counts, macro_second_identifiers, public_associated_methods,
    public_free_functions, string_array_constant,
};
pub(crate) use workspace::{
    declares_implementation, display_path, invariant_rust_files, is_legacy_verifier,
    is_test_module, legacy_verifier_references, read, rust_files, starts_with_module_contract,
    workspace_root,
};
