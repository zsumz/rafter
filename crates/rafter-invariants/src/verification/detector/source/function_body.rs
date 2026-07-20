//! Stateful semantic analysis of one detector-target function body.

use std::collections::{BTreeMap, BTreeSet};

use syn::{visit::Visit, Block, FnArg, Pat, Signature};

use super::{
    control_flow::{FunctionState, PathReachability, StatementState},
    function_index::{CallTarget, LocalCallResolver},
    imports::ImportedPaths,
    model::{FunctionFacts, FunctionFallthrough, SourceDefect},
    policy::{is_detector_test_attribute, is_trusted_function_attribute},
    syntax::block_end_may_complete_normally,
};

mod calls;
mod flow;
mod macros;
mod scope;
mod visit;

pub(super) fn analyze_function(
    imports: &ImportedPaths,
    resolver: &LocalCallResolver,
    module: &[String],
    self_type: Option<&[String]>,
    attributes: &[syn::Attribute],
    signature: &Signature,
    block: &Block,
) -> FunctionFacts {
    let parameter_bindings = signature
        .inputs
        .iter()
        .filter_map(|argument| match argument {
            FnArg::Typed(argument) => Some(argument),
            FnArg::Receiver(_) => None,
        })
        .enumerate()
        .filter_map(|(index, argument)| match argument.pat.as_ref() {
            Pat::Ident(pattern) if pattern.subpat.is_none() => {
                Some((pattern.ident.to_string(), index))
            }
            _ => None,
        })
        .collect();
    let mut visitor = FunctionBodyVisitor {
        imports: imports.clone(),
        resolver,
        module,
        self_type,
        guaranteed: true,
        reachability: PathReachability::Reachable,
        statement: StatementState::default(),
        function: FunctionState::default(),
        value_aliases: BTreeMap::new(),
        value_bindings: BTreeSet::new(),
        parameter_bindings,
        value_types: BTreeMap::new(),
        scoped_resolver: resolver.scoped(),
        facts: FunctionFacts {
            detector_test_attributes: attributes
                .iter()
                .filter(|attribute| is_detector_test_attribute(attribute))
                .count(),
            conditional_compilation: attributes.iter().any(|attribute| {
                attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
            }),
            untrusted_attributes: attributes
                .iter()
                .any(|attribute| !is_trusted_function_attribute(attribute, imports)),
            defects: if signature.unsafety.is_some() || signature.abi.is_some() {
                BTreeSet::from([SourceDefect::UnsafeCapability])
            } else {
                BTreeSet::new()
            },
            ..FunctionFacts::default()
        },
    };
    visitor.visit_signature(signature);
    visitor.visit_block(block);
    visitor.facts.fallthrough = FunctionFallthrough::from_analysis(
        visitor.function.normal_return_seen || block_end_may_complete_normally(block),
        !visitor.function.may_diverge,
    );
    visitor.facts
}

struct FunctionBodyVisitor<'a> {
    imports: ImportedPaths,
    resolver: &'a LocalCallResolver,
    module: &'a [String],
    self_type: Option<&'a [String]>,
    guaranteed: bool,
    reachability: PathReachability,
    statement: StatementState,
    function: FunctionState,
    value_aliases: BTreeMap<String, CallTarget>,
    value_bindings: BTreeSet<String>,
    parameter_bindings: BTreeMap<String, usize>,
    value_types: BTreeMap<String, Vec<String>>,
    scoped_resolver: LocalCallResolver,
    facts: FunctionFacts,
}

impl FunctionBodyVisitor<'_> {
    fn with_guarantee(&mut self, guaranteed: bool, visit: impl FnOnce(&mut Self)) {
        let previous = self.guaranteed;
        self.guaranteed = previous && guaranteed;
        visit(self);
        self.guaranteed = previous;
    }
}
