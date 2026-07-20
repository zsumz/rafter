use std::collections::{BTreeMap, BTreeSet};

use syn::{
    visit::{self, Visit},
    BinOp, Block, Expr, ExprAssign, ExprAsync, ExprBinary, ExprBreak, ExprCall, ExprClosure,
    ExprContinue, ExprForLoop, ExprIf, ExprLoop, ExprMatch, ExprMethodCall, ExprReturn, ExprTry,
    ExprWhile, File, FnArg, ImplItem, ItemConst, ItemFn, ItemImpl, ItemMod, ItemStatic, Local,
    Macro, Pat, PatIdent, PatType, Signature, Stmt,
};

mod binding;
mod cache;
mod control_flow;
mod function_index;
mod imports;
mod macro_args;
mod model;
mod policy;
mod reachability;
mod syntax;

use binding::{bind_target_detector, registered_fixture_id, require_registered_fixture};
pub(crate) use cache::DetectorSourceCache;
use control_flow::{FunctionState, PathReachability, StatementState};
use function_index::{CallTarget, FunctionId, FunctionIndex, LocalCallResolver};
use imports::{
    collect_imports, collect_item_imports, safe_builtin_macro, trusted_oracle_macro,
    validate_oracle_provenance, ImportedPaths,
};
use model::{
    CallableArgument, FunctionCall, FunctionEvent, FunctionFacts, FunctionFallthrough,
    InvocationCall, InvocationKind, SourceDefect,
};
use policy::{
    is_detector_test_attribute, is_trusted_function_attribute, FORBIDDEN_CALLS,
    FORBIDDEN_WITNESS_HELPERS, INVOCATION_MACROS, ORACLE_MACROS, SAFE_BUILTIN_MACROS,
    TOKEN_ONLY_MACROS,
};
use reachability::expand_reachable_fixture;
use syntax::{
    block_end_may_complete_normally, loop_may_complete_normally, statement_may_complete_normally,
    statement_unconditionally_exits, unqualified_called_function, unqualified_expression_name,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct DetectorInvocationContract {
    witnesses: BTreeMap<String, usize>,
    registered_identity: String,
}

impl DetectorInvocationContract {
    pub(super) fn witnesses(&self) -> &BTreeMap<String, usize> {
        &self.witnesses
    }

    pub(super) fn registered_identity(&self) -> &str {
        &self.registered_identity
    }
}

#[cfg(test)]
pub(super) fn verify_invocation_bound_detector(
    binding: &crate::DetectorFixtureSourceBinding<'_>,
) -> Result<DetectorInvocationContract, String> {
    verify_invocation_bound_detector_cached(binding, &mut DetectorSourceCache::default())
}

pub(super) fn verify_invocation_bound_detector_cached(
    binding: &crate::DetectorFixtureSourceBinding<'_>,
    cache: &mut DetectorSourceCache,
) -> Result<DetectorInvocationContract, String> {
    let fixture_source = binding.fixture_source;
    let fixture_path = binding.fixture_path;
    let detector_path = binding.detector_path;
    let fixture = binding.fixture;
    let detector = binding.detector;
    let fixture_file = cache.source(fixture_path, fixture_source, "fixture")?;
    let detector_file = cache.source(detector_path, binding.detector_source, "detector")?;
    let target_analysis = cache.target(binding)?;
    let target_graph = &target_analysis.graph;
    let fixture_module = target_graph.source_module(binding.fixture_path)?;
    let imports = collect_imports(&fixture_file);
    validate_oracle_provenance(&imports)?;
    if imports.local_value_bindings.contains(detector) {
        return Err(format!(
            "registered detector `{detector}` is shadowed by a module value binding"
        ));
    }
    let target_resolver = &target_analysis.resolver;
    let fixture_functions = collect_functions(
        &fixture_file,
        &imports,
        target_resolver,
        &fixture_module.module,
        false,
    );
    let fixture_id = registered_fixture_id(binding.test_identity, fixture)?;
    require_registered_fixture(&fixture_functions, &fixture_id, fixture)?;
    let target = bind_target_detector(
        binding,
        &fixture_functions,
        target_resolver,
        &fixture_id,
        &detector_file,
        target_graph,
        &fixture_module,
    )?;
    let declarations = target.declarations;
    let registered_function = target.registered_function;
    let target_functions = &target_analysis.functions;

    let mut contract = DetectorInvocationContract {
        witnesses: BTreeMap::new(),
        registered_identity: target.registered_identity,
    };
    expand_reachable_fixture(
        target_functions,
        target_graph,
        &fixture_id,
        &registered_function,
        &fixture_module.crate_name,
        &declarations,
        &mut contract,
    )?;
    if !contract.witnesses.keys().any(|witness| {
        witness
            .split_once(':')
            .is_some_and(|(_, identity)| identity == contract.registered_identity)
    }) {
        return Err(format!(
            "negative fixture `{fixture}` does not invoke registered detector `{detector}` through an invocation-bound oracle macro; observed witnesses: {:?}",
            contract.witnesses
        ));
    }
    if !contract
        .witnesses
        .keys()
        .any(|witness| witness.starts_with("expect-err:"))
    {
        return Err(format!(
            "negative fixture `{fixture}` does not execute an invocation-bound rejecting detector"
        ));
    }
    Ok(contract)
}

fn collect_functions(
    file: &File,
    imports: &ImportedPaths,
    resolver: &LocalCallResolver,
    module: &[String],
    active_only: bool,
) -> FunctionIndex {
    let mut collector = FunctionCollector {
        imports: imports.clone(),
        resolver,
        active_only,
        module: module.to_vec(),
        functions: FunctionIndex::default(),
    };
    collector.visit_file(file);
    collector.functions
}

struct FunctionCollector<'a> {
    imports: ImportedPaths,
    resolver: &'a LocalCallResolver,
    active_only: bool,
    module: Vec<String>,
    functions: FunctionIndex,
}

impl FunctionCollector<'_> {
    fn collect_function(
        &mut self,
        attributes: &[syn::Attribute],
        signature: &Signature,
        block: &Block,
        id_module: Vec<String>,
        self_type: Option<&[String]>,
    ) {
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
            imports: self.imports.clone(),
            resolver: self.resolver,
            module: &self.module,
            self_type,
            guaranteed: true,
            reachability: PathReachability::Reachable,
            statement: StatementState::default(),
            function: FunctionState::default(),
            value_aliases: BTreeMap::new(),
            value_bindings: BTreeSet::new(),
            parameter_bindings,
            value_types: BTreeMap::new(),
            scoped_resolver: self.resolver.scoped(),
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
                    .any(|attribute| !is_trusted_function_attribute(attribute, &self.imports)),
                ..FunctionFacts::default()
            },
        };
        visitor.visit_signature(signature);
        visitor.visit_block(block);
        visitor.facts.fallthrough = FunctionFallthrough::from_analysis(
            visitor.function.normal_return_seen || block_end_may_complete_normally(block),
            !visitor.function.may_diverge,
        );
        self.functions
            .functions
            .entry(FunctionId {
                module: id_module,
                name: signature.ident.to_string(),
            })
            .or_default()
            .push(visitor.facts);
    }
}

impl<'ast> Visit<'ast> for FunctionCollector<'_> {
    fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
        self.imports.add_macro_declaration(item);
    }

    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        if self.active_only
            && !crate::verification::target::module_active_for_test(&function.attrs)
                .unwrap_or(false)
        {
            return;
        }
        self.collect_function(
            &function.attrs,
            &function.sig,
            &function.block,
            self.module.clone(),
            None,
        );
        visit::visit_item_fn(self, function);
    }

    fn visit_item_impl(&mut self, item: &'ast ItemImpl) {
        if self.active_only
            && !crate::verification::target::module_active_for_test(&item.attrs).unwrap_or(false)
        {
            return;
        }
        let Some(type_module) = self
            .resolver
            .declared_type_module(&item.self_ty, &self.module)
        else {
            return;
        };
        for item in &item.items {
            match item {
                ImplItem::Const(item) => {
                    if self.active_only
                        && !crate::verification::target::module_active_for_test(&item.attrs)
                            .unwrap_or(false)
                    {
                        continue;
                    }
                    self.functions.values.insert(FunctionId {
                        module: type_module.clone(),
                        name: item.ident.to_string(),
                    });
                }
                ImplItem::Fn(method) => {
                    if self.active_only
                        && !crate::verification::target::module_active_for_test(&method.attrs)
                            .unwrap_or(false)
                    {
                        continue;
                    }
                    self.collect_function(
                        &method.attrs,
                        &method.sig,
                        &method.block,
                        type_module.clone(),
                        Some(&type_module),
                    );
                }
                _ => {}
            }
        }
    }

    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        if self.active_only
            && !crate::verification::target::module_active_for_test(&item.attrs).unwrap_or(false)
        {
            return;
        }
        let Some((_, items)) = &item.content else {
            return;
        };
        let mut nested_imports = collect_item_imports(items);
        nested_imports.inherit_parent_macros(&self.imports);
        if nested_imports.inherits_parent_glob() {
            nested_imports.inherit(&self.imports);
        }
        let previous_imports = std::mem::replace(&mut self.imports, nested_imports);
        self.module.push(item.ident.to_string());
        for item in items {
            self.visit_item(item);
        }
        self.module.pop();
        self.imports = previous_imports;
    }

    fn visit_item_const(&mut self, item: &'ast ItemConst) {
        self.functions.values.insert(FunctionId {
            module: self.module.clone(),
            name: item.ident.to_string(),
        });
    }

    fn visit_item_static(&mut self, item: &'ast ItemStatic) {
        self.functions.values.insert(FunctionId {
            module: self.module.clone(),
            name: item.ident.to_string(),
        });
    }
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

    fn record_call(&mut self, target: CallTarget, arguments: Vec<CallableArgument>) {
        if target.matches_any_name(FORBIDDEN_CALLS) {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
        }
        let call = FunctionCall { target, arguments };
        self.facts.events.push(FunctionEvent::Call {
            call,
            guaranteed: self.guaranteed && !self.statement.may_exit,
        });
    }

    fn callable_arguments<'ast>(
        &self,
        arguments: impl IntoIterator<Item = &'ast Expr>,
    ) -> Vec<CallableArgument> {
        arguments
            .into_iter()
            .map(|argument| self.callable_argument(argument))
            .collect()
    }

    fn callable_argument(&self, argument: &Expr) -> CallableArgument {
        match argument {
            Expr::Closure(_) => CallableArgument::InlineClosure,
            Expr::Group(group) => self.callable_argument(&group.expr),
            Expr::Paren(paren) => self.callable_argument(&paren.expr),
            Expr::Reference(reference) => self.callable_argument(&reference.expr),
            Expr::Path(_) => {
                if let Some(index) = unqualified_expression_name(argument)
                    .and_then(|name| self.parameter_bindings.get(&name))
                {
                    return CallableArgument::Parameter(*index);
                }
                self.alias_target(argument)
                    .map_or(CallableArgument::Opaque, CallableArgument::Known)
            }
            _ => self
                .alias_target(argument)
                .map_or(CallableArgument::Opaque, CallableArgument::Known),
        }
    }

    fn record_potential_callable_arguments<'ast>(
        &mut self,
        arguments: impl IntoIterator<Item = &'ast Expr>,
    ) {
        let targets = arguments
            .into_iter()
            .filter_map(|argument| self.alias_target(argument))
            .collect::<Vec<_>>();
        self.facts.potential_callable_arguments.extend(targets);
    }

    fn call_target(&self, call: &ExprCall) -> Option<CallTarget> {
        if let Some(name) = unqualified_called_function(call) {
            if let Some(alias) = self.value_aliases.get(&name).cloned() {
                return Some(alias);
            }
            if self.value_bindings.contains(&name) {
                return None;
            }
        }
        self.expression_target(&call.func)
    }

    fn expression_target(&self, expression: &Expr) -> Option<CallTarget> {
        if let Some(alias) = unqualified_expression_name(expression)
            .and_then(|name| self.value_aliases.get(&name).cloned())
        {
            return Some(alias);
        }
        if let Some(target) = self.resolver.self_target(expression, self.self_type) {
            return Some(target);
        }
        if let Some(target) = self
            .scoped_resolver
            .explicit_target(expression, self.module)
        {
            return Some(self.resolver.classify_target(target));
        }
        let target = match (
            self.resolver.call_target(expression, self.module),
            self.scoped_resolver.call_target(expression, self.module),
        ) {
            (Some(module), Some(scoped)) => module.merge(scoped),
            (Some(target), None) | (None, Some(target)) => target,
            (None, None) => return None,
        };
        Some(self.resolver.classify_target(target))
    }

    fn alias_target(&self, expression: &Expr) -> Option<CallTarget> {
        let target = self.expression_target(expression)?;
        (self.resolver.can_name_reviewed_function(&target)
            || target.matches_any_name(FORBIDDEN_CALLS))
        .then_some(target)
    }

    fn inferred_value_type(&self, expression: &Expr) -> Option<Vec<String>> {
        let candidates = [
            self.resolver
                .value_expression_type_module(expression, self.module),
            self.scoped_resolver
                .value_expression_type_module(expression, self.module),
        ]
        .into_iter()
        .flatten()
        .collect::<BTreeSet<_>>();
        match candidates.into_iter().collect::<Vec<_>>().as_slice() {
            [candidate] => Some(candidate.clone()),
            _ => None,
        }
    }

    fn record_invocation(&mut self, kind: InvocationKind, target: CallTarget) {
        if target.matches_any_name(FORBIDDEN_CALLS) {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
        }
        let invocation = InvocationCall { kind, target };
        self.facts.events.push(FunctionEvent::Invocation {
            invocation,
            guaranteed: self.guaranteed && !self.statement.may_exit,
        });
    }

    fn reject_forbidden_macro(&mut self, name: &str) -> bool {
        if name == "panic" || name == "unreachable" {
            self.statement.may_exit = true;
        }
        if name == "oracle_detector_witness" || name == "oracle_fabricated_detector_witness" {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
            return true;
        }
        if matches!(
            name,
            "eprint" | "eprintln" | "print" | "println" | "write" | "writeln" | "dbg"
        ) {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
            return true;
        }
        if self.imports.local_macros.contains(name) {
            self.facts.defects.insert(SourceDefect::OpaqueMacro);
            return true;
        }
        false
    }

    fn visit_invocation_macro(
        &mut self,
        name: &str,
        invocation: &Macro,
        arguments: Option<&[Expr]>,
    ) -> bool {
        if !INVOCATION_MACROS.contains(&name) {
            return false;
        }
        if !trusted_oracle_macro(&invocation.path, &self.imports) {
            self.facts
                .defects
                .insert(SourceDefect::UntrustedOracleMacro);
            return true;
        }
        let Some(arguments) = arguments else {
            self.facts
                .defects
                .insert(SourceDefect::MalformedInvocationMacro);
            return true;
        };
        let expected_arguments = if name == "oracle_expect_err" { 2 } else { 1 };
        if arguments.len() != expected_arguments {
            self.facts
                .defects
                .insert(SourceDefect::MalformedInvocationMacro);
            return true;
        }
        let Some(Expr::Call(call)) = arguments.first() else {
            self.facts
                .defects
                .insert(SourceDefect::MalformedInvocationMacro);
            return true;
        };
        if !matches!(call.func.as_ref(), Expr::Path(path)
            if path.qself.is_none()
                && path.path.leading_colon.is_none()
                && path.path.segments.len() == 1)
        {
            self.facts
                .defects
                .insert(SourceDefect::MalformedInvocationMacro);
            return true;
        }
        let Some(target) = self.call_target(call) else {
            self.facts
                .defects
                .insert(SourceDefect::MalformedInvocationMacro);
            return true;
        };
        let kind = if name == "oracle_expect_err" {
            InvocationKind::ExpectErr
        } else {
            InvocationKind::Recorder
        };
        for argument in arguments.iter().skip(1) {
            self.visit_expr(argument);
        }
        self.visit_expr(&call.func);
        for argument in &call.args {
            self.visit_expr(argument);
        }
        self.record_invocation(kind, target);
        true
    }

    fn visit_regular_macro(&mut self, name: &str, invocation: &Macro, arguments: Option<&[Expr]>) {
        if ORACLE_MACROS.contains(&name) {
            if !trusted_oracle_macro(&invocation.path, &self.imports) {
                self.facts
                    .defects
                    .insert(SourceDefect::UntrustedOracleMacro);
                return;
            }
        } else if !safe_builtin_macro(&invocation.path, &self.imports) {
            self.facts.defects.insert(SourceDefect::OpaqueMacro);
            return;
        }
        if TOKEN_ONLY_MACROS.contains(&name) {
            return;
        }
        let Some(arguments) = arguments else {
            self.facts.defects.insert(SourceDefect::OpaqueMacro);
            return;
        };
        self.with_guarantee(false, |visitor| {
            for argument in arguments {
                visitor.visit_expr(argument);
            }
        });
    }
}

impl<'ast> Visit<'ast> for FunctionBodyVisitor<'_> {
    fn visit_block(&mut self, block: &'ast Block) {
        let previous_guarantee = self.guaranteed;
        let previous_reachability = self.reachability;
        let previous_may_exit = self.statement.may_exit;
        let previous_may_diverge = self.statement.may_diverge;
        let previous_aliases = self.value_aliases.clone();
        let previous_bindings = self.value_bindings.clone();
        let previous_types = self.value_types.clone();
        let previous_scoped_resolver = self.scoped_resolver.clone();
        let previous_imports = self.imports.clone();
        for statement in &block.stmts {
            if let Stmt::Item(item) = statement {
                match item {
                    syn::Item::Use(item_use) => {
                        match crate::verification::target::module_active_for_test(&item_use.attrs) {
                            Ok(true) => self.scoped_resolver.add_use(item_use, self.module),
                            Ok(false) => continue,
                            Err(_) => {
                                self.facts.conditional_compilation = true;
                                self.facts.defects.insert(SourceDefect::OpaqueCallable);
                                continue;
                            }
                        }
                    }
                    syn::Item::Fn(function) => {
                        let name = function.sig.ident.to_string();
                        self.value_bindings.insert(name.clone());
                        self.facts.shadowed_values.insert(name);
                    }
                    _ => {}
                }
                self.imports.add_item(item);
            }
        }
        let mut guaranteed_reachable = previous_guarantee;
        let mut potentially_reachable = previous_reachability.is_reachable();
        let mut block_may_exit = false;
        let mut block_may_diverge = false;
        for statement in &block.stmts {
            self.guaranteed = guaranteed_reachable;
            self.reachability = potentially_reachable.into();
            self.statement.may_exit = false;
            self.statement.may_diverge = false;
            self.visit_stmt(statement);
            let statement_may_exit =
                self.statement.may_exit || statement_unconditionally_exits(statement);
            let statement_may_diverge = self.statement.may_diverge;
            block_may_exit |= statement_may_exit;
            block_may_diverge |= statement_may_diverge;
            if guaranteed_reachable && statement_may_exit {
                guaranteed_reachable = false;
            }
            if potentially_reachable && !statement_may_complete_normally(statement) {
                potentially_reachable = false;
            }
        }
        let updated_aliases = self.value_aliases.clone();
        self.value_aliases = previous_aliases;
        for binding in &previous_bindings {
            if let Some(alias) = updated_aliases.get(binding) {
                self.value_aliases.insert(binding.clone(), alias.clone());
            } else {
                self.value_aliases.remove(binding);
            }
        }
        self.value_bindings = previous_bindings;
        self.value_types = previous_types;
        self.scoped_resolver = previous_scoped_resolver;
        self.imports = previous_imports;
        self.guaranteed = previous_guarantee;
        self.reachability = previous_reachability;
        self.statement.may_exit = previous_may_exit || block_may_exit;
        self.statement.may_diverge = previous_may_diverge || block_may_diverge;
        self.function.may_diverge |= block_may_diverge;
    }

    fn visit_expr_call(&mut self, call: &'ast ExprCall) {
        let target = self.call_target(call);
        let arguments = self.callable_arguments(&call.args);
        self.record_potential_callable_arguments(&call.args);
        self.visit_expr(&call.func);
        for argument in &call.args {
            self.visit_expr(argument);
        }
        if let Some(target) = target {
            self.record_call(target, arguments);
        } else if let Some(index) =
            unqualified_called_function(call).and_then(|name| self.parameter_bindings.get(&name))
        {
            if self.guaranteed && !self.statement.may_exit {
                self.facts.guaranteed_called_parameters.insert(*index);
            } else {
                self.facts.conditional_called_parameters.insert(*index);
            }
        } else {
            self.facts.defects.insert(SourceDefect::OpaqueCallable);
        }
    }

    fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
        let receiver_type = unqualified_expression_name(&call.receiver).and_then(|name| {
            if name == "self" {
                self.self_type
            } else {
                self.value_types.get(&name).map(Vec::as_slice)
            }
        });
        let receiver_type = receiver_type
            .map(<[String]>::to_vec)
            .or_else(|| {
                self.resolver.field_expression_type_module(
                    &call.receiver,
                    self.module,
                    self.self_type,
                    &self.value_types,
                )
            })
            .or_else(|| {
                self.scoped_resolver.field_expression_type_module(
                    &call.receiver,
                    self.module,
                    self.self_type,
                    &self.value_types,
                )
            });
        let module_target = self.resolver.method_target(
            &call.receiver,
            &call.method.to_string(),
            self.module,
            receiver_type.as_deref(),
        );
        let scoped_target = self.scoped_resolver.method_target(
            &call.receiver,
            &call.method.to_string(),
            self.module,
            receiver_type.as_deref(),
        );
        let target = self
            .resolver
            .classify_target(module_target.merge(scoped_target));
        let arguments = self.callable_arguments(&call.args);
        self.record_potential_callable_arguments(&call.args);
        self.visit_expr(&call.receiver);
        for argument in &call.args {
            self.visit_expr(argument);
        }
        self.record_call(target, arguments);
    }

    fn visit_local(&mut self, local: &'ast Local) {
        let binding = match &local.pat {
            Pat::Ident(pattern) if pattern.subpat.is_none() => Some(pattern.ident.to_string()),
            _ => None,
        };
        let alias = local
            .init
            .as_ref()
            .and_then(|init| self.alias_target(&init.expr));
        let inferred_type = local
            .init
            .as_ref()
            .and_then(|init| self.inferred_value_type(&init.expr));
        for attribute in &local.attrs {
            self.visit_attribute(attribute);
        }
        if let Some(init) = &local.init {
            self.visit_expr(&init.expr);
            if let Some((_, diverge)) = &init.diverge {
                self.with_guarantee(false, |visitor| visitor.visit_expr(diverge));
                self.statement.may_exit = true;
            }
        }
        self.visit_pat(&local.pat);
        if let Some(binding) = binding {
            self.value_bindings.insert(binding.clone());
            if let Some(inferred_type) = inferred_type {
                self.value_types.insert(binding.clone(), inferred_type);
            } else {
                self.value_types.remove(&binding);
            }
            if let Some(alias) = alias {
                self.value_aliases.insert(binding, alias);
            } else {
                self.value_aliases.remove(&binding);
            }
        }
    }

    fn visit_expr_assign(&mut self, expression: &'ast ExprAssign) {
        self.visit_expr(&expression.right);
        if let Some(binding) = unqualified_expression_name(&expression.left) {
            if let Some(alias) = self.alias_target(&expression.right) {
                if self.guaranteed {
                    self.value_aliases.insert(binding, alias);
                } else {
                    self.value_aliases.remove(&binding);
                    self.facts.defects.insert(SourceDefect::OpaqueCallable);
                }
            } else {
                self.value_aliases.remove(&binding);
            }
        } else {
            self.visit_expr(&expression.left);
        }
    }

    fn visit_macro(&mut self, invocation: &'ast Macro) {
        let name = invocation
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_default();
        if self.reject_forbidden_macro(&name) {
            return;
        }
        let arguments = macro_args::expressions(&name, invocation);
        if self.visit_invocation_macro(&name, invocation, arguments.as_deref()) {
            return;
        }
        self.visit_regular_macro(&name, invocation, arguments.as_deref());
    }

    fn visit_pat_ident(&mut self, pattern: &'ast PatIdent) {
        let name = pattern.ident.to_string();
        self.value_bindings.insert(name.clone());
        self.facts.shadowed_values.insert(name);
        visit::visit_pat_ident(self, pattern);
    }

    fn visit_pat_type(&mut self, pattern: &'ast PatType) {
        if let Pat::Ident(binding) = pattern.pat.as_ref() {
            if let Some(module) = self.resolver.value_type_module(&pattern.ty, self.module) {
                self.value_types.insert(binding.ident.to_string(), module);
            }
        }
        visit::visit_pat_type(self, pattern);
    }

    fn visit_item_const(&mut self, item: &'ast ItemConst) {
        self.facts.shadowed_values.insert(item.ident.to_string());
        visit::visit_item_const(self, item);
    }

    fn visit_item_static(&mut self, item: &'ast ItemStatic) {
        self.facts.shadowed_values.insert(item.ident.to_string());
        visit::visit_item_static(self, item);
    }

    fn visit_expr_try(&mut self, expression: &'ast ExprTry) {
        self.visit_expr(&expression.expr);
        self.statement.may_exit = true;
    }

    fn visit_expr_return(&mut self, expression: &'ast ExprReturn) {
        if let Some(value) = &expression.expr {
            self.visit_expr(value);
        }
        self.function.normal_return_seen |=
            self.reachability.is_reachable() && !self.statement.may_exit;
        self.statement.may_exit = true;
    }

    fn visit_expr_break(&mut self, expression: &'ast ExprBreak) {
        if let Some(value) = &expression.expr {
            self.visit_expr(value);
        }
        self.statement.may_exit = true;
    }

    fn visit_expr_continue(&mut self, _expression: &'ast ExprContinue) {
        self.statement.may_exit = true;
    }

    fn visit_expr_if(&mut self, expression: &'ast ExprIf) {
        self.visit_expr(&expression.cond);
        self.with_guarantee(false, |visitor| {
            visitor.visit_block(&expression.then_branch);
        });
        if let Some((_, otherwise)) = &expression.else_branch {
            self.with_guarantee(false, |visitor| visitor.visit_expr(otherwise));
        }
    }

    fn visit_expr_match(&mut self, expression: &'ast ExprMatch) {
        self.visit_expr(&expression.expr);
        for arm in &expression.arms {
            self.visit_pat(&arm.pat);
            self.with_guarantee(false, |visitor| {
                if let Some((_, guard)) = &arm.guard {
                    visitor.visit_expr(guard);
                }
                visitor.visit_expr(&arm.body);
            });
        }
    }

    fn visit_expr_for_loop(&mut self, expression: &'ast ExprForLoop) {
        self.visit_pat(&expression.pat);
        self.visit_expr(&expression.expr);
        self.with_guarantee(false, |visitor| visitor.visit_block(&expression.body));
    }

    fn visit_expr_while(&mut self, expression: &'ast ExprWhile) {
        self.with_guarantee(false, |visitor| {
            visitor.visit_expr(&expression.cond);
            visitor.visit_block(&expression.body);
        });
    }

    fn visit_expr_loop(&mut self, expression: &'ast ExprLoop) {
        let previous_may_exit = self.statement.may_exit;
        let previous_may_diverge = self.statement.may_diverge;
        self.statement.may_exit = false;
        self.statement.may_diverge = false;
        self.with_guarantee(false, |visitor| visitor.visit_block(&expression.body));
        let loop_may_complete = loop_may_complete_normally(expression);
        self.statement.may_exit = previous_may_exit || !loop_may_complete;
        self.statement.may_diverge =
            previous_may_diverge || self.statement.may_diverge || !loop_may_complete;
    }

    fn visit_expr_closure(&mut self, expression: &'ast ExprClosure) {
        let previous_may_exit = self.statement.may_exit;
        let previous_may_diverge = self.statement.may_diverge;
        let previous_function_may_diverge = self.function.may_diverge;
        let previous_return_seen = self.function.normal_return_seen;
        let previous_reachability = self.reachability;
        let previous_aliases = self.value_aliases.clone();
        let previous_bindings = self.value_bindings.clone();
        let previous_types = self.value_types.clone();
        self.statement.may_exit = false;
        self.statement.may_diverge = false;
        self.function.may_diverge = false;
        self.function.normal_return_seen = false;
        self.reachability = PathReachability::Reachable;
        for input in &expression.inputs {
            self.visit_pat(input);
        }
        self.with_guarantee(false, |visitor| visitor.visit_expr(&expression.body));
        self.value_aliases = previous_aliases;
        self.value_bindings = previous_bindings;
        self.value_types = previous_types;
        self.statement.may_exit = previous_may_exit;
        self.statement.may_diverge = previous_may_diverge;
        self.function.may_diverge = previous_function_may_diverge;
        self.function.normal_return_seen = previous_return_seen;
        self.reachability = previous_reachability;
    }

    fn visit_expr_async(&mut self, expression: &'ast ExprAsync) {
        self.with_guarantee(false, |visitor| visitor.visit_block(&expression.block));
    }

    fn visit_expr_binary(&mut self, expression: &'ast ExprBinary) {
        self.visit_expr(&expression.left);
        if matches!(expression.op, BinOp::And(_) | BinOp::Or(_)) {
            self.with_guarantee(false, |visitor| visitor.visit_expr(&expression.right));
        } else {
            self.visit_expr(&expression.right);
        }
    }

    fn visit_item_fn(&mut self, _function: &'ast ItemFn) {}

    fn visit_item_macro(&mut self, item: &'ast syn::ItemMacro) {
        self.imports.add_macro_declaration(item);
    }
}

#[cfg(test)]
#[path = "detector_source_tests.rs"]
pub(super) mod tests;

#[cfg(test)]
#[path = "detector_source_adversarial_tests.rs"]
mod adversarial_tests;

#[cfg(test)]
#[path = "detector_source_module_graph_tests.rs"]
mod module_graph_tests;
