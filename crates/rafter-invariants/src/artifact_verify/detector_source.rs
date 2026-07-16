use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use syn::{
    parse::{Parse, ParseStream, Parser},
    punctuated::Punctuated,
    visit::{self, Visit},
    BinOp, Block, Expr, ExprAsync, ExprBinary, ExprBreak, ExprCall, ExprClosure, ExprContinue,
    ExprForLoop, ExprIf, ExprLoop, ExprMatch, ExprMethodCall, ExprReturn, ExprTry, ExprWhile, File,
    ItemConst, ItemFn, ItemMod, ItemStatic, Macro, Pat, PatIdent, Stmt, Token,
};

mod imports;

use imports::{
    collect_imports, safe_builtin_macro, trusted_oracle_macro, validate_oracle_provenance,
    verify_detector_resolution, ImportedPaths,
};

const INVOCATION_MACROS: &[&str] = &["oracle_expect_err", "oracle_invoke_recorder"];
const ORACLE_MACROS: &[&str] = &[
    "oracle_assert",
    "oracle_assert_eq",
    "oracle_assert_ne",
    "oracle_expect_err",
    "oracle_invoke_recorder",
    "oracle_prop_assert",
    "oracle_prop_assert_eq",
    "oracle_violation",
];
const SAFE_BUILTIN_MACROS: &[&str] = &[
    "assert",
    "assert_eq",
    "assert_ne",
    "cfg",
    "concat",
    "debug_assert",
    "debug_assert_eq",
    "debug_assert_ne",
    "file",
    "format",
    "format_args",
    "line",
    "matches",
    "module_path",
    "panic",
    "stringify",
    "vec",
];
const TOKEN_ONLY_MACROS: &[&str] = &["cfg", "concat", "file", "line", "module_path", "stringify"];
const FORBIDDEN_WITNESS_HELPERS: &[&str] = &[
    "__oracle_detector_witness",
    "__oracle_fabricated_detector_witness",
];

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

pub(super) fn verify_invocation_bound_detector(
    binding: &crate::DetectorFixtureSourceBinding<'_>,
) -> Result<DetectorInvocationContract, String> {
    let fixture_source = binding.fixture_source;
    let fixture_path = binding.fixture_path;
    let detector_path = binding.detector_path;
    let fixture = binding.fixture;
    let detector = binding.detector;
    require_bound_source(fixture_path, fixture_source, "fixture")?;
    require_bound_source(detector_path, binding.detector_source, "detector")?;
    let fixture_file = syn::parse_file(fixture_source)
        .map_err(|error| format!("parse registered fixture source: {error}"))?;
    let detector_file = syn::parse_file(binding.detector_source)
        .map_err(|error| format!("parse registered detector source: {error}"))?;
    let imports = collect_imports(&fixture_file);
    validate_oracle_provenance(&imports)?;
    let fixture_functions = collect_functions(&fixture_file, detector, &imports);
    require_registered_fixture(&fixture_functions, fixture)?;
    let target = bind_target_detector(binding, &fixture_functions, &detector_file, &imports)?;
    let declarations = target.declarations;

    let mut contract = DetectorInvocationContract {
        witnesses: BTreeMap::new(),
        registered_identity: target.registered_identity,
    };
    let mut stack = Vec::new();
    expand_reachable_function(
        &fixture_functions,
        fixture,
        detector,
        true,
        &declarations,
        &mut contract,
        &mut stack,
    )?;
    if !contract.witnesses.keys().any(|witness| {
        witness
            .split_once(':')
            .is_some_and(|(_, identity)| identity == contract.registered_identity)
    }) {
        return Err(format!(
            "negative fixture `{fixture}` does not invoke registered detector `{detector}` through an invocation-bound oracle macro"
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

struct TargetDetectorContract {
    declarations: BTreeMap<String, Vec<String>>,
    registered_identity: String,
}

fn bind_target_detector(
    binding: &crate::DetectorFixtureSourceBinding<'_>,
    fixture_functions: &FunctionIndex,
    detector_file: &File,
    imports: &ImportedPaths,
) -> Result<TargetDetectorContract, String> {
    let target_graph =
        crate::rust_target::target_source_graph(binding.source_root, binding.test_identity)?;
    let fixture_module = target_graph.source_module(binding.fixture_path)?;
    if binding.test_identity.test_name.rsplit("::").next() != Some(binding.fixture) {
        return Err(format!(
            "registered test identity `{}` does not name fixture `{}`",
            binding.test_identity.test_name, binding.fixture
        ));
    }
    target_graph
        .require_declaration_source(&binding.test_identity.test_name, binding.fixture_path)?;
    let same_source = binding.fixture_path == binding.detector_path;
    let detector_module_result = if same_source {
        Ok(fixture_module.clone())
    } else {
        target_graph.source_module(binding.detector_path)
    };
    let detector_functions = if same_source {
        FunctionIndex::default()
    } else {
        collect_functions(detector_file, binding.detector, &ImportedPaths::default())
    };
    let local_count = fixture_functions.count(binding.detector);
    let external_count = usize::from(!same_source) * detector_functions.count(binding.detector);
    if local_count == 0 {
        detector_module_result.as_ref().map_err(|error| {
            format!(
                "resolve bound detector source {} in registered Cargo target: {error}",
                binding.detector_path.display()
            )
        })?;
    }
    require_single_detector_declaration(binding.detector, local_count + external_count)?;
    let detector_facts = if local_count == 1 {
        fixture_functions.unique(binding.detector)?
    } else {
        detector_functions.unique(binding.detector)?
    }
    .ok_or_else(|| {
        format!(
            "registered detector `{}` has no bound declaration facts",
            binding.detector
        )
    })?;
    if detector_facts.conditional_compilation || detector_facts.untrusted_attributes {
        return Err(format!(
            "registered detector `{}` has conditional or untrusted semantic attributes",
            binding.detector
        ));
    }
    let detector_module = detector_module_result.as_ref().ok();
    verify_detector_resolution(
        imports,
        &fixture_module.module,
        detector_module.map(|module| module.module.as_slice()),
        binding.detector,
        local_count == 1,
    )?;
    let registered_module = if local_count == 1 {
        &fixture_module
    } else {
        detector_module.ok_or_else(|| {
            format!(
                "registered detector `{}` source is outside its Cargo target",
                binding.detector
            )
        })?
    };
    let registered_identity = compiler_identity(registered_module, binding.detector);
    let declarations = target_graph.declaration_identities();
    if !declarations
        .get(binding.detector)
        .is_some_and(|identities| identities.contains(&registered_identity))
    {
        return Err(format!(
            "registered detector `{}` has no declaration at `{registered_identity}`",
            binding.detector
        ));
    }
    Ok(TargetDetectorContract {
        declarations,
        registered_identity,
    })
}

fn require_single_detector_declaration(detector: &str, count: usize) -> Result<(), String> {
    match count {
        0 => Err(format!(
            "registered detector `{detector}` has no function declaration in its bound source paths"
        )),
        1 => Ok(()),
        count => Err(format!(
            "registered detector `{detector}` has {count} ambiguous declarations in its bound source paths"
        )),
    }
}

fn compiler_identity(module: &crate::rust_target::SourceModule, function: &str) -> String {
    std::iter::once(module.crate_name.clone())
        .chain(module.module.iter().cloned())
        .chain(std::iter::once(function.to_owned()))
        .collect::<Vec<_>>()
        .join("::")
}

fn require_registered_fixture(functions: &FunctionIndex, fixture: &str) -> Result<(), String> {
    let facts = functions
        .unique(fixture)
        .map_err(|error| format!("registered negative fixture `{fixture}` {error}"))?
        .ok_or_else(|| format!("registered negative fixture `{fixture}` has no declaration"))?;
    if facts.detector_test_attributes != 1 {
        return Err(format!(
            "registered negative fixture `{fixture}` must have exactly one #[rafter_invariant_test::detector_test] attribute"
        ));
    }
    if facts.conditional_compilation {
        return Err(format!(
            "registered negative fixture `{fixture}` has conditional compilation attributes"
        ));
    }
    if facts.untrusted_attributes {
        return Err(format!(
            "registered negative fixture `{fixture}` has an untrusted semantic attribute"
        ));
    }
    Ok(())
}

fn require_bound_source(path: &Path, expected: &str, label: &str) -> Result<(), String> {
    let actual = std::fs::read_to_string(path)
        .map_err(|error| format!("read bound {label} source {}: {error}", path.display()))?;
    if actual != expected {
        return Err(format!(
            "provided {label} source does not match bound path {}",
            path.display()
        ));
    }
    Ok(())
}

fn expand_reachable_function(
    functions: &FunctionIndex,
    function: &str,
    detector: &str,
    guaranteed_path: bool,
    declarations: &BTreeMap<String, Vec<String>>,
    contract: &mut DetectorInvocationContract,
    stack: &mut Vec<String>,
) -> Result<(), String> {
    let Some(facts) = functions.unique(function)? else {
        return Ok(());
    };
    if stack.iter().any(|active| active == function) {
        return Err(format!(
            "negative fixture call graph is recursive through `{function}`"
        ));
    }
    if facts.conditional_compilation || facts.untrusted_attributes {
        return Err(format!(
            "negative fixture reaches conditional or untrusted semantic attributes through `{function}`"
        ));
    }
    if facts.defects.contains(&SourceDefect::ForbiddenWitness) {
        return Err(format!(
            "negative fixture can emit an arbitrary detector witness through `{function}`"
        ));
    }
    if facts.defects.contains(&SourceDefect::UntrustedOracleMacro) {
        return Err(format!(
            "negative fixture invokes an untrusted oracle macro through `{function}`"
        ));
    }
    if facts
        .defects
        .contains(&SourceDefect::MalformedInvocationMacro)
    {
        return Err(format!(
            "negative fixture has a malformed invocation-bound oracle macro through `{function}`"
        ));
    }
    if facts.defects.contains(&SourceDefect::OpaqueMacro) {
        return Err(format!(
            "negative fixture reaches an opaque macro through `{function}`"
        ));
    }
    if facts.defects.contains(&SourceDefect::ShadowedDetector) {
        return Err(format!(
            "negative fixture shadows registered detector `{detector}` through `{function}`"
        ));
    }
    if !facts.conditional_invocations.is_empty()
        || !guaranteed_path && !facts.guaranteed_invocations.is_empty()
    {
        return Err(format!(
            "negative fixture reaches an invocation-bound oracle macro only through conditional control flow in `{function}`"
        ));
    }

    if guaranteed_path {
        for invocation in &facts.guaranteed_invocations {
            let identity = if invocation.function == detector {
                contract.registered_identity.clone()
            } else {
                unique_declaration_identity(declarations, &invocation.function)?
            };
            *contract
                .witnesses
                .entry(format!("{}:{identity}", invocation.kind.label()))
                .or_default() += 1;
        }
    }

    stack.push(function.to_owned());
    for called in &facts.guaranteed_calls {
        if functions.contains(called) {
            expand_reachable_function(
                functions,
                called,
                detector,
                guaranteed_path,
                declarations,
                contract,
                stack,
            )?;
        }
    }
    for called in &facts.conditional_calls {
        if functions.contains(called) {
            expand_reachable_function(
                functions,
                called,
                detector,
                false,
                declarations,
                contract,
                stack,
            )?;
        }
    }
    stack.pop();
    Ok(())
}

#[derive(Default)]
struct FunctionIndex {
    functions: BTreeMap<String, Vec<FunctionFacts>>,
}

impl FunctionIndex {
    fn count(&self, name: &str) -> usize {
        self.functions.get(name).map_or(0, Vec::len)
    }

    fn contains(&self, name: &str) -> bool {
        self.functions.contains_key(name)
    }

    fn unique(&self, name: &str) -> Result<Option<&FunctionFacts>, String> {
        match self.functions.get(name).map(Vec::as_slice) {
            None => Ok(None),
            Some([function]) => Ok(Some(function)),
            Some(functions) => Err(format!(
                "resolves to {} same-named function declarations",
                functions.len()
            )),
        }
    }
}

fn unique_declaration_identity(
    declarations: &BTreeMap<String, Vec<String>>,
    function: &str,
) -> Result<String, String> {
    let identities = declarations
        .get(function)
        .map(Vec::as_slice)
        .unwrap_or_default();
    let [identity] = identities else {
        return Err(format!(
            "invocation-bound function `{function}` resolves to {} bound source declarations",
            identities.len()
        ));
    };
    Ok(identity.clone())
}

#[derive(Default)]
struct FunctionFacts {
    detector_test_attributes: usize,
    conditional_compilation: bool,
    untrusted_attributes: bool,
    guaranteed_calls: Vec<String>,
    conditional_calls: Vec<String>,
    guaranteed_invocations: Vec<InvocationCall>,
    conditional_invocations: Vec<InvocationCall>,
    defects: BTreeSet<SourceDefect>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum SourceDefect {
    ForbiddenWitness,
    MalformedInvocationMacro,
    OpaqueMacro,
    ShadowedDetector,
    UntrustedOracleMacro,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum InvocationKind {
    ExpectErr,
    Recorder,
}

impl InvocationKind {
    const fn label(self) -> &'static str {
        match self {
            Self::ExpectErr => "expect-err",
            Self::Recorder => "recorder",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InvocationCall {
    kind: InvocationKind,
    function: String,
}

fn collect_functions(file: &File, detector: &str, imports: &ImportedPaths) -> FunctionIndex {
    let mut collector = FunctionCollector {
        detector,
        imports,
        functions: FunctionIndex::default(),
    };
    collector.visit_file(file);
    collector.functions
}

struct FunctionCollector<'a> {
    detector: &'a str,
    imports: &'a ImportedPaths,
    functions: FunctionIndex,
}

impl<'ast> Visit<'ast> for FunctionCollector<'_> {
    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        let mut visitor = FunctionBodyVisitor {
            detector: self.detector,
            imports: self.imports,
            guaranteed: true,
            statement_may_exit: false,
            facts: FunctionFacts {
                detector_test_attributes: function
                    .attrs
                    .iter()
                    .filter(|attribute| is_detector_test_attribute(attribute))
                    .count(),
                conditional_compilation: function.attrs.iter().any(|attribute| {
                    attribute.path().is_ident("cfg") || attribute.path().is_ident("cfg_attr")
                }),
                untrusted_attributes: function.attrs.iter().any(|attribute| {
                    !is_detector_test_attribute(attribute) && !attribute.path().is_ident("ignore")
                }),
                ..FunctionFacts::default()
            },
        };
        visitor.visit_signature(&function.sig);
        visitor.visit_block(&function.block);
        self.functions
            .functions
            .entry(function.sig.ident.to_string())
            .or_default()
            .push(visitor.facts);
        visit::visit_item_fn(self, function);
    }

    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        let Some((_, items)) = &item.content else {
            return;
        };
        for item in items {
            self.visit_item(item);
        }
    }
}

fn is_detector_test_attribute(attribute: &syn::Attribute) -> bool {
    attribute
        .path()
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .eq(["rafter_invariant_test", "detector_test"].map(str::to_owned))
}

struct FunctionBodyVisitor<'a> {
    detector: &'a str,
    imports: &'a ImportedPaths,
    guaranteed: bool,
    statement_may_exit: bool,
    facts: FunctionFacts,
}

impl FunctionBodyVisitor<'_> {
    fn with_guarantee(&mut self, guaranteed: bool, visit: impl FnOnce(&mut Self)) {
        let previous = self.guaranteed;
        self.guaranteed = previous && guaranteed;
        visit(self);
        self.guaranteed = previous;
    }

    fn record_call(&mut self, called: String) {
        if FORBIDDEN_WITNESS_HELPERS.contains(&called.as_str()) {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
        }
        if self.guaranteed {
            self.facts.guaranteed_calls.push(called);
        } else {
            self.facts.conditional_calls.push(called);
        }
    }

    fn record_invocation(&mut self, kind: InvocationKind, called: String) {
        let invocation = InvocationCall {
            kind,
            function: called,
        };
        if self.guaranteed {
            self.facts.guaranteed_invocations.push(invocation);
        } else {
            self.facts.conditional_invocations.push(invocation);
        }
    }
}

impl<'ast> Visit<'ast> for FunctionBodyVisitor<'_> {
    fn visit_block(&mut self, block: &'ast Block) {
        let previous_guarantee = self.guaranteed;
        let previous_may_exit = self.statement_may_exit;
        let mut reachable = previous_guarantee;
        let mut block_may_exit = false;
        for statement in &block.stmts {
            self.guaranteed = reachable;
            self.statement_may_exit = false;
            self.visit_stmt(statement);
            let statement_may_exit =
                self.statement_may_exit || statement_unconditionally_exits(statement);
            block_may_exit |= statement_may_exit;
            if reachable && statement_may_exit {
                reachable = false;
            }
        }
        self.guaranteed = previous_guarantee;
        self.statement_may_exit = previous_may_exit || block_may_exit;
    }

    fn visit_expr_call(&mut self, call: &'ast ExprCall) {
        if let Some(called) = called_function_leaf(call) {
            if FORBIDDEN_WITNESS_HELPERS.contains(&called.as_str())
                || matches!(
                    called.as_str(),
                    "exit" | "_exit" | "abort" | "write" | "write_all" | "write_fmt"
                )
            {
                self.facts.defects.insert(SourceDefect::ForbiddenWitness);
            }
        }
        if let Some(called) = unqualified_called_function(call) {
            self.record_call(called);
        }
        for argument in &call.args {
            self.visit_expr(argument);
        }
    }

    fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
        if matches!(
            call.method.to_string().as_str(),
            "write" | "write_all" | "write_fmt"
        ) {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
        }
        visit::visit_expr_method_call(self, call);
    }

    fn visit_macro(&mut self, invocation: &'ast Macro) {
        let name = invocation
            .path
            .segments
            .last()
            .map(|segment| segment.ident.to_string())
            .unwrap_or_default();
        if name == "oracle_detector_witness" || name == "oracle_fabricated_detector_witness" {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
            return;
        }
        if matches!(
            name.as_str(),
            "eprint" | "eprintln" | "print" | "println" | "write" | "writeln" | "dbg"
        ) {
            self.facts.defects.insert(SourceDefect::ForbiddenWitness);
            return;
        }
        if self.imports.local_macros.contains(&name) {
            self.facts.defects.insert(SourceDefect::OpaqueMacro);
            return;
        }
        let arguments = macro_expressions(&name, invocation);
        if INVOCATION_MACROS.contains(&name.as_str()) {
            if !trusted_oracle_macro(&invocation.path, self.imports) {
                self.facts
                    .defects
                    .insert(SourceDefect::UntrustedOracleMacro);
                return;
            }
            let Some(arguments) = arguments else {
                self.facts
                    .defects
                    .insert(SourceDefect::MalformedInvocationMacro);
                return;
            };
            let Some(Expr::Call(call)) = arguments.first() else {
                self.facts
                    .defects
                    .insert(SourceDefect::MalformedInvocationMacro);
                return;
            };
            let Some(called) = unqualified_called_function(call) else {
                self.facts
                    .defects
                    .insert(SourceDefect::MalformedInvocationMacro);
                return;
            };
            let kind = if name == "oracle_expect_err" {
                InvocationKind::ExpectErr
            } else {
                InvocationKind::Recorder
            };
            self.record_invocation(kind, called);
            for argument in &call.args {
                self.visit_expr(argument);
            }
            for argument in arguments.iter().skip(1) {
                self.visit_expr(argument);
            }
            return;
        }
        if ORACLE_MACROS.contains(&name.as_str()) {
            if !trusted_oracle_macro(&invocation.path, self.imports) {
                self.facts
                    .defects
                    .insert(SourceDefect::UntrustedOracleMacro);
                return;
            }
        } else if !safe_builtin_macro(&invocation.path, self.imports) {
            self.facts.defects.insert(SourceDefect::OpaqueMacro);
            return;
        }
        if TOKEN_ONLY_MACROS.contains(&name.as_str()) {
            return;
        }
        let Some(arguments) = arguments else {
            self.facts.defects.insert(SourceDefect::OpaqueMacro);
            return;
        };
        self.with_guarantee(false, |visitor| {
            for argument in &arguments {
                visitor.visit_expr(argument);
            }
        });
    }

    fn visit_pat_ident(&mut self, pattern: &'ast PatIdent) {
        if pattern.ident == self.detector {
            self.facts.defects.insert(SourceDefect::ShadowedDetector);
        }
        visit::visit_pat_ident(self, pattern);
    }

    fn visit_item_const(&mut self, item: &'ast ItemConst) {
        if item.ident == self.detector {
            self.facts.defects.insert(SourceDefect::ShadowedDetector);
        }
        visit::visit_item_const(self, item);
    }

    fn visit_item_static(&mut self, item: &'ast ItemStatic) {
        if item.ident == self.detector {
            self.facts.defects.insert(SourceDefect::ShadowedDetector);
        }
        visit::visit_item_static(self, item);
    }

    fn visit_expr_try(&mut self, expression: &'ast ExprTry) {
        self.visit_expr(&expression.expr);
        self.statement_may_exit = true;
    }

    fn visit_expr_return(&mut self, expression: &'ast ExprReturn) {
        if let Some(value) = &expression.expr {
            self.visit_expr(value);
        }
        self.statement_may_exit = true;
    }

    fn visit_expr_break(&mut self, expression: &'ast ExprBreak) {
        if let Some(value) = &expression.expr {
            self.visit_expr(value);
        }
        self.statement_may_exit = true;
    }

    fn visit_expr_continue(&mut self, _expression: &'ast ExprContinue) {
        self.statement_may_exit = true;
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
        self.with_guarantee(false, |visitor| visitor.visit_block(&expression.body));
    }

    fn visit_expr_closure(&mut self, expression: &'ast ExprClosure) {
        for input in &expression.inputs {
            self.visit_pat(input);
        }
        self.with_guarantee(false, |visitor| visitor.visit_expr(&expression.body));
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
}

fn macro_expressions(name: &str, invocation: &Macro) -> Option<Vec<Expr>> {
    match name {
        "matches" => syn::parse2::<MatchesMacroArguments>(invocation.tokens.clone())
            .ok()
            .map(|arguments| {
                std::iter::once(arguments.expression)
                    .chain(arguments.guard)
                    .collect()
            }),
        "vec" => syn::parse2::<VecMacroArguments>(invocation.tokens.clone())
            .ok()
            .map(|arguments| arguments.expressions),
        _ => Punctuated::<Expr, Token![,]>::parse_terminated
            .parse2(invocation.tokens.clone())
            .ok()
            .map(Punctuated::into_iter)
            .map(Iterator::collect),
    }
}

struct MatchesMacroArguments {
    expression: Expr,
    guard: Option<Expr>,
}

impl Parse for MatchesMacroArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let expression = input.parse()?;
        input.parse::<Token![,]>()?;
        Pat::parse_multi_with_leading_vert(input)?;
        let guard = if input.peek(Token![if]) {
            input.parse::<Token![if]>()?;
            Some(input.parse()?)
        } else {
            None
        };
        input.parse::<Option<Token![,]>>()?;
        if !input.is_empty() {
            return Err(input.error("unexpected matches! arguments"));
        }
        Ok(Self { expression, guard })
    }
}

struct VecMacroArguments {
    expressions: Vec<Expr>,
}

impl Parse for VecMacroArguments {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        if input.is_empty() {
            return Ok(Self {
                expressions: Vec::new(),
            });
        }
        let first = input.parse()?;
        if input.peek(Token![;]) {
            input.parse::<Token![;]>()?;
            let length = input.parse()?;
            if !input.is_empty() {
                return Err(input.error("unexpected vec! repeat arguments"));
            }
            return Ok(Self {
                expressions: vec![first, length],
            });
        }
        let mut expressions = vec![first];
        while !input.is_empty() {
            input.parse::<Token![,]>()?;
            if input.is_empty() {
                break;
            }
            expressions.push(input.parse()?);
        }
        Ok(Self { expressions })
    }
}

fn called_function_leaf(call: &ExprCall) -> Option<String> {
    let Expr::Path(path) = call.func.as_ref() else {
        return None;
    };
    path.path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
}

fn statement_unconditionally_exits(statement: &Stmt) -> bool {
    matches!(
        statement,
        Stmt::Expr(Expr::Return(_) | Expr::Break(_) | Expr::Continue(_), _)
    )
}

fn unqualified_called_function(call: &ExprCall) -> Option<String> {
    let Expr::Path(path) = call.func.as_ref() else {
        return None;
    };
    if path.qself.is_some() || path.path.leading_colon.is_some() || path.path.segments.len() != 1 {
        return None;
    }
    path.path
        .segments
        .first()
        .map(|segment| segment.ident.to_string())
}

#[cfg(test)]
#[path = "detector_source_tests.rs"]
mod tests;
