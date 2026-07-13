use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use syn::{
    visit::Visit, ExprCall, ExprMethodCall, File, ImplItemFn, ItemConst, ItemFn, ItemMacro,
    ItemStatic, ItemUse, UseTree,
};

use super::{Clause, Entry, Evidence, COVERAGE_LAYERS, VALID_EVIDENCE_STRENGTHS};

pub(super) fn assert_evidence_is_machine_checkable(
    workspace: &Path,
    entries: &[Entry],
    clauses: &[Clause],
    evidence: &[Evidence],
) {
    assert!(
        !evidence.is_empty(),
        "registry must declare machine-checkable evidence records",
    );

    let ids = entries
        .iter()
        .map(|entry| entry.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen_records = BTreeSet::new();
    let clauses_by_id = clauses
        .iter()
        .map(|clause| (clause.id.as_str(), clause))
        .collect::<std::collections::BTreeMap<_, _>>();

    for record in evidence {
        assert!(
            ids.contains(record.id.as_str()),
            "evidence record references unknown invariant ID {}",
            record.id,
        );
        assert!(
            COVERAGE_LAYERS.contains(&record.layer.as_str()),
            "{} evidence has unknown layer {}",
            record.id,
            record.layer,
        );
        assert!(
            VALID_EVIDENCE_STRENGTHS.contains(&record.strength.as_str()),
            "{} evidence has unknown strength {}",
            record.id,
            record.strength,
        );
        assert!(
            !record.clauses.is_empty(),
            "{} evidence must bind at least one normative clause",
            record.id,
        );
        for clause_id in &record.clauses {
            let clause = clauses_by_id.get(clause_id.as_str()).unwrap_or_else(|| {
                panic!(
                    "{} evidence references unknown clause {clause_id}",
                    record.id
                )
            });
            assert_eq!(
                clause.invariant_id, record.id,
                "{} evidence cannot bind clause {clause_id} owned by {}",
                record.id, clause.invariant_id,
            );
        }
        assert!(
            seen_records.insert((
                record.id.as_str(),
                record.clauses.as_slice(),
                record.layer.as_str(),
                record.strength.as_str(),
                record.path.as_str(),
                record.symbol.as_str(),
                record.atomic_group.as_deref(),
                record.negative_fixture.as_deref(),
                record.negative_fixture_path.as_deref(),
                record.negative_fixture_detector.as_deref(),
                record.negative_fixture_exemption.as_deref(),
            )),
            "{} {} {} evidence record for {}#{} is duplicated",
            record.id,
            record.layer,
            record.strength,
            record.path,
            record.symbol,
        );
        assert!(
            !record.path.trim().is_empty() && !record.symbol.trim().is_empty(),
            "{} {} {} evidence must name path and symbol",
            record.id,
            record.layer,
            record.strength,
        );
        let path = workspace.join(&record.path);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read evidence path {}: {error}", path.display()));
        assert!(
            source_declares_symbol(&path, &source, &record.symbol),
            "{} {} {} evidence symbol `{}` was not found in {}",
            record.id,
            record.layer,
            record.strength,
            record.symbol,
            record.path,
        );
        assert_negative_fixture_policy(workspace, record, &source);
        assert_atomic_group_policy(record);
    }

    assert_coverage_bindings(entries, clauses, evidence);
}

fn assert_atomic_group_policy(record: &Evidence) {
    let direct_simulator = record.layer == "simulator" && record.strength == "direct";
    if direct_simulator && record.clauses.len() > 1 {
        let group = record.atomic_group.as_deref().unwrap_or_else(|| {
            panic!(
                "{} direct simulator evidence spanning multiple clauses must declare a reviewed atomic_group",
                record.id
            )
        });
        assert!(
            group.starts_with(&format!("{}/", record.id)) && group.len() > record.id.len() + 1,
            "{} atomic_group `{group}` must be a stable ID prefixed with {}/",
            record.id,
            record.id,
        );
        assert_eq!(
            (group, record.clauses.as_slice()),
            (
                "CM-03/current-term-commit-point",
                ["CM-03.a".to_owned(), "CM-03.b".to_owned()].as_slice(),
            ),
            "{} atomic_group `{group}` is not a reviewed atomic clause set",
            record.id,
        );
        assert!(
            record.negative_fixture.is_some() && record.negative_fixture_detector.is_some(),
            "{} atomic_group `{group}` must bind a detector-level negative fixture",
            record.id,
        );
    } else {
        assert!(
            record.atomic_group.is_none(),
            "{} atomic_group is only valid for multi-clause direct simulator evidence",
            record.id,
        );
    }
}

fn assert_coverage_bindings(entries: &[Entry], clauses: &[Clause], evidence: &[Evidence]) {
    for clause in clauses.iter().filter(|clause| clause.required) {
        assert!(
            evidence.iter().any(|record| {
                record.id == clause.invariant_id
                    && record.strength == "direct"
                    && record.clauses.contains(&clause.id)
            }),
            "{} has no direct executable evidence binding",
            clause.id,
        );
    }

    for entry in entries {
        for layer in COVERAGE_LAYERS {
            let coverage = entry
                .current_coverage
                .get(*layer)
                .unwrap_or_else(|| panic!("{} missing current_coverage.{layer}", entry.id));
            let Some(strength) = evidence_strength_for_coverage(coverage) else {
                continue;
            };
            assert!(
                evidence.iter().any(|record| {
                    record.id == entry.id && record.layer == *layer && record.strength == strength
                }),
                "{} current_coverage.{} declares {} evidence but has no machine-checkable evidence record",
                entry.id,
                layer,
                coverage,
            );
        }
    }
}

fn assert_negative_fixture_policy(workspace: &Path, record: &Evidence, source: &str) {
    if let Some(negative_fixture) = &record.negative_fixture {
        let fixture_source = record.negative_fixture_path.as_ref().map_or_else(
            || source.to_owned(),
            |path| {
                let fixture_path = workspace.join(path);
                fs::read_to_string(&fixture_path).unwrap_or_else(|error| {
                    panic!(
                        "read negative fixture path {}: {error}",
                        fixture_path.display()
                    )
                })
            },
        );
        let fixture_path = record
            .negative_fixture_path
            .as_deref()
            .map_or_else(|| workspace.join(&record.path), |path| workspace.join(path));
        assert!(
            source_declares_symbol(&fixture_path, &fixture_source, negative_fixture),
            "{} {} {} negative fixture `{}` was not found in {}",
            record.id,
            record.layer,
            record.strength,
            negative_fixture,
            record
                .negative_fixture_path
                .as_deref()
                .unwrap_or(record.path.as_str()),
        );
        if record.layer == "simulator" && record.strength == "direct" {
            let Some(detector) = record.negative_fixture_detector.as_ref() else {
                panic!(
                    "{} simulator direct negative fixture `{}` must name negative_fixture_detector",
                    record.id, negative_fixture
                )
            };
            assert!(
                fixture_exercises_detector(&fixture_source, negative_fixture, detector),
                "{} simulator direct negative fixture `{}` must exercise detector `{}` in {}",
                record.id,
                negative_fixture,
                detector,
                record
                    .negative_fixture_path
                    .as_deref()
                    .unwrap_or(record.path.as_str()),
            );
        }
    }

    if let Some(exemption) = &record.negative_fixture_exemption {
        assert!(
            !exemption.trim().is_empty(),
            "{} {} {} negative fixture exemption must explain the reviewed exception",
            record.id,
            record.layer,
            record.strength,
        );
        assert!(
            record.negative_fixture.is_none(),
            "{} {} {} must not declare both negative_fixture and negative_fixture_exemption",
            record.id,
            record.layer,
            record.strength,
        );
    }

    if record.layer == "simulator" && record.strength == "direct" {
        assert!(
            record.negative_fixture.is_some() || record.negative_fixture_exemption.is_some(),
            "{} simulator direct evidence must name a negative_fixture or reviewed negative_fixture_exemption",
            record.id,
        );
    }

    if record.negative_fixture_path.is_some() {
        assert!(
            record.negative_fixture.is_some(),
            "{} {} {} must not declare negative_fixture_path without negative_fixture",
            record.id,
            record.layer,
            record.strength,
        );
    }

    if record.negative_fixture_detector.is_some() {
        assert!(
            record.layer == "simulator" && record.strength == "direct",
            "{} {} {} negative_fixture_detector is only meaningful for simulator direct evidence",
            record.id,
            record.layer,
            record.strength,
        );
        assert!(
            record.negative_fixture.is_some(),
            "{} {} {} must not declare negative_fixture_detector without negative_fixture",
            record.id,
            record.layer,
            record.strength,
        );
    }
}

fn fixture_exercises_detector(source: &str, fixture: &str, detector: &str) -> bool {
    let Ok(file) = syn::parse_file(source) else {
        return false;
    };
    let functions = function_calls(&file);
    let mut pending = vec![fixture.to_owned()];
    let mut visited = BTreeSet::new();
    while let Some(function) = pending.pop() {
        if !visited.insert(function.clone()) {
            continue;
        }
        let Some(calls) = functions.get(&function) else {
            continue;
        };
        if calls.contains(detector) {
            return true;
        }
        pending.extend(
            calls
                .iter()
                .filter(|call| functions.contains_key(*call))
                .cloned(),
        );
    }
    false
}

fn source_declares_symbol(path: &Path, source: &str, symbol: &str) -> bool {
    if path.extension().and_then(std::ffi::OsStr::to_str) != Some("rs") {
        return source.contains(symbol);
    }
    syn::parse_file(source).is_ok_and(|file| declared_symbols(&file).contains(symbol))
}

fn declared_symbols(file: &File) -> BTreeSet<String> {
    let mut visitor = DeclarationVisitor::default();
    visitor.visit_file(file);
    visitor.symbols
}

#[derive(Default)]
struct DeclarationVisitor {
    symbols: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for DeclarationVisitor {
    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        self.symbols.insert(function.sig.ident.to_string());
        syn::visit::visit_item_fn(self, function);
    }

    fn visit_impl_item_fn(&mut self, function: &'ast ImplItemFn) {
        self.symbols.insert(function.sig.ident.to_string());
        syn::visit::visit_impl_item_fn(self, function);
    }

    fn visit_item_const(&mut self, item: &'ast ItemConst) {
        self.symbols.insert(item.ident.to_string());
        syn::visit::visit_item_const(self, item);
    }

    fn visit_item_static(&mut self, item: &'ast ItemStatic) {
        self.symbols.insert(item.ident.to_string());
        syn::visit::visit_item_static(self, item);
    }

    fn visit_item_macro(&mut self, item: &'ast ItemMacro) {
        let tokens = item.mac.tokens.to_string();
        let mut previous = None;
        for token in tokens.split_whitespace() {
            if previous == Some("fn") {
                self.symbols.insert(
                    token
                        .trim_matches(|character: char| {
                            !character.is_alphanumeric() && character != '_'
                        })
                        .to_owned(),
                );
            }
            previous = Some(token);
        }
        syn::visit::visit_item_macro(self, item);
    }

    fn visit_item_use(&mut self, item: &'ast ItemUse) {
        collect_use_symbols(&item.tree, &mut self.symbols);
        syn::visit::visit_item_use(self, item);
    }
}

fn collect_use_symbols(tree: &UseTree, symbols: &mut BTreeSet<String>) {
    match tree {
        UseTree::Path(path) => collect_use_symbols(&path.tree, symbols),
        UseTree::Name(name) => {
            symbols.insert(name.ident.to_string());
        }
        UseTree::Rename(rename) => {
            symbols.insert(rename.rename.to_string());
        }
        UseTree::Group(group) => {
            for item in &group.items {
                collect_use_symbols(item, symbols);
            }
        }
        UseTree::Glob(_) => {}
    }
}

fn function_calls(file: &File) -> BTreeMap<String, BTreeSet<String>> {
    let mut visitor = FunctionVisitor::default();
    visitor.visit_file(file);
    visitor.functions
}

#[derive(Default)]
struct FunctionVisitor {
    functions: BTreeMap<String, BTreeSet<String>>,
}

impl<'ast> Visit<'ast> for FunctionVisitor {
    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        let mut calls = CallVisitor::default();
        calls.visit_block(&function.block);
        self.functions
            .entry(function.sig.ident.to_string())
            .or_default()
            .extend(calls.calls);
        syn::visit::visit_item_fn(self, function);
    }

    fn visit_impl_item_fn(&mut self, function: &'ast ImplItemFn) {
        let mut calls = CallVisitor::default();
        calls.visit_block(&function.block);
        self.functions
            .entry(function.sig.ident.to_string())
            .or_default()
            .extend(calls.calls);
        syn::visit::visit_impl_item_fn(self, function);
    }
}

#[derive(Default)]
struct CallVisitor {
    calls: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for CallVisitor {
    fn visit_expr_call(&mut self, call: &'ast ExprCall) {
        if let syn::Expr::Path(path) = call.func.as_ref() {
            if let Some(segment) = path.path.segments.last() {
                self.calls.insert(segment.ident.to_string());
            }
        }
        syn::visit::visit_expr_call(self, call);
    }

    fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
        self.calls.insert(call.method.to_string());
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_item_fn(&mut self, _function: &'ast ItemFn) {}

    fn visit_impl_item_fn(&mut self, _function: &'ast ImplItemFn) {}
}

#[test]
fn negative_fixture_guard_scopes_detector_to_named_test() {
    let source =
        "#[test]\nfn target_fixture() { reporter(); }\n\n#[test]\nfn neighbor() { detector(); }\n";
    assert!(!fixture_exercises_detector(
        source,
        "target_fixture",
        "detector"
    ));
}

#[test]
fn negative_fixture_guard_follows_local_fixture_helpers() {
    let source = "#[test]\nfn target_fixture() { helper(); }\n\nfn helper() { detector(); }\n";
    assert!(fixture_exercises_detector(
        source,
        "target_fixture",
        "detector"
    ));
}

#[test]
fn negative_fixture_guard_ignores_detector_names_in_comments_and_strings() {
    let source = r#"
#[test]
fn target_fixture() {
    // detector();
    let _description = "detector()";
}
"#;
    assert!(!fixture_exercises_detector(
        source,
        "target_fixture",
        "detector"
    ));
}

#[test]
fn rust_symbol_guard_requires_a_real_declaration() {
    let path = Path::new("fixture.rs");
    assert!(!source_declares_symbol(
        path,
        "// fn claimed_symbol() {}\nconst NOTE: &str = \"claimed_symbol\";",
        "claimed_symbol"
    ));
    assert!(source_declares_symbol(
        path,
        "fn claimed_symbol() {}",
        "claimed_symbol"
    ));
}

fn evidence_strength_for_coverage(coverage: &str) -> Option<&'static str> {
    let coverage = coverage.trim();
    if coverage == "D" || coverage.starts_with("D:") {
        Some("direct")
    } else if coverage == "E2E" || coverage.starts_with("E2E:") {
        Some("e2e")
    } else {
        None
    }
}
