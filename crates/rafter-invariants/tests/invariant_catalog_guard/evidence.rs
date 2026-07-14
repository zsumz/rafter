use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use syn::{
    visit::Visit, ExprCall, ExprMethodCall, File, ImplItemFn, ItemConst, ItemFn, ItemMacro,
    ItemMod, ItemStatic, ItemUse, Macro, UseTree,
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
        assert_cargo_test_target_matches_path(record);
        assert_registered_test_contract(workspace, record, &path, &source);
        assert_negative_fixture_policy(workspace, record, &source);
        assert_atomic_group_policy(record);
    }

    assert_coverage_bindings(entries, clauses, evidence);
}

fn assert_registered_test_contract(workspace: &Path, record: &Evidence, path: &Path, source: &str) {
    let Some(identity) = &record.test else {
        return;
    };
    assert!(
        matches!(identity.target_kind.as_str(), "lib" | "test" | "bin"),
        "{} tests evidence uses unsupported Cargo target kind {}",
        record.id,
        identity.target_kind,
    );
    assert_eq!(
        identity.test_name.rsplit("::").next(),
        Some(record.symbol.as_str()),
        "{} tests evidence symbol must equal its exact libtest identity leaf",
        record.id,
    );
    let file = syn::parse_file(source).unwrap_or_else(|error| {
        panic!(
            "parse registered test source {} for {}: {error}",
            path.display(),
            record.id
        )
    });
    let module = test_source_module_path(workspace, Path::new(&record.path), identity)
        .unwrap_or_else(|| {
            panic!(
                "{} tests evidence source {} is outside the registered {}/{}/{} target",
                record.id, record.path, identity.package, identity.target_kind, identity.target,
            )
        });
    let mut visitor = RegisteredTestVisitor {
        symbol: &record.symbol,
        module,
        inline_modules: Vec::new(),
        declarations: Vec::new(),
    };
    visitor.visit_file(&file);
    let declarations = visitor
        .declarations
        .into_iter()
        .filter(|(test_name, _, _)| test_name == &identity.test_name)
        .map(|(_, is_test, should_panic)| (is_test, should_panic))
        .collect::<Vec<_>>();
    assert_eq!(
        declarations,
        [(true, false)],
        "{} tests evidence identity `{}` must name one #[test] function without #[should_panic] in {}",
        record.id, identity.test_name, record.path,
    );
    assert!(
        test_identity_uses_typed_oracle(workspace, &record.path, source, &record.symbol, identity),
        "{} tests evidence identity `{}` must execute an explicit typed oracle macro",
        record.id,
        identity.test_name,
    );
}

struct RegisteredTestVisitor<'a> {
    symbol: &'a str,
    module: Vec<String>,
    inline_modules: Vec<String>,
    declarations: Vec<(String, bool, bool)>,
}

impl<'ast> Visit<'ast> for RegisteredTestVisitor<'_> {
    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        if function.sig.ident == self.symbol {
            let mut test_name = self.module.clone();
            test_name.extend(self.inline_modules.iter().cloned());
            test_name.push(self.symbol.to_owned());
            self.declarations.push((
                test_name.join("::"),
                function
                    .attrs
                    .iter()
                    .any(|attribute| attribute.path().is_ident("test")),
                function
                    .attrs
                    .iter()
                    .any(|attribute| attribute.path().is_ident("should_panic")),
            ));
        }
        syn::visit::visit_item_fn(self, function);
    }

    fn visit_item_mod(&mut self, item: &'ast ItemMod) {
        let Some((_, items)) = &item.content else {
            return;
        };
        self.inline_modules.push(item.ident.to_string());
        for item in items {
            self.visit_item(item);
        }
        self.inline_modules.pop();
    }

    fn visit_item_macro(&mut self, item: &'ast ItemMacro) {
        if item.mac.path.is_ident("proptest") {
            let tokens = item.mac.tokens.to_string();
            let declaration = format!("fn {}", self.symbol);
            if tokens.contains(&declaration) {
                let mut test_name = self.module.clone();
                test_name.extend(self.inline_modules.iter().cloned());
                test_name.push(self.symbol.to_owned());
                self.declarations.push((
                    test_name.join("::"),
                    tokens.contains(&format!("# [test] {declaration}")),
                    tokens.contains("# [should_panic]"),
                ));
            }
        }
        syn::visit::visit_item_macro(self, item);
    }
}

fn assert_cargo_test_target_matches_path(record: &Evidence) {
    let Some(identity) = &record.test else {
        return;
    };
    if identity.target_kind != "test" {
        return;
    }

    let expected = cargo_integration_test_root(identity);
    assert_eq!(
        record.path, expected,
        "{} tests evidence declares Cargo integration target {} but its source path is not the target root",
        record.id, identity.target,
    );
}

fn cargo_integration_test_root(identity: &rafter_invariants::TestIdentity) -> String {
    format!("crates/{}/tests/{}.rs", identity.package, identity.target)
}

fn test_source_module_path(
    workspace: &Path,
    path: &Path,
    identity: &rafter_invariants::TestIdentity,
) -> Option<Vec<String>> {
    type CacheKey = (PathBuf, String, String, String);
    type ModuleGraph = BTreeMap<PathBuf, BTreeSet<String>>;
    static MODULE_GRAPHS: OnceLock<Mutex<BTreeMap<CacheKey, ModuleGraph>>> = OnceLock::new();

    let workspace = fs::canonicalize(workspace).ok()?;
    let target = fs::canonicalize(workspace.join(path)).ok()?;
    let key = (
        workspace.clone(),
        identity.package.clone(),
        identity.target_kind.clone(),
        identity.target.clone(),
    );
    let mut cache = MODULE_GRAPHS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()?;
    let graph = cache
        .entry(key)
        .or_insert_with(|| build_test_target_module_graph(&workspace, identity));
    let matches = graph.get(&target)?.iter().collect::<Vec<_>>();
    let [module] = matches.as_slice() else {
        return None;
    };
    Some(
        module
            .split("::")
            .filter(|part| !part.is_empty())
            .map(str::to_owned)
            .collect(),
    )
}

fn build_test_target_module_graph(
    workspace: &Path,
    identity: &rafter_invariants::TestIdentity,
) -> BTreeMap<PathBuf, BTreeSet<String>> {
    let mut graph = BTreeMap::new();
    for root in test_target_roots(workspace, identity) {
        let Ok(root) = fs::canonicalize(root) else {
            continue;
        };
        let Some(parent) = root.parent() else {
            continue;
        };
        collect_module_graph(&root, &[], parent, parent, &mut BTreeSet::new(), &mut graph);
    }
    graph
}

fn test_target_roots(workspace: &Path, identity: &rafter_invariants::TestIdentity) -> Vec<PathBuf> {
    let package = workspace.join("crates").join(&identity.package);
    let candidates = match identity.target_kind.as_str() {
        "lib" => vec![package.join("src/lib.rs")],
        "test" => vec![package
            .join("tests")
            .join(format!("{}.rs", identity.target))],
        "bin" => {
            let mut roots = vec![
                package
                    .join("src/bin")
                    .join(format!("{}.rs", identity.target)),
                package
                    .join("src/bin")
                    .join(&identity.target)
                    .join("main.rs"),
            ];
            if identity.target == identity.package {
                roots.push(package.join("src/main.rs"));
            }
            roots
        }
        _ => Vec::new(),
    };
    candidates
        .into_iter()
        .filter(|candidate| candidate.is_file())
        .collect()
}

#[allow(clippy::too_many_arguments)]
fn collect_module_graph(
    source_file: &Path,
    module: &[String],
    path_base: &Path,
    module_dir: &Path,
    visited: &mut BTreeSet<(PathBuf, String)>,
    graph: &mut BTreeMap<PathBuf, BTreeSet<String>>,
) {
    let key = (source_file.to_owned(), module.join("::"));
    if !visited.insert(key) {
        return;
    }
    graph
        .entry(source_file.to_owned())
        .or_default()
        .insert(module.join("::"));
    let Ok(source) = fs::read_to_string(source_file) else {
        return;
    };
    let Ok(file) = syn::parse_file(&source) else {
        return;
    };
    collect_module_graph_from_items(&file.items, module, path_base, module_dir, visited, graph);
}

fn collect_module_graph_from_items(
    items: &[syn::Item],
    module: &[String],
    path_base: &Path,
    module_dir: &Path,
    visited: &mut BTreeSet<(PathBuf, String)>,
    graph: &mut BTreeMap<PathBuf, BTreeSet<String>>,
) {
    for item in items {
        let syn::Item::Mod(item) = item else {
            continue;
        };
        let mut child_module = module.to_vec();
        child_module.push(item.ident.to_string());
        if let Some((_, inline_items)) = &item.content {
            let inline_dir = module_dir.join(item.ident.to_string());
            collect_module_graph_from_items(
                inline_items,
                &child_module,
                &inline_dir,
                &inline_dir,
                visited,
                graph,
            );
            continue;
        }
        let Some(child_file) = resolve_external_module(item, path_base, module_dir) else {
            continue;
        };
        let Ok(child_file) = fs::canonicalize(child_file) else {
            continue;
        };
        let child_dir =
            if child_file.file_name().and_then(std::ffi::OsStr::to_str) == Some("mod.rs") {
                child_file.parent().unwrap_or(module_dir).to_owned()
            } else {
                module_dir.join(item.ident.to_string())
            };
        let child_path_base = child_file.parent().unwrap_or(path_base);
        collect_module_graph(
            &child_file,
            &child_module,
            child_path_base,
            &child_dir,
            visited,
            graph,
        );
    }
}

fn resolve_external_module(item: &ItemMod, path_base: &Path, module_dir: &Path) -> Option<PathBuf> {
    if let Some(path) = item.attrs.iter().find_map(module_path_attribute) {
        return Some(path_base.join(path));
    }
    let name = item.ident.to_string();
    let candidates = [
        module_dir.join(format!("{name}.rs")),
        module_dir.join(&name).join("mod.rs"),
    ];
    let existing = candidates
        .into_iter()
        .filter(|candidate| candidate.is_file())
        .collect::<Vec<_>>();
    let [path] = existing.as_slice() else {
        return None;
    };
    Some(path.clone())
}

fn module_path_attribute(attribute: &syn::Attribute) -> Option<PathBuf> {
    let syn::Meta::NameValue(name_value) = &attribute.meta else {
        return None;
    };
    if !name_value.path.is_ident("path") {
        return None;
    }
    let syn::Expr::Lit(expression) = &name_value.value else {
        return None;
    };
    let syn::Lit::Str(path) = &expression.lit else {
        return None;
    };
    Some(PathBuf::from(path.value()))
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
    assert_declared_negative_fixture(workspace, record, source);

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

    assert_direct_simulator_fixture_policy(record);

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

fn assert_declared_negative_fixture(workspace: &Path, record: &Evidence, source: &str) {
    let Some(negative_fixture) = &record.negative_fixture else {
        return;
    };
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
        assert_simulator_detector_fixture(
            workspace,
            record,
            negative_fixture,
            &fixture_path,
            &fixture_source,
        );
    }
}

fn assert_simulator_detector_fixture(
    workspace: &Path,
    record: &Evidence,
    negative_fixture: &str,
    fixture_path: &Path,
    fixture_source: &str,
) {
    let detector = record
        .negative_fixture_detector
        .as_ref()
        .unwrap_or_else(|| {
            panic!(
                "{} simulator direct negative fixture `{negative_fixture}` must name negative_fixture_detector",
                record.id,
            )
        });
    let fixture_path_text = record
        .negative_fixture_path
        .as_deref()
        .unwrap_or(record.path.as_str());
    let identity = record
        .simulator
        .as_ref()
        .and_then(|identity| identity.negative_test.as_ref())
        .expect("direct simulator fixture has executable identity");
    let fixture_module = test_source_module_path(workspace, Path::new(fixture_path_text), identity)
        .unwrap_or_else(|| {
            panic!(
                "{} simulator fixture source {fixture_path_text} is outside registered target",
                record.id,
            )
        });
    let detector_module = if source_declares_symbol(fixture_path, fixture_source, detector) {
        fixture_module.clone()
    } else {
        rust_module_path(Path::new(&record.path))
    };
    assert!(
        fixture_exercises_detector_from_module(
            fixture_source,
            negative_fixture,
            detector,
            &detector_module,
            &fixture_module,
        ),
        "{} simulator direct negative fixture `{negative_fixture}` must exercise detector `{detector}` in {fixture_path_text}",
        record.id,
    );
    assert!(
        test_identity_matches_source(
            workspace,
            fixture_path_text,
            fixture_source,
            negative_fixture,
            identity,
        ),
        "{} simulator fixture `{negative_fixture}` execution identity `{}` does not match its analyzed module",
        record.id,
        identity.test_name,
    );
    assert!(
        test_identity_uses_typed_oracle(
            workspace,
            fixture_path_text,
            fixture_source,
            negative_fixture,
            identity,
        ),
        "{} simulator fixture `{negative_fixture}` must execute an explicit typed oracle macro",
        record.id,
    );
}

fn assert_direct_simulator_fixture_policy(record: &Evidence) {
    if record.layer != "simulator" || record.strength != "direct" {
        return;
    }
    assert!(
        record.negative_fixture_exemption.is_none(),
        "{} simulator direct evidence may not use negative_fixture_exemption",
        record.id,
    );
    assert!(
        record.negative_fixture.is_some()
            && record.negative_fixture_detector.is_some()
            && record
                .simulator
                .as_ref()
                .and_then(|identity| identity.negative_test.as_ref())
                .is_some(),
        "{} simulator direct evidence must bind an executable detector-level negative fixture",
        record.id,
    );
}

fn fixture_exercises_detector(source: &str, fixture: &str, detector: &str) -> bool {
    fixture_exercises_detector_from_module(source, fixture, detector, &[], &[])
}

fn fixture_exercises_detector_from_module(
    source: &str,
    fixture: &str,
    detector: &str,
    detector_module: &[String],
    fixture_module: &[String],
) -> bool {
    let Ok(file) = syn::parse_file(source) else {
        return false;
    };
    let imports = imported_paths(&file);
    let detector_declared_locally = declared_symbols(&file).contains(detector);
    let detector_imports = imports.explicit.get(detector);
    if !detector_declared_locally
        && (imports.aliases.contains(detector)
            || detector_imports.is_some_and(|paths| {
                paths
                    .iter()
                    .any(|path| !trusted_import_path(path, detector_module, fixture_module))
            }))
    {
        return false;
    }
    let detector_unqualified_trusted = detector_declared_locally
        || detector_imports.is_some_and(|paths| {
            !paths.is_empty()
                && paths
                    .iter()
                    .all(|path| trusted_import_path(path, detector_module, fixture_module))
        })
        || imports
            .globs
            .iter()
            .any(|path| trusted_import_path(path, detector_module, fixture_module));
    let functions = function_calls(
        &file,
        detector,
        detector_unqualified_trusted,
        detector_module,
        fixture_module,
    );
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
                .filter(|call| {
                    imports.explicit.get(*call).is_none_or(|paths| {
                        paths
                            .iter()
                            .all(|path| trusted_import_path(path, detector_module, fixture_module))
                    }) && functions.contains_key(*call)
                })
                .cloned(),
        );
    }
    false
}

#[derive(Default)]
struct ImportedPaths {
    explicit: BTreeMap<String, Vec<Vec<String>>>,
    globs: Vec<Vec<String>>,
    aliases: BTreeSet<String>,
}

fn imported_paths(file: &File) -> ImportedPaths {
    #[derive(Default)]
    struct ImportVisitor {
        imports: ImportedPaths,
    }

    impl<'ast> Visit<'ast> for ImportVisitor {
        fn visit_item_use(&mut self, item: &'ast ItemUse) {
            collect_use_tree(&item.tree, &mut Vec::new(), &mut self.imports);
        }
    }

    let mut visitor = ImportVisitor::default();
    visitor.visit_file(file);
    visitor.imports
}

fn collect_use_tree(tree: &UseTree, prefix: &mut Vec<String>, imports: &mut ImportedPaths) {
    match tree {
        UseTree::Path(path) => {
            prefix.push(path.ident.to_string());
            collect_use_tree(&path.tree, prefix, imports);
            prefix.pop();
        }
        UseTree::Name(name) => {
            let mut path = prefix.clone();
            path.push(name.ident.to_string());
            imports
                .explicit
                .entry(name.ident.to_string())
                .or_default()
                .push(path);
        }
        UseTree::Rename(rename) => {
            let mut path = prefix.clone();
            path.push(rename.ident.to_string());
            imports
                .explicit
                .entry(rename.rename.to_string())
                .or_default()
                .push(path);
            imports.aliases.insert(rename.rename.to_string());
        }
        UseTree::Glob(_) => imports.globs.push(prefix.clone()),
        UseTree::Group(group) => {
            for item in &group.items {
                collect_use_tree(item, prefix, imports);
            }
        }
    }
}

fn trusted_import_path(
    path: &[String],
    detector_module: &[String],
    fixture_module: &[String],
) -> bool {
    if detector_module.is_empty() {
        return path
            .first()
            .is_some_and(|segment| segment == "self" || segment == "super")
            && path.len() <= 2;
    }
    let mut exact = vec!["crate".to_owned()];
    exact.extend_from_slice(detector_module);
    if path == exact || path.len() == exact.len() + 1 && path.starts_with(&exact) {
        return true;
    }
    resolve_relative_module(path, fixture_module).is_some_and(|resolved| {
        resolved == detector_module
            || resolved.len() == detector_module.len() + 1 && resolved.starts_with(detector_module)
    })
}

fn resolve_relative_module(path: &[String], fixture_module: &[String]) -> Option<Vec<String>> {
    let first = path.first()?;
    if first != "self" && first != "super" {
        return None;
    }
    let mut resolved = fixture_module.to_vec();
    let mut position = 0;
    if first == "self" {
        position = 1;
    }
    while path.get(position).is_some_and(|segment| segment == "super") {
        resolved.pop()?;
        position += 1;
    }
    resolved.extend_from_slice(&path[position..]);
    Some(resolved)
}

fn rust_module_path(path: &Path) -> Vec<String> {
    let components = path
        .components()
        .filter_map(|component| component.as_os_str().to_str().map(str::to_owned))
        .collect::<Vec<_>>();
    let Some(src) = components.iter().position(|component| component == "src") else {
        return Vec::new();
    };
    let mut modules = components[src + 1..].to_vec();
    let Some(last) = modules.last_mut() else {
        return modules;
    };
    *last = Path::new(last)
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .unwrap_or_default()
        .to_owned();
    if matches!(last.as_str(), "lib" | "mod") {
        modules.pop();
    }
    modules
}

fn test_identity_matches_source(
    workspace: &Path,
    fixture_path: &str,
    source: &str,
    fixture: &str,
    identity: &rafter_invariants::TestIdentity,
) -> bool {
    let Ok(file) = syn::parse_file(source) else {
        return false;
    };
    let Some(module) = test_source_module_path(workspace, Path::new(fixture_path), identity) else {
        return false;
    };
    let mut visitor = RegisteredTestVisitor {
        symbol: fixture,
        module,
        inline_modules: Vec::new(),
        declarations: Vec::new(),
    };
    visitor.visit_file(&file);
    visitor
        .declarations
        .iter()
        .filter(|(test_name, is_test, should_panic)| {
            test_name == &identity.test_name && *is_test && !*should_panic
        })
        .count()
        == 1
}

fn test_identity_uses_typed_oracle(
    workspace: &Path,
    fixture_path: &str,
    source: &str,
    fixture: &str,
    identity: &rafter_invariants::TestIdentity,
) -> bool {
    let Ok(file) = syn::parse_file(source) else {
        return false;
    };
    if source.contains("RAFTER_INVARIANT_ORACLE_OBSERVED:")
        || source.contains("RAFTER_INVARIANT_ORACLE_VIOLATION:")
        || declares_local_oracle_macro(&file)
    {
        return false;
    }
    let trusted_macros = trusted_oracle_imports(&file);
    let Some(module) = test_source_module_path(workspace, Path::new(fixture_path), identity) else {
        return false;
    };
    let mut visitor = TypedOracleTestVisitor {
        symbol: fixture,
        test_name: &identity.test_name,
        module,
        inline_modules: Vec::new(),
        trusted_macros,
        matches: 0,
    };
    visitor.visit_file(&file);
    visitor.matches == 1
}

struct TypedOracleTestVisitor<'a> {
    symbol: &'a str,
    test_name: &'a str,
    module: Vec<String>,
    inline_modules: Vec<String>,
    trusted_macros: BTreeSet<String>,
    matches: usize,
}

impl Visit<'_> for TypedOracleTestVisitor<'_> {
    fn visit_item_fn(&mut self, function: &ItemFn) {
        if function.sig.ident == self.symbol && self.current_test_name() == self.test_name {
            let mut oracle = OracleMacroVisitor {
                trusted_macros: &self.trusted_macros,
                found: false,
                untrusted: false,
            };
            oracle.visit_block(&function.block);
            self.matches += usize::from(oracle.found && !oracle.untrusted);
        }
        syn::visit::visit_item_fn(self, function);
    }

    fn visit_item_mod(&mut self, item: &ItemMod) {
        let Some((_, items)) = &item.content else {
            return;
        };
        self.inline_modules.push(item.ident.to_string());
        for item in items {
            self.visit_item(item);
        }
        self.inline_modules.pop();
    }

    fn visit_item_macro(&mut self, item: &ItemMacro) {
        if item.mac.path.is_ident("proptest")
            && item
                .mac
                .tokens
                .to_string()
                .contains(&format!("fn {}", self.symbol))
            && self.current_test_name() == self.test_name
            && ["oracle_prop_assert", "oracle_prop_assert_eq"]
                .iter()
                .any(|name| {
                    self.trusted_macros.contains(*name)
                        && item.mac.tokens.to_string().contains(name)
                })
        {
            self.matches += 1;
        }
    }
}

impl TypedOracleTestVisitor<'_> {
    fn current_test_name(&self) -> String {
        let mut path = self.module.clone();
        path.extend(self.inline_modules.iter().cloned());
        path.push(self.symbol.to_owned());
        path.join("::")
    }
}

struct OracleMacroVisitor<'a> {
    trusted_macros: &'a BTreeSet<String>,
    found: bool,
    untrusted: bool,
}

impl Visit<'_> for OracleMacroVisitor<'_> {
    fn visit_macro(&mut self, invocation: &Macro) {
        let segments = invocation
            .path
            .segments
            .iter()
            .map(|segment| segment.ident.to_string())
            .collect::<Vec<_>>();
        let Some(name) = segments.last() else {
            return;
        };
        if is_oracle_macro(name) {
            let qualified = segments.as_slice() == ["rafter_invariant_test", name.as_str()];
            let imported = segments.len() == 1 && self.trusted_macros.contains(name);
            self.found |= qualified || imported;
            self.untrusted |= !qualified && !imported;
        }
        syn::visit::visit_macro(self, invocation);
    }
}

fn trusted_oracle_imports(file: &File) -> BTreeSet<String> {
    imported_paths(file)
        .explicit
        .into_iter()
        .filter_map(|(name, paths)| {
            (is_oracle_macro(&name)
                && paths.len() == 1
                && paths[0].as_slice() == ["rafter_invariant_test", name.as_str()])
            .then_some(name)
        })
        .collect()
}

fn declares_local_oracle_macro(file: &File) -> bool {
    #[derive(Default)]
    struct Visitor {
        found: bool,
    }

    impl Visit<'_> for Visitor {
        fn visit_item_macro(&mut self, item: &ItemMacro) {
            self.found |= item
                .ident
                .as_ref()
                .is_some_and(|ident| is_oracle_macro(&ident.to_string()));
            syn::visit::visit_item_macro(self, item);
        }
    }

    let mut visitor = Visitor::default();
    visitor.visit_file(file);
    visitor.found
}

fn is_oracle_macro(name: &str) -> bool {
    matches!(
        name,
        "oracle_assert"
            | "oracle_assert_eq"
            | "oracle_assert_ne"
            | "oracle_expect_err"
            | "oracle_violation"
            | "oracle_prop_assert"
            | "oracle_prop_assert_eq"
    )
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
}

fn function_calls(
    file: &File,
    detector: &str,
    detector_unqualified_trusted: bool,
    detector_module: &[String],
    fixture_module: &[String],
) -> BTreeMap<String, BTreeSet<String>> {
    let mut visitor = FunctionVisitor {
        functions: BTreeMap::new(),
        detector: detector.to_owned(),
        detector_unqualified_trusted,
        detector_module: detector_module.to_owned(),
        fixture_module: fixture_module.to_owned(),
    };
    visitor.visit_file(file);
    visitor.functions
}

struct FunctionVisitor {
    functions: BTreeMap<String, BTreeSet<String>>,
    detector: String,
    detector_unqualified_trusted: bool,
    detector_module: Vec<String>,
    fixture_module: Vec<String>,
}

impl<'ast> Visit<'ast> for FunctionVisitor {
    fn visit_item_fn(&mut self, function: &'ast ItemFn) {
        let mut calls = CallVisitor {
            calls: BTreeSet::new(),
            detector: self.detector.clone(),
            detector_unqualified_trusted: self.detector_unqualified_trusted,
            detector_module: self.detector_module.clone(),
            fixture_module: self.fixture_module.clone(),
        };
        calls.visit_block(&function.block);
        self.functions
            .entry(function.sig.ident.to_string())
            .or_default()
            .extend(calls.calls);
        syn::visit::visit_item_fn(self, function);
    }

    fn visit_impl_item_fn(&mut self, function: &'ast ImplItemFn) {
        let mut calls = CallVisitor {
            calls: BTreeSet::new(),
            detector: self.detector.clone(),
            detector_unqualified_trusted: self.detector_unqualified_trusted,
            detector_module: self.detector_module.clone(),
            fixture_module: self.fixture_module.clone(),
        };
        calls.visit_block(&function.block);
        self.functions
            .entry(function.sig.ident.to_string())
            .or_default()
            .extend(calls.calls);
        syn::visit::visit_impl_item_fn(self, function);
    }
}

struct CallVisitor {
    calls: BTreeSet<String>,
    detector: String,
    detector_unqualified_trusted: bool,
    detector_module: Vec<String>,
    fixture_module: Vec<String>,
}

impl<'ast> Visit<'ast> for CallVisitor {
    fn visit_expr_call(&mut self, call: &'ast ExprCall) {
        if let syn::Expr::Path(path) = call.func.as_ref() {
            if path.qself.is_none() {
                let segments = &path.path.segments;
                let path = segments
                    .iter()
                    .map(|segment| segment.ident.to_string())
                    .collect::<Vec<_>>();
                let called = path.last().map(String::as_str).unwrap_or_default();
                let rooted_locally = if segments.len() == 1 {
                    called != self.detector || self.detector_unqualified_trusted
                } else {
                    trusted_import_path(&path, &self.detector_module, &self.fixture_module)
                };
                if rooted_locally {
                    if let Some(segment) = segments.last() {
                        self.calls.insert(segment.ident.to_string());
                    }
                }
            }
        }
        syn::visit::visit_expr_call(self, call);
    }

    fn visit_expr_method_call(&mut self, call: &'ast ExprMethodCall) {
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_macro(&mut self, invocation: &'ast Macro) {
        let is_typed_rejection = invocation
            .path
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "oracle_expect_err");
        if is_typed_rejection {
            let tuple = format!("({})", invocation.tokens);
            if let Ok(arguments) = syn::parse_str::<syn::ExprTuple>(&tuple) {
                if let Some(result) = arguments.elems.first() {
                    self.visit_expr(result);
                }
            }
        }
    }

    fn visit_item_fn(&mut self, _function: &'ast ItemFn) {}

    fn visit_impl_item_fn(&mut self, _function: &'ast ImplItemFn) {}
}

#[cfg(test)]
#[path = "evidence_tests.rs"]
mod tests;

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
