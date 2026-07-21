//! Declared Rust module-graph discovery, including cfg and include semantics.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use syn::{parse::Parser, punctuated::Punctuated, Attribute, Item, ItemMod, Meta, Token};

use super::workspace::{display_path, is_test_module, read};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct DeclaredModule {
    path: Vec<String>,
    test_only: bool,
}

pub(crate) type DeclaredModuleGraph = BTreeMap<String, DeclaredModule>;

pub(crate) fn declared_module_graph(root: &Path) -> DeclaredModuleGraph {
    let source_root = "crates/rafter-invariants/src/";
    declared_module_graph_from_roots(
        root,
        &[
            root.join(source_root).join("lib.rs"),
            root.join(source_root).join("main.rs"),
        ],
    )
}

pub(crate) fn declared_module_graph_from_roots(
    root: &Path,
    roots: &[PathBuf],
) -> DeclaredModuleGraph {
    let root = std::fs::canonicalize(root).unwrap_or_else(|error| {
        panic!("canonicalize module graph root {}: {error}", root.display())
    });
    let mut graph = BTreeMap::new();
    let mut visited = BTreeSet::new();
    for source in roots {
        collect_module_file(&root, source, &[], false, &mut graph, &mut visited);
    }
    graph
}

fn collect_module_file(
    root: &Path,
    source: &Path,
    module_path: &[String],
    test_only: bool,
    graph: &mut DeclaredModuleGraph,
    visited: &mut BTreeSet<PathBuf>,
) {
    let source = std::fs::canonicalize(source)
        .unwrap_or_else(|error| panic!("canonicalize module source {}: {error}", source.display()));
    let relative = display_path(root, &source);
    let declaration = DeclaredModule {
        path: module_path.to_owned(),
        test_only,
    };
    if let Some(existing) = graph.insert(relative.clone(), declaration.clone()) {
        assert_eq!(
            existing, declaration,
            "{relative} is mounted under multiple module identities"
        );
        return;
    }
    assert!(
        visited.insert(source.clone()),
        "module graph contains a cycle through {relative}"
    );
    let syntax = syn::parse_file(&read(&source))
        .unwrap_or_else(|error| panic!("parse module source {relative}: {error}"));
    let module_directory = if source.file_name().and_then(|name| name.to_str()) == Some("mod.rs")
        || matches!(
            source.file_stem().and_then(|stem| stem.to_str()),
            Some("lib" | "main")
        ) {
        source.parent().unwrap().to_path_buf()
    } else {
        source.with_extension("")
    };
    collect_module_items(
        root,
        &source,
        &module_directory,
        &syntax.items,
        module_path,
        test_only,
        graph,
        visited,
    );
    visited.remove(&source);
}

#[allow(clippy::too_many_arguments)]
fn collect_module_items(
    root: &Path,
    source: &Path,
    module_directory: &Path,
    items: &[Item],
    owner: &[String],
    inherited_test_only: bool,
    graph: &mut DeclaredModuleGraph,
    visited: &mut BTreeSet<PathBuf>,
) {
    for (included, test_only) in items.iter().filter_map(|item| match item {
        Item::Macro(item) if item.mac.path.is_ident("include") => {
            let path =
                syn::parse2::<syn::LitStr>(item.mac.tokens.clone()).unwrap_or_else(|error| {
                    panic!("parse include path in {}: {error}", source.display())
                });
            Some((
                source.parent().unwrap().join(path.value()),
                inherited_test_only || attributes_require_test(&item.attrs),
            ))
        }
        _ => None,
    }) {
        collect_module_file(root, &included, owner, test_only, graph, visited);
    }
    for module in items.iter().filter_map(|item| match item {
        Item::Mod(module) => Some(module),
        _ => None,
    }) {
        let mut module_path = owner.to_vec();
        let segment = module.ident.to_string();
        module_path.push(segment.clone());
        let test_only = inherited_test_only || module_requires_test(module);
        if let Some((_, inline)) = &module.content {
            collect_module_items(
                root,
                source,
                &module_directory.join(&segment),
                inline,
                &module_path,
                test_only,
                graph,
                visited,
            );
            continue;
        }
        let (child, path_is_test_only) =
            module_path_attribute(source, module).unwrap_or_else(|| {
                (
                    [
                        module_directory.join(format!("{segment}.rs")),
                        module_directory.join(&segment).join("mod.rs"),
                    ]
                    .into_iter()
                    .find(|candidate| candidate.is_file())
                    .unwrap_or_else(|| {
                        panic!(
                            "cannot resolve module {segment} declared by {}",
                            source.display()
                        )
                    }),
                    false,
                )
            });
        collect_module_file(
            root,
            &child,
            &module_path,
            test_only || path_is_test_only,
            graph,
            visited,
        );
    }
}

fn module_path_attribute(source: &Path, module: &ItemMod) -> Option<(PathBuf, bool)> {
    let mut selected = Vec::new();
    collect_active_path_attributes(source, &module.attrs, false, &mut selected);
    assert!(
        selected.len() <= 1,
        "multiple active path attributes in {}",
        source.display()
    );
    selected
        .pop()
        .map(|(path, test_only)| (source.parent().unwrap().join(path), test_only))
}

fn collect_active_path_attributes(
    source: &Path,
    attributes: &[Attribute],
    test_only: bool,
    selected: &mut Vec<(String, bool)>,
) {
    for attribute in attributes {
        collect_active_path_meta(source, &attribute.meta, test_only, selected);
    }
}

fn collect_active_path_meta(
    source: &Path,
    meta: &Meta,
    test_only: bool,
    selected: &mut Vec<(String, bool)>,
) {
    if meta.path().is_ident("path") {
        selected.push((path_value(source, meta), test_only));
        return;
    }
    let Meta::List(list) = meta else {
        return;
    };
    if !list.path.is_ident("cfg_attr") {
        return;
    }
    let nested = parse_meta_list(list, "cfg_attr");
    let Some((predicate, attributes)) = nested.split_first() else {
        panic!("empty cfg_attr in {}", source.display());
    };
    match cfg_truth(predicate, true) {
        Certainty::AlwaysTrue => {
            let nested_test_only =
                test_only || cfg_truth(predicate, false) == Certainty::AlwaysFalse;
            for attribute in attributes {
                collect_active_path_meta(source, attribute, nested_test_only, selected);
            }
        }
        Certainty::Unknown if attributes.iter().any(meta_contains_path) => {
            panic!("target-conditional path attribute in {}", source.display())
        }
        Certainty::AlwaysFalse | Certainty::Unknown => {}
    }
}

fn meta_contains_path(meta: &Meta) -> bool {
    if meta.path().is_ident("path") {
        return true;
    }
    let Meta::List(list) = meta else {
        return false;
    };
    if !list.path.is_ident("cfg_attr") {
        return false;
    }
    parse_meta_list(list, "cfg_attr")
        .iter()
        .skip(1)
        .any(meta_contains_path)
}

fn path_value(source: &Path, meta: &Meta) -> String {
    let Meta::NameValue(value) = meta else {
        panic!("invalid path attribute in {}", source.display());
    };
    let syn::Expr::Lit(expression) = &value.value else {
        panic!("non-literal path attribute in {}", source.display());
    };
    let syn::Lit::Str(path) = &expression.lit else {
        panic!("non-string path attribute in {}", source.display());
    };
    path.value()
}

pub(crate) fn is_declared_test_module(modules: &DeclaredModuleGraph, relative: &str) -> bool {
    const SOURCE_ROOT: &str = "crates/rafter-invariants/src/";
    if !relative.starts_with(SOURCE_ROOT) {
        return is_test_module(relative);
    }
    module_is_test_only(modules, relative)
}

pub(crate) fn module_is_test_only(modules: &DeclaredModuleGraph, relative: &str) -> bool {
    modules
        .get(relative)
        .unwrap_or_else(|| panic!("{relative} is not mounted in the invariant crate module graph"))
        .test_only
}

pub(crate) fn declared_module_path(modules: &DeclaredModuleGraph, relative: &str) -> Vec<String> {
    modules
        .get(relative)
        .unwrap_or_else(|| panic!("{relative} is not mounted in the invariant crate module graph"))
        .path
        .clone()
}

fn module_requires_test(module: &ItemMod) -> bool {
    attributes_require_test(&module.attrs)
}

fn attributes_require_test(attributes: &[Attribute]) -> bool {
    attributes_constraint(attributes, false) == Certainty::AlwaysFalse
}

fn attributes_constraint(attributes: &[Attribute], test: bool) -> Certainty {
    attributes
        .iter()
        .fold(Certainty::AlwaysTrue, |state, attribute| {
            state.and(attribute_constraint(&attribute.meta, test))
        })
}

fn attribute_constraint(meta: &Meta, test: bool) -> Certainty {
    let Meta::List(list) = meta else {
        return Certainty::AlwaysTrue;
    };
    if list.path.is_ident("cfg") {
        let condition = syn::parse2::<Meta>(list.tokens.clone())
            .unwrap_or_else(|error| panic!("parse cfg expression: {error}"));
        return cfg_truth(&condition, test);
    }
    if !list.path.is_ident("cfg_attr") {
        return Certainty::AlwaysTrue;
    }
    let nested = parse_meta_list(list, "cfg_attr");
    let Some((predicate, attributes)) = nested.split_first() else {
        panic!("empty cfg_attr");
    };
    let nested_constraint = attributes
        .iter()
        .fold(Certainty::AlwaysTrue, |state, attribute| {
            state.and(attribute_constraint(attribute, test))
        });
    cfg_truth(predicate, test).not().or(nested_constraint)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Certainty {
    AlwaysFalse,
    AlwaysTrue,
    Unknown,
}

impl Certainty {
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Self::AlwaysFalse, _) | (_, Self::AlwaysFalse) => Self::AlwaysFalse,
            (Self::AlwaysTrue, Self::AlwaysTrue) => Self::AlwaysTrue,
            _ => Self::Unknown,
        }
    }

    fn or(self, other: Self) -> Self {
        match (self, other) {
            (Self::AlwaysTrue, _) | (_, Self::AlwaysTrue) => Self::AlwaysTrue,
            (Self::AlwaysFalse, Self::AlwaysFalse) => Self::AlwaysFalse,
            _ => Self::Unknown,
        }
    }

    fn not(self) -> Self {
        match self {
            Self::AlwaysFalse => Self::AlwaysTrue,
            Self::AlwaysTrue => Self::AlwaysFalse,
            Self::Unknown => Self::Unknown,
        }
    }
}

fn cfg_truth(meta: &Meta, test: bool) -> Certainty {
    match meta {
        Meta::Path(path) if path.is_ident("test") => {
            if test {
                Certainty::AlwaysTrue
            } else {
                Certainty::AlwaysFalse
            }
        }
        Meta::List(list) if list.path.is_ident("all") || list.path.is_ident("any") => {
            let nested = parse_meta_list(list, "cfg expression");
            if list.path.is_ident("all") {
                nested.iter().fold(Certainty::AlwaysTrue, |state, item| {
                    state.and(cfg_truth(item, test))
                })
            } else {
                nested.iter().fold(Certainty::AlwaysFalse, |state, item| {
                    state.or(cfg_truth(item, test))
                })
            }
        }
        Meta::List(list) if list.path.is_ident("not") => {
            let nested = parse_meta_list(list, "cfg not expression");
            assert_eq!(nested.len(), 1, "cfg not requires exactly one predicate");
            cfg_truth(&nested[0], test).not()
        }
        Meta::Path(_) | Meta::NameValue(_) | Meta::List(_) => Certainty::Unknown,
    }
}

fn parse_meta_list(list: &syn::MetaList, context: &str) -> Vec<Meta> {
    Punctuated::<Meta, Token![,]>::parse_terminated
        .parse2(list.tokens.clone())
        .unwrap_or_else(|error| panic!("parse {context}: {error}"))
        .into_iter()
        .collect()
}
