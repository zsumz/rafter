//! Domain dependency assertions shared by architecture scenarios.

use std::{collections::BTreeMap, path::Path};

use syn::visit::Visit;

use crate::invariant_tooling::{
    InvariantDomain, ReviewedDomainImportException, INVARIANT_DOMAINS,
    REVIEWED_DOMAIN_IMPORT_EXCEPTIONS,
};

use super::{
    declared_module_path, display_path, is_declared_test_module, read, rust_files,
    DeclaredModuleGraph, RustPathCollector,
};

pub(crate) fn domain(name: &str) -> &'static InvariantDomain {
    INVARIANT_DOMAINS
        .iter()
        .find(|domain| domain.name == name)
        .unwrap()
}

pub(crate) fn assert_forbidden_domain_imports_absent(
    root: &Path,
    modules: &DeclaredModuleGraph,
    source: &str,
    forbidden: &[&str],
) {
    let source_root = root.join(source);
    for path in rust_files(&source_root) {
        let relative = display_path(root, &path);
        if is_declared_test_module(modules, &relative) {
            continue;
        }
        let source = read(&path);
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {relative} for forbidden domains: {error}"));
        let mut paths = RustPathCollector::new(declared_module_path(modules, &relative));
        paths.visit_file(&syntax);
        assert!(
            paths.crate_root_aliases.is_empty(),
            "{relative} aliases the crate root through {:?}",
            paths.crate_root_aliases
        );
        for occurrence in &paths.occurrences {
            if occurrence.normalized.first().map(String::as_str) == Some("crate")
                && occurrence
                    .normalized
                    .get(1)
                    .is_some_and(|domain| forbidden.contains(&domain.as_str()))
            {
                panic!(
                    "{relative} imports forbidden domain through {:?}",
                    occurrence.normalized
                );
            }
        }
        for identifiers in &paths.macro_identifier_groups {
            if let Some(domain) = forbidden
                .iter()
                .find(|domain| identifiers.iter().any(|identifier| identifier == **domain))
            {
                panic!("{relative} names forbidden domain {domain} inside macro tokens");
            }
        }
    }
}

pub(crate) fn assert_domain_source_imports_follow_manifest(
    root: &Path,
    modules: &DeclaredModuleGraph,
    name: &str,
    source: &str,
) {
    let owner = domain(name);
    let source_root = root.join(source);
    let owner_module_prefix = source_module_prefix(root, modules, &source_root);
    let applicable_exceptions = REVIEWED_DOMAIN_IMPORT_EXCEPTIONS
        .iter()
        .enumerate()
        .filter(|(_, exception)| {
            exception.owner_domain == name && source_owns_path(source, exception.source)
        })
        .collect::<Vec<_>>();
    let mut exception_uses = BTreeMap::<usize, usize>::new();
    for path in rust_files(&source_root) {
        let relative = display_path(root, &path);
        if is_declared_test_module(modules, &relative) {
            continue;
        }
        let source = read(&path);
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {relative} for dependency validation: {error}"));
        let mut paths = RustPathCollector::new(declared_module_path(modules, &relative));
        paths.visit_file(&syntax);
        assert!(
            paths.crate_root_aliases.is_empty(),
            "{relative} aliases the crate root through {:?}",
            paths.crate_root_aliases
        );
        let forbidden_domains = INVARIANT_DOMAINS
            .iter()
            .map(|domain| domain.name)
            .filter(|dependency| *dependency != name && !owner.may_depend_on.contains(dependency))
            .collect::<Vec<_>>();
        for identifiers in &paths.macro_identifier_groups {
            if let Some(dependency) = forbidden_domains.iter().find(|dependency| {
                identifiers
                    .iter()
                    .any(|identifier| identifier == **dependency)
            }) {
                panic!("{relative} names forbidden domain {dependency} inside macro tokens");
            }
        }
        for occurrence in paths.occurrences {
            let import = occurrence.normalized;
            if import.first().map(String::as_str) != Some("crate") {
                continue;
            }
            if import.starts_with(&owner_module_prefix) {
                continue;
            }
            let Some(dependency) = import.get(1) else {
                panic!("{relative} accesses the crate root without a domain owner: {import:?}");
            };
            if dependency == name || owner.may_depend_on.contains(&dependency.as_str()) {
                continue;
            }
            if INVARIANT_DOMAINS
                .iter()
                .any(|domain| domain.name == dependency)
            {
                panic!("{relative} imports forbidden domain {dependency} via {import:?}");
            }
            if let Some((index, _)) = applicable_exceptions.iter().find(|(_, exception)| {
                reviewed_exception_matches(exception, name, &relative, &import)
            }) {
                *exception_uses.entry(*index).or_default() += 1;
                continue;
            }
            panic!("{relative} accesses crate-root facade item {dependency} via {import:?}; use its owning domain");
        }
    }
    for (index, exception) in applicable_exceptions {
        assert_eq!(
            exception_uses.get(&index),
            Some(&1),
            "reviewed exception {} is stale or no longer exact: {} -> {:?}",
            exception.tracking_label,
            exception.source,
            exception.import
        );
    }
}

fn source_module_prefix(
    root: &Path,
    modules: &DeclaredModuleGraph,
    source_root: &Path,
) -> Vec<String> {
    if source_root.is_file() {
        let relative = display_path(root, source_root);
        return std::iter::once("crate".to_owned())
            .chain(declared_module_path(modules, &relative))
            .collect();
    }
    display_path(root, source_root)
        .strip_prefix("crates/rafter-invariants/src/")
        .map(|relative| {
            std::iter::once("crate".to_owned())
                .chain(relative.split('/').map(str::to_owned))
                .collect()
        })
        .unwrap_or_default()
}

fn source_owns_path(source: &str, candidate: &str) -> bool {
    source == candidate
        || candidate
            .strip_prefix(source)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

fn reviewed_exception_matches(
    exception: &ReviewedDomainImportException,
    domain: &str,
    relative: &str,
    path: &[String],
) -> bool {
    exception.owner_domain == domain
        && exception.source == relative
        && path.len() == exception.import.len()
        && path
            .iter()
            .zip(exception.import)
            .all(|(actual, expected)| actual == expected)
}
