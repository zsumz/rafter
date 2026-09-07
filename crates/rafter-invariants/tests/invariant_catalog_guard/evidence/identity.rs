//! Registered test identities and the typed oracle each one must execute.
//!
//! An identity must name exactly one `#[test]` function without
//! `#[should_panic]` at the module path its file actually occupies, and that
//! function must reach a trusted oracle macro bound to the helper crate rather
//! than a locally shadowed one.

use std::{collections::BTreeSet, path::Path};

use syn::{visit::Visit, ItemFn, ItemMacro, ItemMod};

use super::imports::{declares_local_oracle_macro, imported_paths, trusted_oracle_imports};
use super::module_graph::test_source_module_path;
use super::{Evidence, OracleMacroVisitor, RegisteredTestVisitor};

pub(super) fn assert_registered_test_contract(
    workspace: &Path,
    record: &Evidence,
    path: &Path,
    source: &str,
) {
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

pub(super) fn assert_cargo_test_target_matches_path(record: &Evidence) {
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

pub(super) fn cargo_integration_test_root(identity: &rafter_invariants::TestIdentity) -> String {
    format!("crates/{}/tests/{}.rs", identity.package, identity.target)
}

pub(super) fn test_identity_matches_source(
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

pub(super) fn test_identity_uses_typed_oracle(
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
        || source.contains("RAFTER_INVARIANT_DETECTOR_WITNESS:")
        || declares_local_oracle_macro(&file)
    {
        return false;
    }
    let trusted_macros = trusted_oracle_imports(&file);
    let qualified_crate_trusted = !imported_paths(&file).local_rafter_invariant_test;
    let Some(module) = test_source_module_path(workspace, Path::new(fixture_path), identity) else {
        return false;
    };
    let mut visitor = TypedOracleTestVisitor {
        symbol: fixture,
        test_name: &identity.test_name,
        module,
        inline_modules: Vec::new(),
        trusted_macros,
        qualified_crate_trusted,
        matches: 0,
    };
    visitor.visit_file(&file);
    visitor.matches == 1
}

pub(super) struct TypedOracleTestVisitor<'a> {
    symbol: &'a str,
    test_name: &'a str,
    module: Vec<String>,
    inline_modules: Vec<String>,
    trusted_macros: BTreeSet<String>,
    qualified_crate_trusted: bool,
    matches: usize,
}

impl Visit<'_> for TypedOracleTestVisitor<'_> {
    fn visit_item_fn(&mut self, function: &ItemFn) {
        if function.sig.ident == self.symbol && self.current_test_name() == self.test_name {
            let mut oracle = OracleMacroVisitor {
                trusted_macros: &self.trusted_macros,
                qualified_crate_trusted: self.qualified_crate_trusted,
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
