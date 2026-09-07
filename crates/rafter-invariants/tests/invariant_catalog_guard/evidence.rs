//! Proves every evidence record names something a machine can find.
//!
//! Each record must bind to a known invariant and layer, and its path, symbol,
//! and test identity must resolve to a real declaration in the workspace —
//! never an import, an alias, or a `should_panic` test. It proves the binding
//! exists; whether running that evidence passes is not asked here.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use syn::{visit::Visit, ItemFn, ItemMacro, ItemMod, Macro};

use super::{Clause, Entry, Evidence, COVERAGE_LAYERS, VALID_EVIDENCE_STRENGTHS};

#[path = "evidence/call_closure.rs"]
mod call_closure;
#[path = "evidence/coverage.rs"]
mod coverage;
#[path = "evidence/identity.rs"]
mod identity;
#[path = "evidence/imports.rs"]
mod imports;
#[path = "evidence/module_graph.rs"]
mod module_graph;
#[path = "evidence/negative_fixture.rs"]
mod negative_fixture;
#[path = "evidence/symbols.rs"]
mod symbols;

use coverage::{
    assert_atomic_group_policy, assert_coverage_bindings,
    assert_pr_model_check_inventory_is_registered,
};
use identity::{
    assert_cargo_test_target_matches_path, assert_registered_test_contract,
    cargo_integration_test_root, test_identity_matches_source,
};
use imports::{declares_local_oracle_macro, is_oracle_macro, trusted_oracle_imports};
use negative_fixture::assert_negative_fixture_policy;
use symbols::source_declares_symbol;

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
    let mut detector_sources = rafter_invariants::DetectorFixtureAnalysis::default();

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
                record.negative_fixture_detector_path.as_deref(),
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
        assert_negative_fixture_policy(workspace, record, &source, &mut detector_sources);
        assert_atomic_group_policy(record);
    }

    assert_coverage_bindings(entries, clauses, evidence);
    assert_pr_model_check_inventory_is_registered(workspace, evidence);
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
                function.attrs.iter().any(|attribute| {
                    attribute.path().is_ident("test")
                        || attribute
                            .path()
                            .segments
                            .iter()
                            .map(|segment| segment.ident.to_string())
                            .eq(["rafter_invariant_test", "detector_test"].map(str::to_owned))
                }),
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

struct OracleMacroVisitor<'a> {
    trusted_macros: &'a BTreeSet<String>,
    qualified_crate_trusted: bool,
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
            let qualified = self.qualified_crate_trusted
                && segments.as_slice() == ["rafter_invariant_test", name.as_str()];
            let imported = segments.len() == 1 && self.trusted_macros.contains(name);
            self.found |= qualified || imported;
            self.untrusted |= !qualified && !imported;
        }
        syn::visit::visit_macro(self, invocation);
    }
}

#[cfg(test)]
#[path = "evidence_tests.rs"]
mod tests;
