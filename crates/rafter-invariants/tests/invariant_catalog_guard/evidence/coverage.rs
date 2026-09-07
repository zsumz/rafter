//! Coverage bindings, atomic clause sets, and the PR model-check inventory.
//!
//! A required clause must name direct executable evidence, a declared coverage
//! cell must have a record of that strength, a multi-clause direct simulator
//! row must carry a reviewed atomic group, and every check the PR runner emits
//! must be claimed by some evidence row.

use std::{collections::BTreeSet, fs, path::Path};

use syn::{visit::Visit, Expr, ExprCall, Item, Lit};

use super::{Clause, Entry, Evidence, COVERAGE_LAYERS};

pub(super) fn assert_pr_model_check_inventory_is_registered(
    workspace: &Path,
    evidence: &[Evidence],
) {
    let path = workspace.join("crates/rafter-sim/src/bin/rafter_model_check_fast/runner/checks.rs");
    let source = fs::read_to_string(&path)
        .unwrap_or_else(|error| panic!("read PR model-check runner {}: {error}", path.display()));
    let file = syn::parse_file(&source)
        .unwrap_or_else(|error| panic!("parse PR model-check runner {}: {error}", path.display()));
    let function = file
        .items
        .iter()
        .find_map(|item| match item {
            Item::Fn(function) if function.sig.ident == "run_fast_profile" => Some(function),
            _ => None,
        })
        .expect("PR model-check runner must define run_fast_profile");
    let mut visitor = ModelCheckCallVisitor::default();
    visitor.visit_item_fn(function);
    assert!(
        !visitor.checks.is_empty(),
        "PR model-check inventory is empty"
    );
    assert_eq!(
        visitor.calls,
        visitor.checks.len(),
        "every PR run_raft_check call must use a unique literal check ID",
    );

    let claimed = evidence
        .iter()
        .filter_map(|record| record.simulator.as_ref())
        .flat_map(|identity| identity.checks.iter().map(String::as_str))
        .collect::<BTreeSet<_>>();
    let unclaimed = visitor
        .checks
        .iter()
        .map(String::as_str)
        .filter(|check| !claimed.contains(check))
        .collect::<Vec<_>>();
    assert!(
        unclaimed.is_empty(),
        "PR model-check runner emits check IDs absent from invariant evidence: {}",
        unclaimed.join(", "),
    );
}

#[derive(Default)]
pub(super) struct ModelCheckCallVisitor {
    calls: usize,
    checks: BTreeSet<String>,
}

impl<'ast> Visit<'ast> for ModelCheckCallVisitor {
    fn visit_expr_call(&mut self, call: &'ast ExprCall) {
        let is_model_check = matches!(
            call.func.as_ref(),
            Expr::Path(path)
                if path.path.segments.last().is_some_and(|segment| segment.ident == "run_raft_check")
        );
        if is_model_check {
            self.calls += 1;
            if let Some(Expr::Lit(literal)) = call.args.first() {
                if let Lit::Str(check) = &literal.lit {
                    self.checks.insert(check.value());
                }
            }
        }
        syn::visit::visit_expr_call(self, call);
    }
}

pub(super) fn assert_atomic_group_policy(record: &Evidence) {
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

pub(super) fn assert_coverage_bindings(
    entries: &[Entry],
    clauses: &[Clause],
    evidence: &[Evidence],
) {
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

pub(super) fn evidence_strength_for_coverage(coverage: &str) -> Option<&'static str> {
    let coverage = coverage.trim();
    if coverage == "D" || coverage.starts_with("D:") {
        Some("direct")
    } else if coverage == "E2E" || coverage.starts_with("E2E:") {
        Some("e2e")
    } else {
        None
    }
}
