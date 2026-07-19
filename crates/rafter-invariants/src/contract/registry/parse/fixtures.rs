//! Minimal valid registry fragments for parser contract tests.

pub(super) const VALID_INVARIANT: &str = r#"invariants:
  - id: "AA-01"
    kind: "safety"
    family: "test"
    tier: "feature"
    priority: "p1"
    title: "Test invariant"
    statement: "The statement holds."
    scope: "Test scope."
    assumptions: "Test assumptions."
    current_coverage:
      tla: "none"
      simulator: "direct"
      tests: "direct"
      maelstrom: "none"
    action_class: "retain"
    next_action: "Keep testing."
"#;

pub(super) const VALID_EVIDENCE: &str = r#"evidence:
  - id: "AA-01"
    clauses: "AA-01.a"
    layer: "tests"
    strength: "direct"
    path: "src/lib.rs"
    symbol: "test_symbol"
    package: "test-package"
    target_kind: "lib"
    target: "test_package"
    test_name: "tests::test_symbol"
"#;

pub(super) const VALID_CLAUSE: &str = r#"clauses:
  - id: "AA-01.a"
    invariant_id: "AA-01"
    statement: "The clause holds."
    scope: "Test scope."
    assumptions: "Test assumptions."
    required: "true"
"#;

pub(super) const VALID_ATOMIC_SIMULATOR_EVIDENCE: &str = r#"evidence:
  - id: "CM-03"
    clauses: "CM-03.a,CM-03.b"
    layer: "simulator"
    strength: "direct"
    path: "src/model.rs"
    symbol: "check_atomic_rule"
    atomic_group: "CM-03/current-term-commit-point"
    simulator_check: "model-check"
    minimum_protocol_states: "1"
    minimum_verifier_states: "1"
    required_observation: "atomic_rule_checks"
    minimum_observation: "1"
    negative_fixture: "atomic_rule_rejects_mutation"
    negative_fixture_path: "src/model/tests.rs"
    negative_fixture_detector: "check_atomic_rule"
    negative_fixture_package: "test-package"
    negative_fixture_target_kind: "lib"
    negative_fixture_target: "test_package"
    negative_fixture_test_name: "tests::atomic_rule_rejects_mutation"
"#;
