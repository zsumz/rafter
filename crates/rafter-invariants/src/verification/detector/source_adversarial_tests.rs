//! Adversarial source fixtures that must fail closed.

use std::fs;

use super::{
    tests::{
        detector_fixture, synthetic_identity, synthetic_workspace, verify, verify_decorated,
        DETECTOR_PATH, DETECTOR_SOURCE, FIXTURE_PATH,
    },
    verify_invocation_bound_detector,
};

#[path = "source_adversarial_tests/trust_and_flow.rs"]
mod trust_and_flow;

#[test]
fn invocation_macros_bind_the_exact_function_not_a_same_leaf_decoy() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
mod decoy { pub(super) fn detector() -> Result<(), ()> { Err(()) } }
fn fixture() {
    use self::decoy::detector;
    oracle_expect_err!(detector(), "reject");
}
"#;
    let error = verify(source).expect_err("same-leaf decoy must not receive registered identity");
    assert!(
        error.contains("does not invoke registered detector"),
        "{error}"
    );
}

#[test]
fn imported_target_helpers_are_resolved_and_non_returning_helpers_block_credit() {
    let source = detector_fixture(
        r#"
use crate::{detector::detector, other::stop};
use rafter_invariant_test::oracle_expect_err;
fn fixture() { stop(); oracle_expect_err!(detector(), "reject"); }
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/other.rs"),
        "pub fn stop() { panic!(\"stop\"); }\n",
    )
    .expect("write sibling helper");
    let error = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect_err("non-returning sibling helper must block the later invocation");
    assert!(
        error.contains("does not invoke registered detector"),
        "{error}"
    );
}

#[test]
fn absolute_self_crate_alias_helpers_are_inspected_before_later_invocations() {
    let source = detector_fixture(
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    ::fixture_alias::other::stop();
    oracle_expect_err!(detector(), "reject");
}
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"extern crate self as fixture_alias;
#[path = "mapped_detector.rs"]
mod detector;
mod other;
#[cfg(test)]
mod tests;
"#,
    )
    .expect("write crate-root self alias");
    fs::write(
        root.join("crates/fixture/src/other.rs"),
        "pub(crate) fn stop() { panic!(\"stop\"); }\n",
    )
    .expect("write non-returning aliased helper");

    let error = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect_err("absolute self-crate aliases must not hide pre-invocation helpers");
    assert!(
        error.contains("does not invoke registered detector"),
        "{error}"
    );
}

#[test]
fn indirect_callables_fail_closed_while_exact_alias_chains_remain_bound() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    let callback: fn() -> Result<(), ()> = detector;
    (*callback)();
    oracle_expect_err!(detector(), "reject");
}
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn factory() -> fn() -> Result<(), ()> { detector }
fn fixture() { (factory())(); oracle_expect_err!(detector(), "reject"); }
"#,
    ] {
        assert!(
            verify(source).is_err(),
            "accepted indirect callable: {source}"
        );
    }

    let aliases = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    let first = detector;
    let second = first;
    oracle_expect_err!(second(), "reject");
}
"#;
    verify(aliases).expect("an exact transitive alias retains detector identity");
}

#[test]
fn reachable_helpers_cannot_hide_indirect_callables() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn factory() -> fn() { || std::process::exit(0) }
fn helper() { let callback = factory(); callback(); }
fn fixture() { helper(); oracle_expect_err!(detector(), "reject"); }
"#;
    let error = verify(source).expect_err("opaque helper callables must fail closed");
    assert!(error.contains("unresolved local callable"), "{error}");
}

#[test]
fn imported_non_function_value_cannot_substitute_the_detector() {
    let source = r#"
use crate::detector::*;
use rafter_invariant_test::oracle_expect_err;
mod forged {
    fn decoy() -> Result<(), ()> { Err(()) }
    pub const detector: fn() -> Result<(), ()> = decoy;
}
use forged::detector;
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#;
    let error = verify(source).expect_err("a const function pointer must not bind as a detector");
    assert!(error.contains("non-function value declarations"), "{error}");
}

#[test]
fn nested_assignment_updates_outer_alias_identity() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn decoy() -> Result<(), ()> { Err(()) }
fn fixture() {
    let mut callback = detector;
    { callback = decoy; }
    oracle_expect_err!(callback(), "reject");
}
"#;
    let error = verify(source).expect_err("nested assignment must replace the outer alias");
    assert!(
        error.contains("does not invoke registered detector"),
        "{error}"
    );
}

#[test]
fn qualified_trait_dispatch_and_local_trait_defaults_fail_closed() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
trait Invoke { fn detector() -> Result<(), ()>; }
struct Value;
impl Invoke for Value { fn detector() -> Result<(), ()> { Err(()) } }
fn fixture() {
    oracle_expect_err!(<Value as Invoke>::detector(), "reject");
    oracle_expect_err!(detector(), "registered");
}
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
trait Stop { fn stop(&self) { panic!("stop") } }
struct Value;
impl Stop for Value {}
fn fixture() { Value.stop(); oracle_expect_err!(detector(), "reject"); }
"#,
    ] {
        assert!(
            verify(source).is_err(),
            "accepted opaque trait dispatch: {source}"
        );
    }
}

#[test]
fn unresolved_process_replacement_calls_fail_closed() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
use std::os::unix::process::CommandExt;
fn helper() { let _ = std::process::Command::new("true").exec(); }
fn fixture() { helper(); oracle_expect_err!(detector(), "reject"); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn helper() {
    let mut command = std::process::Command::new("true");
    let _ = std::os::unix::process::CommandExt::exec(&mut command);
}
fn fixture() { helper(); oracle_expect_err!(detector(), "reject"); }
"#,
    ] {
        let error = verify(source).expect_err("process replacement before the oracle must fail");
        assert!(error.contains("process-replacement"), "{error}");
    }
}

#[test]
fn foreign_code_cannot_intercept_the_proof_descriptor_before_the_detector() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
unsafe extern "C" { fn forge_detector_proof() -> !; }
fn fixture() {
    unsafe { forge_detector_proof() }
    oracle_expect_err!(detector(), "reject");
}
"#;

    let error = verify(source).expect_err("foreign proof helper must fail closed");
    assert!(error.contains("unsafe or foreign code"), "{error}");
}

#[test]
fn renamed_self_module_alias_resolves_exactly() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
mod nested { pub(super) fn setup() {} }
use self::nested::{self as helpers};
fn fixture() { helpers::setup(); oracle_expect_err!(detector(), "reject"); }
"#;
    verify(source).expect("renamed self import must resolve to the nested module");
}

#[test]
fn aliased_external_termination_and_output_calls_are_rejected() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
use std::process::exit as quit;
fn fixture() { quit(0); oracle_expect_err!(detector(), "reject"); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
use std::io::Write::write_all as emit;
fn fixture() {
    let mut sink = std::io::sink();
    let _ = emit(&mut sink, b"forged");
    oracle_expect_err!(detector(), "reject");
}
"#,
    ] {
        let error = verify(source).expect_err("aliased forbidden call must be rejected");
        assert!(error.contains("arbitrary detector witness"), "{error}");
    }
}

#[test]
fn let_else_makes_later_invocations_conditional() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    let value = Some(1);
    let Some(_) = value else { return };
    oracle_expect_err!(detector(), "reject");
}
"#;
    assert!(
        verify(source).is_err(),
        "accepted conditional invocation: {source}"
    );
}

#[test]
fn trusted_assertions_preserve_the_passing_receipt_path() {
    for assertion in [
        "assert!(std::hint::black_box(true));",
        "assert_eq!(1, 1);",
        "assert_ne!(1, 2);",
        "debug_assert!(true);",
        "debug_assert_eq!(1, 1);",
        "debug_assert_ne!(1, 2);",
    ] {
        let source = format!(
            r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {{ {assertion} oracle_expect_err!(detector(), "reject"); }}
"#
        );
        verify(&source).expect("a passing trusted assertion falls through to the invocation");
    }

    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn forged() -> bool { std::process::exit(0); }
fn fixture() { assert!(forged()); oracle_expect_err!(detector(), "reject"); }
"#;
    let error = verify(source).expect_err("assertion arguments must still be inspected");
    assert!(error.contains("arbitrary detector witness"), "{error}");
}

#[test]
fn block_local_detector_and_oracle_macro_shadow_trusted_imports() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    fn detector() -> Result<(), ()> { Err(()) }
    oracle_expect_err!(detector(), "reject");
}
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
    oracle_expect_err!(detector(), "reject");
}
"#,
    ] {
        assert!(
            verify(source).is_err(),
            "accepted block-local shadow: {source}"
        );
    }
}

#[test]
fn unrelated_function_imports_do_not_contaminate_fixture_provenance() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn unrelated() { use rafter_invariant_test::oracle_expect_err; }
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#;
    verify(source).expect("an unrelated lexical import cannot poison fixture provenance");
}
