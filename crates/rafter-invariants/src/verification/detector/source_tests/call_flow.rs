//! Call-flow scenarios included by the detector source test module.

use super::*;

#[test]
fn self_qualified_helper_cannot_bypass_the_required_invocation() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    self::forge_successful_test_process();
    oracle_expect_err!(detector(), "reject");
}
fn forge_successful_test_process() { std::process::exit(0); }
"#;
    assert!(verify(source)
        .expect_err("self-qualified helper must be recursively inspected")
        .contains("arbitrary detector witness"));
}

#[test]
fn crate_super_and_deep_local_helpers_are_resolved_exactly() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    crate::tests::forge_successful_test_process();
    oracle_expect_err!(detector(), "reject");
}
fn forge_successful_test_process() { std::process::exit(0); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    self::nested::forge_successful_test_process();
    oracle_expect_err!(detector(), "reject");
}
mod nested {
    pub(super) fn forge_successful_test_process() { std::process::exit(0); }
}
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    nested::enter();
    oracle_expect_err!(detector(), "reject");
}
fn forge_successful_test_process() { std::process::exit(0); }
mod nested {
    pub(super) fn enter() { super::forge_successful_test_process(); }
}
"#,
    ] {
        assert!(verify(source)
            .expect_err("every exact local helper path must be recursively inspected")
            .contains("arbitrary detector witness"));
    }
}

#[test]
fn wildcard_alias_parentheses_and_function_values_are_resolved() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { nested::enter(); oracle_expect_err!(detector(), "reject"); }
fn forge_successful_test_process() { std::process::exit(0); }
mod nested {
    use super::*;
    pub(super) fn enter() { forge_successful_test_process(); }
}
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
use self::forge_successful_test_process as forged;
fn fixture() { forged(); oracle_expect_err!(detector(), "reject"); }
fn forge_successful_test_process() { std::process::exit(0); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { (self::forge_successful_test_process)(); oracle_expect_err!(detector(), "reject"); }
fn forge_successful_test_process() { std::process::exit(0); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    let forged = self::forge_successful_test_process;
    forged();
    oracle_expect_err!(detector(), "reject");
}
fn forge_successful_test_process() { std::process::exit(0); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    let mut forged = self::safe;
    forged = self::forge_successful_test_process;
    forged();
    oracle_expect_err!(detector(), "reject");
}
fn safe() {}
fn forge_successful_test_process() { std::process::exit(0); }
"#,
    ] {
        assert!(verify(source)
            .expect_err("indirect local helper must be recursively inspected")
            .contains("arbitrary detector witness"));
    }
}

#[test]
fn associated_const_function_values_cannot_hide_pre_invocation_helpers() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
struct Local;
impl Local { const FORGED: fn() = forge_successful_test_process; }
fn fixture() {
    (Local::FORGED)();
    oracle_expect_err!(detector(), "reject");
}
fn forge_successful_test_process() { std::process::exit(0); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
struct Local;
impl Local {
    const FORGED: fn() = forge_successful_test_process;
    fn enter() { (Self::FORGED)(); }
}
fn fixture() {
    Local::enter();
    oracle_expect_err!(detector(), "reject");
}
fn forge_successful_test_process() { std::process::exit(0); }
"#,
    ] {
        let error = verify(source)
            .expect_err("associated const callables are non-function values, not trusted calls");
        assert!(error.contains("non-function value"), "{error}");
    }
}

#[test]
fn module_aliases_branch_values_and_callable_parameters_fail_closed() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
use self::nested as helpers;
fn fixture() { helpers::forge(); oracle_expect_err!(detector(), "reject"); }
mod nested { pub(super) fn forge() { std::process::exit(0); } }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    let forged = if true { self::forge } else { self::safe };
    forged();
    oracle_expect_err!(detector(), "reject");
}
fn safe() {}
fn forge() { std::process::exit(0); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    let (forged,) = (self::forge,);
    forged();
    oracle_expect_err!(detector(), "reject");
}
fn forge() { std::process::exit(0); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    invoke(self::forge);
    oracle_expect_err!(detector(), "reject");
}
fn invoke(call: fn()) { call(); }
fn forge() { std::process::exit(0); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    invoke(factory());
    oracle_expect_err!(detector(), "reject");
}
fn invoke(call: fn(i32) -> !) { call(0); }
fn factory() -> fn(i32) -> ! { std::process::exit }
"#,
    ] {
        assert!(
            verify(source).is_err(),
            "accepted opaque callable: {source}"
        );
    }
}

#[test]
fn local_methods_and_associated_functions_are_recursively_inspected() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
struct Local;
impl Local { fn forge(&self) { std::process::exit(0); } }
fn fixture() { Local.forge(); oracle_expect_err!(detector(), "reject"); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
struct Local;
impl Local { fn forge() { std::process::exit(0); } }
fn fixture() { Local::forge(); oracle_expect_err!(detector(), "reject"); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
struct Local;
impl Local { fn forge(&self) { std::process::exit(0); } }
fn invoke(value: &Local) { value.forge(); }
fn fixture() { let value = Local; invoke(&value); oracle_expect_err!(detector(), "reject"); }
"#,
    ] {
        assert!(verify(source)
            .expect_err("local method bodies must be reachable")
            .contains("arbitrary detector witness"));
    }
}

#[test]
fn non_path_receivers_with_plausible_local_methods_fail_closed() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
mod nested {
    pub(super) struct Stop;
    impl Stop { pub(super) fn stop(&self) { panic!("stop"); } }
    pub(super) fn make() -> Stop { Stop }
}
fn fixture() {
    nested::make().stop();
    oracle_expect_err!(detector(), "reject");
}
"#;

    let error = verify(source)
        .expect_err("a factory receiver must resolve locally or remain an opaque local call");
    assert!(error.contains("unresolved local call `stop`"), "{error}");
}

#[test]
fn field_receivers_do_not_fall_back_to_self_methods_with_the_same_name() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;

struct Inner;
impl Inner {
    fn committed_membership(&self) {}
}

struct Outer {
    inner: Inner,
}

impl Outer {
    fn committed_membership(&self) {
        self.snapshot_committed_membership();
    }

    fn snapshot_committed_membership(&self) {
        self.inner.committed_membership();
    }
}

fn make_outer() -> Outer {
    Outer { inner: Inner }
}

fn fixture() {
    let value: Outer = make_outer();
    value.snapshot_committed_membership();
    oracle_expect_err!(detector(), "reject");
}
"#;

    verify(source).expect("a field receiver must resolve through the field type, not Self");
}

#[path = "call_flow/reachability.rs"]
mod reachability;

#[path = "call_flow/source_boundaries.rs"]
mod source_boundaries;

macro_rules! delegated_test {
    ($module:ident, $name:ident) => {
        #[test]
        fn $name() {
            $module::$name();
        }
    };
}

delegated_test!(
    reachability,
    invocation_trailing_arguments_must_complete_before_the_witness
);
delegated_test!(
    reachability,
    panic_and_unconditional_loop_make_later_invocations_unreachable
);
delegated_test!(
    reachability,
    conditional_helper_panic_does_not_hide_a_successful_bypass
);
delegated_test!(
    reachability,
    conditional_non_returning_helper_makes_later_invocation_conditional
);
delegated_test!(
    reachability,
    conditional_divergence_inside_helper_makes_caller_invocation_conditional
);
delegated_test!(
    reachability,
    conditional_callable_divergence_makes_caller_invocation_conditional
);
delegated_test!(
    reachability,
    conditional_recursion_makes_later_invocation_conditional
);
delegated_test!(
    reachability,
    helper_loop_with_a_guaranteed_break_can_return_to_the_fixture
);
delegated_test!(
    reachability,
    helper_loop_with_a_literal_true_conditional_break_can_return
);
delegated_test!(
    reachability,
    recursive_invocation_helpers_are_rejected_until_multiplicity_is_bounded
);
delegated_test!(
    reachability,
    closure_local_return_does_not_exit_the_enclosing_fixture
);
delegated_test!(
    source_boundaries,
    block_scoped_imports_are_resolved_without_cross_function_contamination
);
delegated_test!(
    source_boundaries,
    out_of_line_local_helper_calls_fail_closed
);
delegated_test!(
    source_boundaries,
    benign_custom_exec_method_does_not_trigger_process_exit_guard
);
