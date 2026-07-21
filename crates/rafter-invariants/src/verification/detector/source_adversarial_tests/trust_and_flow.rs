//! Trust-path and pre-invocation control-flow adversarial scenarios.

use super::{fs, verify, verify_decorated, verify_invocation_bound_detector};

#[test]
fn compiled_qualified_helper_fixture_cannot_forge_detector_proof() {
    let fixture = "qualified_helper_forged_transcript_subprocess_fixture";
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let source_root = fs::canonicalize(root).expect("canonicalize source root");
    let fixture_path =
        fs::canonicalize(source_root.join("crates/rafter-invariant-test/src/tests.rs"))
            .expect("canonicalize fixture source");
    let fixture_source = fs::read_to_string(&fixture_path).expect("read fixture source");
    let identity = crate::contract::TestIdentity {
        package: "rafter-invariant-test".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_invariant_test".to_owned(),
        test_name: format!("tests::{fixture}"),
    };

    let error = verify_invocation_bound_detector(
        &crate::verification::detector::DetectorFixtureSourceBinding {
            fixture_source: &fixture_source,
            detector_source: &fixture_source,
            source_root: &source_root,
            fixture_path: &fixture_path,
            detector_path: &fixture_path,
            test_identity: &identity,
            fixture,
            detector: "token_bound_regression_detector",
        },
    )
    .expect_err("qualified helper forgery must fail closed");

    assert!(
        error.contains("can emit an arbitrary detector witness"),
        "{error}"
    );
}

#[test]
fn glob_import_cannot_substitute_detector_test_attribute() {
    let source = r#"
mod forged { pub mod rafter_invariant_test {} }
use forged::*;
use crate::detector::detector;
#[rafter_invariant_test::detector_test]
fn fixture() {
    ::rafter_invariant_test::oracle_expect_err!(detector(), "reject");
}
"#;
    let error = verify_decorated(source).expect_err("glob-substituted attribute must fail closed");
    assert!(error.contains("untrusted semantic attribute"), "{error}");
}

#[test]
fn glob_import_cannot_substitute_qualified_oracle_macro() {
    let source = r#"
mod forged { pub mod rafter_invariant_test {} }
use forged::*;
use crate::detector::detector;
#[::rafter_invariant_test::detector_test]
fn fixture() {
    rafter_invariant_test::oracle_expect_err!(detector(), "reject");
}
"#;
    let error = verify_decorated(source).expect_err("glob-substituted oracle must fail closed");
    assert!(error.contains("untrusted oracle macro"), "{error}");
}

#[test]
fn absolute_trust_paths_remain_bound_under_globs() {
    let source = r#"
mod unrelated { pub fn value() {} }
use unrelated::*;
use crate::detector::detector;
#[::rafter_invariant_test::detector_test]
fn fixture() {
    ::rafter_invariant_test::oracle_expect_err!(detector(), "reject");
}
"#;
    verify_decorated(source).expect("absolute trust paths bypass relative glob ambiguity");
}

#[test]
fn explicit_unqualified_oracle_import_remains_trusted_under_globs() {
    let source = r#"
mod unrelated { pub fn value() {} }
use unrelated::*;
use crate::detector::detector;
use ::rafter_invariant_test::oracle_expect_err;
#[::rafter_invariant_test::detector_test]
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#;
    verify_decorated(source).expect("an exact explicit macro import is not replaced by a glob");
}

#[test]
fn detector_arguments_must_fall_through_before_the_invocation_is_credited() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { oracle_expect_err!(detector({ return; }), "reject"); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn stop() -> i32 { panic!("stop") }
fn sink(_: i32) {}
fn fixture() { sink(stop()); oracle_expect_err!(detector(), "reject"); }
"#,
    ] {
        assert!(
            verify(source).is_err(),
            "accepted diverging source: {source}"
        );
    }
}

#[test]
fn conditional_non_returning_helpers_make_later_invocations_conditional() {
    for stop in [r#"fn stop() { panic!("stop"); }"#, "fn stop() { loop {} }"] {
        let source = format!(
            r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
{stop}
fn fixture() {{
    if std::hint::black_box(false) {{ stop(); }}
    oracle_expect_err!(detector(), "reject");
}}
"#,
        );
        let error = verify(&source)
            .expect_err("conditional divergence must downgrade later invocation credit");
        assert!(error.contains("conditional control flow"), "{error}");
    }
}
