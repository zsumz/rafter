//! Source identity, cache invalidation, and detector name-resolution scenarios.

use super::*;

pub(super) fn analyzed_fixture_source_must_own_the_exact_executed_test_identity() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/other.rs"),
        "#[rafter_invariant_test::detector_test]\nfn fixture() {}\n",
    )
    .expect("write same-leaf executed decoy");
    let mut identity = synthetic_identity();
    identity.test_name = "other::fixture".to_owned();
    let error = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &identity,
        fixture: "fixture",
        detector: "detector",
    })
    .expect_err("analyzed source and executed test identity must be inseparable");
    assert!(error.contains("has no declaration"), "{error}");
}

pub(super) fn conditionally_compiled_detector_fixtures_are_rejected() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
#[cfg(any())]
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#;
    assert!(verify(source)
        .expect_err("a cfg-disabled registered fixture must fail closed")
        .contains("conditional compilation"));
}

pub(super) fn auxiliary_rejection_cannot_qualify_a_registered_recorder() {
    let detector_source = r"
fn detector() {}
fn auxiliary() -> Result<(), ()> { Err(()) }
";
    let source = detector_fixture(
        r#"
use crate::detector::{auxiliary, detector};
use rafter_invariant_test::{oracle_expect_err, oracle_invoke_recorder};
fn fixture() {
    oracle_invoke_recorder!(detector());
    oracle_expect_err!(auxiliary(), "reject");
}
"#,
    );
    let root = synthetic_workspace(&source, detector_source);
    fs::write(
        root.join("crates/fixture/src/other.rs"),
        "fn auxiliary() -> Result<(), ()> { Err(()) }\n",
    )
    .expect("write same-leaf decoy");
    let error = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect_err("an unrelated rejecting detector cannot qualify a registered recorder");
    assert!(
        error.contains("does not invoke registered detector"),
        "{error}"
    );
}

pub(super) fn supplied_source_must_match_its_bound_path() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    let error = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: "#[rafter_invariant_test::detector_test]\nfn fixture() {}\n",
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect_err("in-memory source must not drift from the bound file");
    assert!(error.contains("does not match bound path"), "{error}");
}

pub(super) fn target_analysis_cache_invalidates_when_bound_source_changes() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    let fixture_path = root.join(FIXTURE_PATH);
    let detector_path = root.join(DETECTOR_PATH);
    let identity = synthetic_identity();
    let mut batch = DetectorFixtureAnalysis::default();
    batch
        .validate(&crate::DetectorFixtureSourceBinding {
            fixture_source: &source,
            detector_source: DETECTOR_SOURCE,
            source_root: &root,
            fixture_path: &fixture_path,
            detector_path: &detector_path,
            test_identity: &identity,
            fixture: "fixture",
            detector: "detector",
        })
        .expect("initial target analysis");

    let changed = "#[cfg(any())]\nfn detector() -> Result<(), ()> { Err(()) }\n";
    fs::write(&detector_path, changed).expect("replace bound detector source");
    let error = batch
        .validate(&crate::DetectorFixtureSourceBinding {
            fixture_source: &source,
            detector_source: changed,
            source_root: &root,
            fixture_path: &fixture_path,
            detector_path: &detector_path,
            test_identity: &identity,
            fixture: "fixture",
            detector: "detector",
        })
        .expect_err("source mutation must invalidate cached target semantics");
    assert!(error.contains("does not resolve"), "{error}");
    assert_eq!(batch.target_analysis_count(), 2);
}

pub(super) fn qualified_calls_are_not_rebound_to_same_leaf_local_helpers() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { crate::external::emit(); }
fn emit() { oracle_expect_err!(detector(), "reject"); }
"#;
    assert!(verify(source)
        .expect_err("qualified external call must not reach a local same-leaf helper")
        .contains("does not invoke registered detector"));
}

pub(super) fn exact_inline_fixture_identity_ignores_same_leaf_decoys() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
mod selected {
    use super::*;
    fn fixture() { oracle_expect_err!(detector(), "reject"); }
}
mod decoy { fn fixture() {} }
"#;
    let mut identity = synthetic_identity();
    identity.test_name = "tests::selected::fixture".to_owned();
    verify_with_identity(source, &identity)
        .expect("the exact inline fixture identity must not be leaf-global");
}

pub(super) fn inline_detector_uses_its_exact_compiler_identity() {
    let source = detector_fixture(
        r#"
use rafter_invariant_test::oracle_expect_err;
mod nested {
    pub(super) fn detector() -> Result<(), ()> { Err(()) }
}
use self::nested::detector;
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#,
    );
    let root = synthetic_workspace(&source, "");
    let fixture_path = root.join(FIXTURE_PATH);
    let contract = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: &source,
        source_root: &root,
        fixture_path: &fixture_path,
        detector_path: &fixture_path,
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect("inline detector must resolve by exact module identity");
    assert_eq!(
        contract.registered_identity(),
        "fixture::tests::nested::detector"
    );
}

pub(super) fn detector_declaration_must_belong_to_its_registered_source_path() {
    let source = detector_fixture(
        r#"
use rafter_invariant_test::oracle_expect_err;
fn detector() -> Result<(), ()> { Err(()) }
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#,
    );
    let root = synthetic_workspace(&source, "fn unrelated() {}\n");
    let error = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: "fn unrelated() {}\n",
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect_err("a fixture-local detector cannot satisfy another registered source path");
    assert!(error.contains("not its bound fixture source"), "{error}");
}

pub(super) fn imported_detector_identity_ignores_same_leaf_module_decoys() {
    let detector_source = r"
pub(super) mod selected { pub fn detector() -> Result<(), ()> { Err(()) } }
mod decoy { fn detector() -> Result<(), ()> { Ok(()) } }
";
    let source = detector_fixture(
        r#"
use crate::detector::selected::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#,
    );
    let root = synthetic_workspace(&source, detector_source);
    let contract = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect("the exact detector import must select one same-leaf declaration");
    assert_eq!(
        contract.registered_identity(),
        "fixture::detector::selected::detector"
    );
}

pub(super) fn module_value_bindings_cannot_shadow_the_registered_detector() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
const detector: fn() -> Result<(), ()> = fake;
fn fake() -> Result<(), ()> { Err(()) }
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#;
    assert!(verify(source)
        .expect_err("module const shadow must fail")
        .contains("shadowed"));
}

pub(super) fn shadowing_and_same_leaf_declarations_are_rejected() {
    let shadow = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { let detector = || Err::<(), ()>(()); oracle_expect_err!(detector(), "reject"); }
"#;
    assert!(verify(shadow)
        .expect_err("closure shadow must fail")
        .contains("shadows"));

    let local_impostor = r#"
use rafter_invariant_test::oracle_expect_err;
fn fixture() { oracle_expect_err!(detector(), "reject"); }
fn detector() -> Result<(), ()> { Err(()) }
"#;
    let error = verify(local_impostor)
        .expect_err("same-leaf local and bound detector declarations must fail");
    assert!(error.contains("not its bound fixture source"), "{error}");
}

pub(super) fn explicit_external_import_takes_precedence_over_detector_glob() {
    let source = r#"
use external_crate::detector;
use crate::detector::*;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#;

    assert!(
        verify(source).is_err(),
        "an explicit external import must not resolve through an in-tree glob"
    );
}

pub(super) fn a_forged_helper_is_rejected_even_when_a_real_invocation_is_found_first() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { oracle_expect_err!(detector(), "reject"); forged(); }
fn forged() { __oracle_detector_witness("detector()"); }
"#;
    assert!(verify(source)
        .expect_err("all reachable helpers must be inspected")
        .contains("arbitrary detector witness"));
}
