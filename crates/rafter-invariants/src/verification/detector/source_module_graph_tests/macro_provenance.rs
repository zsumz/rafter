//! Oracle-macro lexical provenance and cached target-analysis scenarios.

use super::*;

pub(super) fn parent_oracle_macro_shadowing_is_inherited_by_child_modules() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
mod other;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
#[cfg(test)]
mod tests;
"#,
    )
    .expect("write parent-scoped macro shadow");

    let error = verify_module_graph(&source, &root)
        .expect_err("parent lexical macros must shadow child imports during analysis");
    assert!(error.contains("shadows a trusted oracle macro"), "{error}");
}

pub(super) fn inherited_oracle_macro_shadowing_is_rejected_for_reachable_helper_identity() {
    let source = detector_fixture(
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    crate::shadowed_helpers::helper();
    oracle_expect_err!(detector(), "reject");
}
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
#[cfg(test)]
mod tests;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
mod shadowed_helpers;
"#,
    )
    .expect("write helper-only inherited oracle shadow");
    fs::write(
        root.join("crates/fixture/src/shadowed_helpers.rs"),
        r#"use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
pub(crate) fn helper() {
    oracle_expect_err!(detector(), "shadowed");
}
"#,
    )
    .expect("write shadowed reachable helper");
    track_workspace_paths(&root, &["crates/fixture/src/shadowed_helpers.rs"]);

    let error = verify_module_graph(&source, &root)
        .expect_err("reachable helper identities must inherit oracle macro shadows");
    assert!(error.contains("shadows a trusted oracle macro"), "{error}");
    assert!(error.contains("shadowed_helpers::helper"), "{error}");
}

fn verify_shadowed_impl_method(call: &str, method: &str) {
    let source = detector_fixture(&format!(
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {{
    {call}
    oracle_expect_err!(detector(), "reject");
}}
"#,
    ));
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
#[cfg(test)]
mod tests;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
mod shadowed_methods;
"#,
    )
    .expect("write impl-only inherited oracle shadow");
    fs::write(
        root.join("crates/fixture/src/shadowed_methods.rs"),
        r#"use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;

pub(crate) struct Helper;

impl Helper {
    pub(crate) fn associated() {
        oracle_expect_err!(detector(), "shadowed associated method");
    }

    pub(crate) fn receiver(&self) {
        oracle_expect_err!(detector(), "shadowed receiver method");
    }
}
"#,
    )
    .expect("write shadowed impl methods");
    track_workspace_paths(&root, &["crates/fixture/src/shadowed_methods.rs"]);

    let error = verify_module_graph(&source, &root)
        .expect_err("reachable impl methods must inherit oracle macro shadows");
    assert!(error.contains("shadows a trusted oracle macro"), "{error}");
    assert!(
        error.contains(&format!("shadowed_methods::Helper::{method}")),
        "{error}"
    );
}

pub(super) fn inherited_oracle_macro_shadowing_tracks_absolute_alias_impl_method_identity() {
    let source = detector_fixture(
        r#"
extern crate self as fixture_alias;
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
use crate::shadowed_methods::Local;
fn fixture() {
    Local::forge();
    oracle_expect_err!(detector(), "reject");
}
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
#[cfg(test)]
mod tests;
extern crate self as fixture_alias;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
mod shadowed_methods;
"#,
    )
    .expect("write absolute-alias impl shadow target");
    fs::write(
        root.join("crates/fixture/src/shadowed_methods.rs"),
        r#"use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
pub(crate) struct Local;
impl ::fixture_alias::shadowed_methods::Local {
    pub(crate) fn forge() {
        oracle_expect_err!(detector(), "shadowed");
    }
}
"#,
    )
    .expect("write absolute-alias impl method");
    track_workspace_paths(&root, &["crates/fixture/src/shadowed_methods.rs"]);

    let error = verify_module_graph(&source, &root)
        .expect_err("absolute self-crate aliases must preserve impl method shadow identities");
    assert!(error.contains("shadows a trusted oracle macro"), "{error}");
    assert!(error.contains("shadowed_methods::Local::forge"), "{error}");
}

pub(super) fn inherited_oracle_macro_shadowing_tracks_associated_function_identity() {
    verify_shadowed_impl_method(
        "crate::shadowed_methods::Helper::associated();",
        "associated",
    );
}

pub(super) fn inherited_oracle_macro_shadowing_tracks_receiver_method_identity() {
    verify_shadowed_impl_method(
        "let helper = crate::shadowed_methods::Helper; helper.receiver();",
        "receiver",
    );
}

pub(super) fn oracle_macro_shadowing_respects_lexical_fixture_scope() {
    let source = detector_fixture(
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { oracle_expect_err!(detector(), "reject"); }
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
mod other;
mod unrelated {
    macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
    fn unreachable_helper() { oracle_expect_err!((), "unreachable"); }
}
#[cfg(test)]
mod tests;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
"#,
    )
    .expect("write lexically unrelated macro declarations");

    verify_module_graph(&source, &root)
        .expect("unrelated and later macro declarations cannot shadow the fixture invocation");
}

pub(super) fn cached_target_analysis_revalidates_oracle_shadows_for_each_fixture_in_any_order() {
    let clean_source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
#[::rafter_invariant_test::detector_test]
fn clean_fixture() { oracle_expect_err!(detector(), "reject"); }
"#;
    let shadowed_source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
#[::rafter_invariant_test::detector_test]
fn shadowed_fixture() { oracle_expect_err!(detector(), "reject"); }
"#;
    let root = synthetic_workspace(clean_source, DETECTOR_SOURCE);
    let clean_path = root.join("crates/fixture/src/clean_tests.rs");
    let shadowed_path = root.join("crates/fixture/src/shadowed_tests.rs");
    fs::write(&clean_path, clean_source).expect("write clean fixture module");
    fs::write(&shadowed_path, shadowed_source).expect("write shadowed fixture module");
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
#[cfg(test)]
mod clean_tests;
macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; }
#[cfg(test)]
mod shadowed_tests;
"#,
    )
    .expect("write ordered fixture modules");
    track_workspace_paths(
        &root,
        &[
            "crates/fixture/src/lib.rs",
            "crates/fixture/src/clean_tests.rs",
            "crates/fixture/src/shadowed_tests.rs",
        ],
    );

    let clean_identity = crate::TestIdentity {
        test_name: "clean_tests::clean_fixture".to_owned(),
        ..synthetic_identity()
    };
    let shadowed_identity = crate::TestIdentity {
        test_name: "shadowed_tests::shadowed_fixture".to_owned(),
        ..synthetic_identity()
    };
    let cases = [
        (
            clean_source,
            clean_path.as_path(),
            &clean_identity,
            "clean_fixture",
            false,
        ),
        (
            shadowed_source,
            shadowed_path.as_path(),
            &shadowed_identity,
            "shadowed_fixture",
            true,
        ),
    ];

    for order in [[0, 1], [1, 0]] {
        let mut batch = DetectorFixtureAnalysis::default();
        for index in order {
            let (source, fixture_path, identity, fixture, should_reject) = cases[index];
            let result = batch.validate(&crate::DetectorFixtureSourceBinding {
                fixture_source: source,
                detector_source: DETECTOR_SOURCE,
                source_root: &root,
                fixture_path,
                detector_path: &root.join(DETECTOR_PATH),
                test_identity: identity,
                fixture,
                detector: "detector",
            });
            if should_reject {
                let error = result.expect_err("oracle-shadowed fixture must be rejected");
                assert!(error.contains("shadows a trusted oracle macro"), "{error}");
            } else {
                result.expect("clean fixture must remain accepted");
            }
        }
        assert_eq!(
            batch.target_analysis_count(),
            1,
            "both fixtures must reuse one Cargo-target analysis"
        );
    }
}
