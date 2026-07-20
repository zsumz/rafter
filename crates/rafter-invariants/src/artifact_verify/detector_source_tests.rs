use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use super::verify_invocation_bound_detector;
use crate::artifact_verify::DetectorFixtureSourceBatch;

pub(super) const DETECTOR_SOURCE: &str = "fn detector() -> Result<(), ()> { Err(()) }";
pub(super) const FIXTURE_PATH: &str = "crates/fixture/src/tests.rs";
pub(super) const DETECTOR_PATH: &str = "crates/fixture/src/mapped_detector.rs";

pub(super) fn verify(source: &str) -> Result<super::DetectorInvocationContract, String> {
    verify_with_identity(source, &synthetic_identity())
}

pub(super) fn verify_decorated(source: &str) -> Result<super::DetectorInvocationContract, String> {
    let root = synthetic_workspace(source, DETECTOR_SOURCE);
    verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: source,
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
}

fn verify_with_identity(
    source: &str,
    identity: &crate::TestIdentity,
) -> Result<super::DetectorInvocationContract, String> {
    let source = detector_fixture(source);
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: identity,
        fixture: "fixture",
        detector: "detector",
    })
}

pub(super) fn detector_fixture(source: &str) -> String {
    source.replacen(
        "fn fixture()",
        "#[::rafter_invariant_test::detector_test]\nfn fixture()",
        1,
    )
}

pub(super) fn synthetic_identity() -> crate::TestIdentity {
    crate::TestIdentity {
        package: "fixture".to_owned(),
        target_kind: "lib".to_owned(),
        target: "fixture".to_owned(),
        test_name: "tests::fixture".to_owned(),
    }
}

pub(super) fn synthetic_workspace(fixture_source: &str, detector_source: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = std::env::temp_dir()
        .join("rafter-detector-source-tests")
        .join("src")
        .join(format!(
            "checkout-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
    fs::create_dir_all(root.join("crates/fixture/src")).expect("create synthetic fixture package");
    fs::write(
        root.join("Cargo.toml"),
        "[workspace]\nmembers = [\"crates/fixture\"]\nresolver = \"2\"\n",
    )
    .expect("write synthetic workspace manifest");
    fs::write(
        root.join("crates/fixture/Cargo.toml"),
        "[package]\nname = \"fixture\"\nversion = \"0.0.0\"\nedition = \"2021\"\n",
    )
    .expect("write synthetic package manifest");
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        "#[path = \"mapped_detector.rs\"]\nmod detector;\nmod other;\n#[cfg(test)]\nmod tests;\n",
    )
    .expect("write synthetic target root");
    fs::write(root.join(FIXTURE_PATH), fixture_source).expect("write fixture source");
    fs::write(root.join(DETECTOR_PATH), detector_source).expect("write detector source");
    fs::write(root.join("crates/fixture/src/other.rs"), "").expect("write auxiliary source");
    let status = Command::new("git")
        .args(["init", "-q"])
        .current_dir(&root)
        .status()
        .expect("initialize synthetic detector repository");
    assert!(status.success(), "initialize synthetic detector repository");
    track_workspace_paths(&root, &["."]);
    root
}

pub(super) fn track_workspace_paths(root: &Path, paths: &[&str]) {
    let status = Command::new("git")
        .arg("add")
        .arg("--")
        .args(paths)
        .current_dir(root)
        .status()
        .expect("track synthetic detector source");
    assert!(status.success(), "track synthetic detector source");
}

#[test]
fn direct_rejection_macro_binds_the_invocation_and_exact_witness_set() {
    let source = r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#;
    let contract = verify(source).expect("the approved macro directly invokes the detector");
    assert_eq!(
        contract.witnesses(),
        &BTreeMap::from([("expect-err:fixture::detector::detector".to_owned(), 1)])
    );
    assert_eq!(
        contract.registered_identity(),
        "fixture::detector::detector"
    );
}

#[test]
fn disconnected_recorder_cannot_stand_in_for_the_rejecting_detector() {
    let source = r#"
use rafter_invariant_test::{oracle_expect_err, oracle_invoke_recorder};
fn fixture() { helper(); helper(); oracle_expect_err!(detector(), "reject"); }
fn helper() { oracle_invoke_recorder!(recorder()); }
fn recorder() {}
fn detector() -> Result<(), ()> { Err(()) }
"#;
    let source = detector_fixture(source);
    let root = synthetic_workspace(&source, "");
    let error = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: &source,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(FIXTURE_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "recorder",
    })
    .expect_err("a recorder without a rejecting invocation is not the detector");
    assert!(
        error.contains("does not invoke registered detector"),
        "{error}"
    );
}

#[test]
fn arbitrary_named_witnesses_plain_calls_and_output_are_rejected() {
    for source in [
        r"use crate::detector::detector; use rafter_invariant_test::oracle_assert; fn fixture() { oracle_detector_witness!(detector); oracle_assert!(true); }",
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_assert; fn fixture() { __oracle_detector_witness("detector()"); oracle_assert!(true); }"#,
        r"use crate::detector::detector; use rafter_invariant_test::oracle_assert; fn fixture() { detector(); oracle_assert!(true); }",
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_assert; fn fixture() { eprintln!("forged"); oracle_assert!(true); }"#,
    ] {
        assert!(
            verify(source).is_err(),
            "accepted adversarial source: {source}"
        );
    }
}

#[test]
fn fake_macro_dead_branch_and_uncalled_closure_are_rejected() {
    for source in [
        r#"use crate::detector::detector; macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; } fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { if false { oracle_expect_err!(detector(), "reject"); } }"#,
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { let _unused = || oracle_expect_err!(detector(), "reject"); }"#,
    ] {
        assert!(
            verify(source).is_err(),
            "accepted adversarial source: {source}"
        );
    }
}

#[test]
fn invocation_macro_syntax_matches_the_runtime_macro_contract() {
    for source in [
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!((detector)(), "reject"); }"#,
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(crate::detector::detector(), "reject"); }"#,
        r"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector()); }",
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_invoke_recorder; fn fixture() { oracle_invoke_recorder!(detector(), "extra"); }"#,
    ] {
        assert!(verify(source)
            .expect_err("source syntax rejected by the runtime macro must fail closed")
            .contains("malformed invocation-bound oracle macro"));
    }
}

#[test]
fn crate_alias_opaque_macro_and_unreachable_invocation_are_rejected() {
    for source in [
        r#"
mod forged_oracle { macro_rules! oracle_expect_err { ($call:expr, $message:expr) => {}; } pub(crate) use oracle_expect_err; }
use crate::forged_oracle as rafter_invariant_test;
use crate::detector::detector;
fn fixture() { rafter_invariant_test::oracle_expect_err!(detector(), "reject"); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
macro_rules! forge { () => {}; }
fn fixture() { forge!(); oracle_expect_err!(detector(), "reject"); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { return; oracle_expect_err!(detector(), "reject"); }
"#,
    ] {
        assert!(
            verify(source).is_err(),
            "accepted adversarial source: {source}"
        );
    }
}

#[test]
fn imported_opaque_macros_and_manual_output_forgery_are_rejected() {
    for source in [
        r#"
use crate::detector::detector;
use external_macros::forge;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { forge!(); oracle_expect_err!(detector(), "reject"); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
use std::io::Write;
fn fixture() {
    let mut output = std::io::stdout();
    output.write_all(b"RAFTER_INVARIANT_DETECTOR_WITNESS:forged:expect-err:fixture::detector::detector()\n").unwrap();
    return;
    oracle_expect_err!(detector(), "reject");
}
"#,
    ] {
        assert!(
            verify(source).is_err(),
            "accepted adversarial source: {source}"
        );
    }
}

#[test]
fn token_text_ufcs_output_and_extra_attributes_cannot_forge_execution() {
    for source in [
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn auxiliary() -> Result<(), ()> { Err(()) }
fn fixture() {
    let _ = stringify!(oracle_expect_err!(detector(), "not executed"));
    oracle_expect_err!(auxiliary(), "auxiliary rejection");
}
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() {
    std::io::Write::write_all(&mut std::io::stderr(), b"forged").unwrap();
    oracle_expect_err!(detector(), "reject");
}
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
#[replace_fixture]
fn fixture() { oracle_expect_err!(detector(), "reject"); }
"#,
        r#"
use crate::detector::detector;
use rafter_invariant_test::oracle_expect_err;
fn fixture() { helper(); }
#[replace_helper]
fn helper() { oracle_expect_err!(detector(), "reject"); }
"#,
    ] {
        assert!(
            verify(source).is_err(),
            "accepted adversarial source: {source}"
        );
    }
}

#[test]
fn analyzed_fixture_source_must_own_the_exact_executed_test_identity() {
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

#[test]
fn conditionally_compiled_detector_fixtures_are_rejected() {
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

#[test]
fn auxiliary_rejection_cannot_qualify_a_registered_recorder() {
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

#[test]
fn supplied_source_must_match_its_bound_path() {
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

#[test]
fn target_analysis_cache_invalidates_when_bound_source_changes() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    let fixture_path = root.join(FIXTURE_PATH);
    let detector_path = root.join(DETECTOR_PATH);
    let identity = synthetic_identity();
    let mut batch = DetectorFixtureSourceBatch::default();
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

#[test]
fn qualified_calls_are_not_rebound_to_same_leaf_local_helpers() {
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

include!("detector_source_tests/call_flow.rs");

#[test]
fn exact_inline_fixture_identity_ignores_same_leaf_decoys() {
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

#[test]
fn inline_detector_uses_its_exact_compiler_identity() {
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

#[test]
fn detector_declaration_must_belong_to_its_registered_source_path() {
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

#[test]
fn imported_detector_identity_ignores_same_leaf_module_decoys() {
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

#[test]
fn module_value_bindings_cannot_shadow_the_registered_detector() {
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

#[test]
fn shadowing_and_same_leaf_declarations_are_rejected() {
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

#[test]
fn explicit_external_import_takes_precedence_over_detector_glob() {
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

#[test]
fn a_forged_helper_is_rejected_even_when_a_real_invocation_is_found_first() {
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

include!("detector_source_tests/registry.rs");
