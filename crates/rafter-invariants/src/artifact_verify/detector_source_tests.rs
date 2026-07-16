use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use super::verify_invocation_bound_detector;

const DETECTOR_SOURCE: &str = "fn detector() -> Result<(), ()> { Err(()) }";
const FIXTURE_PATH: &str = "crates/fixture/src/tests.rs";
const DETECTOR_PATH: &str = "crates/fixture/src/mapped_detector.rs";

fn verify(source: &str) -> Result<super::DetectorInvocationContract, String> {
    let source = detector_fixture(source);
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
}

fn detector_fixture(source: &str) -> String {
    source.replacen(
        "fn fixture()",
        "#[rafter_invariant_test::detector_test]\nfn fixture()",
        1,
    )
}

fn synthetic_identity() -> crate::TestIdentity {
    crate::TestIdentity {
        package: "fixture".to_owned(),
        target_kind: "lib".to_owned(),
        target: "fixture".to_owned(),
        test_name: "tests::fixture".to_owned(),
    }
}

fn synthetic_workspace(fixture_source: &str, detector_source: &str) -> PathBuf {
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
    root
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
fn recorder_helpers_are_counted_at_each_guaranteed_call_site() {
    let source = r#"
use rafter_invariant_test::{oracle_expect_err, oracle_invoke_recorder};
fn fixture() { helper(); helper(); oracle_expect_err!(detector(), "reject"); }
fn helper() { oracle_invoke_recorder!(recorder()); }
fn recorder() {}
fn detector() -> Result<(), ()> { Err(()) }
"#;
    let source = detector_fixture(source);
    let root = synthetic_workspace(&source, "");
    let contract = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: "",
        source_root: &root,
        fixture_path: &root.join(FIXTURE_PATH),
        detector_path: &root.join("crates/fixture/src/other.rs"),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "recorder",
    })
    .expect("a guaranteed helper call preserves invocation cardinality");
    assert_eq!(
        contract.witnesses(),
        &BTreeMap::from([
            ("expect-err:fixture::tests::detector".to_owned(), 1),
            ("recorder:fixture::tests::recorder".to_owned(), 2),
        ])
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
    assert!(error.contains("not its bound fixture source"), "{error}");
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
fn same_leaf_auxiliary_oracles_are_ambiguous_across_the_target_graph() {
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
    .expect_err("same-leaf auxiliary declarations must fail closed");
    assert!(error.contains("auxiliary` resolves to 2"), "{error}");
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
fn source_root_stripping_ignores_ancestor_src_components() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    let fixture_path = root.join(FIXTURE_PATH);
    let detector_path = root.join(DETECTOR_PATH);
    let contract = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &fixture_path,
        detector_path: &detector_path,
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect("workspace-relative module inference ignores host path components");
    assert_eq!(
        contract.registered_identity(),
        "fixture::detector::detector"
    );
}

#[test]
fn cargo_test_cfg_and_cfg_attr_path_select_the_actual_fixture_module() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    let alternate_fixture = root.join("crates/fixture/src/alternate_tests.rs");
    fs::write(&alternate_fixture, &source).expect("write cfg_attr-selected fixture source");
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        r#"#[path = "mapped_detector.rs"]
mod detector;
mod other;
#[cfg(any(unix, windows))]
#[cfg(not(feature = "disabled"))]
#[cfg_attr(test, path = "alternate_tests.rs")]
mod tests;
"#,
    )
    .expect("write cfg-selected target root");

    verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
        fixture_source: &source,
        detector_source: DETECTOR_SOURCE,
        source_root: &root,
        fixture_path: &alternate_fixture,
        detector_path: &root.join(DETECTOR_PATH),
        test_identity: &synthetic_identity(),
        fixture: "fixture",
        detector: "detector",
    })
    .expect("ordinary Cargo test cfg and active cfg_attr path must resolve exactly");
}

#[test]
fn unknown_custom_cfg_fails_closed_instead_of_omitting_the_module() {
    let source = detector_fixture(
        r#"use crate::detector::detector; use rafter_invariant_test::oracle_expect_err; fn fixture() { oracle_expect_err!(detector(), "reject"); }"#,
    );
    let root = synthetic_workspace(&source, DETECTOR_SOURCE);
    fs::write(
        root.join("crates/fixture/src/lib.rs"),
        "#[path = \"mapped_detector.rs\"]\nmod detector;\nmod other;\n#[cfg(custom_build_flag)]\nmod tests;\n",
    )
    .expect("write custom-cfg target root");
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
    .expect_err("unbound custom cfg must fail closed");
    assert!(
        error.contains("outside the reviewed test context"),
        "{error}"
    );
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
    assert!(verify(local_impostor)
        .expect_err("same-leaf local and bound detector declarations must fail")
        .contains("ambiguous declarations"));
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

#[test]
fn reviewed_registry_fixtures_have_source_bound_invocation_contracts() {
    let (catalog, _) = crate::tests::loaded();
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let root = fs::canonicalize(root).expect("canonical workspace root");
    let mut failures = Vec::new();

    for descriptor in catalog
        .evidence
        .iter()
        .filter(|evidence| evidence.layer == "simulator" && evidence.strength == "direct")
    {
        let Some(fixture) = descriptor.negative_fixture.as_deref() else {
            continue;
        };
        let Some(fixture_path) = descriptor.negative_fixture_path.as_deref() else {
            continue;
        };
        let Some(detector) = descriptor.negative_fixture_detector.as_deref() else {
            continue;
        };
        let fixture_path =
            fs::canonicalize(root.join(fixture_path)).expect("canonical registered fixture source");
        let detector_path = fs::canonicalize(root.join(&descriptor.path))
            .expect("canonical registered detector source");
        let fixture_source =
            fs::read_to_string(&fixture_path).expect("read registered fixture source");
        let detector_source =
            fs::read_to_string(&detector_path).expect("read registered detector source");
        let identity = descriptor
            .simulator
            .as_ref()
            .and_then(|identity| identity.negative_test.as_ref())
            .expect("registered direct simulator fixture identity");

        if let Err(error) = verify_invocation_bound_detector(&crate::DetectorFixtureSourceBinding {
            fixture_source: &fixture_source,
            detector_source: &detector_source,
            source_root: &root,
            fixture_path: &fixture_path,
            detector_path: &detector_path,
            test_identity: identity,
            fixture,
            detector,
        }) {
            failures.push(format!(
                "{} {fixture} -> {detector}: {error}",
                descriptor.invariant_id
            ));
        }
    }

    assert!(
        failures.is_empty(),
        "registered detector invocation contracts failed:\n{}",
        failures.join("\n")
    );
}
