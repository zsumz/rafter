//! Adversarial source qualification scenarios for ordinary test oracles.

use std::{collections::BTreeSet, path::PathBuf};

use super::{qualify_source_text, verify_registered_oracle_sources, RegisteredTestBinding};

fn binding(name: &str) -> RegisteredTestBinding {
    RegisteredTestBinding {
        identity: crate::TestIdentity {
            package: "fixture".to_owned(),
            target_kind: "lib".to_owned(),
            target: "fixture".to_owned(),
            test_name: name.to_owned(),
        },
        path: "src/lib.rs".to_owned(),
        symbol: name.rsplit("::").next().unwrap().to_owned(),
    }
}

#[test]
fn exact_imported_oracle_macro_qualifies_the_registered_test() {
    let source = r"
        use rafter_invariant_test::oracle_assert;

        #[test]
        fn checks_state() {
            oracle_assert!(true);
        }
    ";

    qualify_source_text(source, &[], &binding("checks_state"))
        .expect("the exact public oracle macro is trusted");
}

#[test]
fn direct_hidden_helper_cannot_forge_a_qualified_observation() {
    let source = r"
        use rafter_invariant_test::oracle_assert;

        #[test]
        fn checks_state() {
            oracle_assert!(true);
            rafter_invariant_test::__oracle_observed();
        }
    ";

    let error = qualify_source_text(source, &[], &binding("checks_state"))
        .expect_err("direct access to the macro expansion ABI must fail closed");
    assert!(error.contains("reserved oracle channel"));
}

#[test]
fn manual_marker_output_cannot_forge_a_qualified_observation() {
    let source = r#"
        use rafter_invariant_test::oracle_assert;

        #[test]
        fn checks_state() {
            oracle_assert!(true);
            eprintln!("RAFTER_INVARIANT_ORACLE_OBSERVED:invented");
        }
    "#;

    let error = qualify_source_text(source, &[], &binding("checks_state"))
        .expect_err("manual marker text must fail closed");
    assert!(error.contains("reserved oracle marker"));
}

#[test]
fn reserved_channel_access_in_a_top_level_helper_cannot_bless_a_dead_oracle() {
    let source = r#"
        use rafter_invariant_test::oracle_assert;

        fn forge_observation() {
            let token = std::env::var("RAFTER_INVARIANT_ORACLE_TOKEN").unwrap();
            eprintln!("RAFTER_INVARIANT_ORACLE_OBSERVED:{token}");
        }

        #[test]
        fn checks_state() {
            if false {
                oracle_assert!(true);
            }
            forge_observation();
        }
    "#;

    let error = qualify_source_text(source, &[], &binding("checks_state"))
        .expect_err("a helper cannot access the reserved observation channel");
    assert!(error.contains("reserved oracle marker"));
}

#[test]
fn an_unrelated_modules_import_cannot_bless_the_registered_scope() {
    let source = r"
        mod unrelated {
            use rafter_invariant_test::oracle_assert;

            fn unrelated() {
                oracle_assert!(true);
            }
        }

        use attacker::*;

        #[test]
        fn checks_state() {
            oracle_assert!(true);
        }
    ";

    let error = qualify_source_text(source, &[], &binding("checks_state"))
        .expect_err("macro imports are resolved in the registered module only");
    assert!(error.contains("untrusted oracle macro"));
}

#[test]
fn local_oracle_macro_cannot_shadow_the_trusted_definition() {
    let source = r"
        macro_rules! oracle_assert { ($value:expr) => { let _ = $value; } }

        #[test]
        fn checks_state() {
            oracle_assert!(true);
        }
    ";

    let error = qualify_source_text(source, &[], &binding("checks_state"))
        .expect_err("a local namesake is not a trusted oracle");
    assert!(error.contains("local oracle macro"));
}

#[test]
fn unrelated_import_with_an_oracle_name_is_not_trusted() {
    let source = r"
        use other_crate::oracle_assert;

        #[test]
        fn checks_state() {
            oracle_assert!(true);
        }
    ";

    let error = qualify_source_text(source, &[], &binding("checks_state"))
        .expect_err("only the reviewed crate path is trusted");
    assert!(error.contains("untrusted oracle macro"));
}

#[test]
fn reviewed_proptest_body_is_qualified_without_marker_text_search() {
    let source = r"
        use proptest::prelude::*;
        use rafter_invariant_test::oracle_prop_assert;

        proptest! {
            #[test]
            fn generated(value in any::<bool>()) {
                oracle_prop_assert!(value || !value);
            }
        }
    ";

    qualify_source_text(source, &[], &binding("generated"))
        .expect("the exact generated test body uses a trusted oracle");
}

#[test]
fn every_reviewed_tests_layer_source_qualifies_under_the_runtime_policy() {
    let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    let (catalog, _) = crate::tests::loaded();
    let bindings = catalog
        .evidence
        .iter()
        .filter(|evidence| evidence.layer == "tests")
        .filter_map(|evidence| {
            evidence
                .test
                .as_ref()
                .map(|identity| RegisteredTestBinding {
                    identity: identity.clone(),
                    path: evidence.path.clone(),
                    symbol: evidence.symbol.clone(),
                })
        })
        .collect::<BTreeSet<_>>();

    verify_registered_oracle_sources(&workspace, &bindings)
        .expect("all reviewed tests-layer sources meet the production oracle policy");
}
