use super::*;

#[test]
fn cargo_integration_test_identity_requires_the_target_root_path() {
    let identity = rafter_invariants::TestIdentity {
        package: "rafter-sim".to_owned(),
        target_kind: "test".to_owned(),
        target: "raft_invariants".to_owned(),
        test_name: "committed_prefix_is_stable_across_failover".to_owned(),
    };
    let expected = cargo_integration_test_root(&identity);

    assert_eq!(expected, "crates/rafter-sim/tests/raft_invariants.rs");
    assert_ne!(expected, "crates/rafter-sim/src/tests/raft_invariants.rs");
}

#[test]
fn negative_fixture_execution_identity_matches_the_analyzed_module() {
    let fixture_path = "crates/rafter-sim/src/model_check/invariants/tests/election.rs";
    let source = "#[test]\nfn detector_fixture() {}\n";
    let mut identity = rafter_invariants::TestIdentity {
        package: "rafter-sim".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_sim".to_owned(),
        test_name: "model_check::invariants::tests::election::detector_fixture".to_owned(),
    };
    assert!(test_identity_matches_source(
        &workspace_root(),
        fixture_path,
        source,
        "detector_fixture",
        &identity,
    ));
    identity.test_name = "model_check::invariants::tests::neighbor::detector_fixture".to_owned();
    assert!(!test_identity_matches_source(
        &workspace_root(),
        fixture_path,
        source,
        "detector_fixture",
        &identity,
    ));
}

#[test]
fn registered_library_identity_rejects_same_leaf_in_a_different_module() {
    let path = Path::new("crates/rafter-sim/src/model_check/invariants/tests/election.rs");
    let source = "#[test]\nfn detector_fixture() {}\n";
    let mut identity = rafter_invariants::TestIdentity {
        package: "rafter-sim".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter_sim".to_owned(),
        test_name: "model_check::invariants::tests::election::detector_fixture".to_owned(),
    };
    assert!(test_identity_matches_source(
        &workspace_root(),
        path.to_str().expect("UTF-8 test path"),
        source,
        "detector_fixture",
        &identity,
    ));

    identity.test_name = "model_check::invariants::tests::neighbor::detector_fixture".to_owned();
    assert!(!test_identity_matches_source(
        &workspace_root(),
        path.to_str().expect("UTF-8 test path"),
        source,
        "detector_fixture",
        &identity,
    ));
}

#[test]
fn registered_binary_identity_uses_the_binary_module_root() {
    let path = "crates/rafter-maelstrom/src/app/ps04_tests/snapshot.rs";
    let source = "#[test]\nfn restores_snapshot() {}\n";
    let mut identity = rafter_invariants::TestIdentity {
        package: "rafter-maelstrom".to_owned(),
        target_kind: "bin".to_owned(),
        target: "rafter-maelstrom".to_owned(),
        test_name: "app::ps04_tests::snapshot::restores_snapshot".to_owned(),
    };
    assert!(test_identity_matches_source(
        &workspace_root(),
        path,
        source,
        "restores_snapshot",
        &identity,
    ));

    identity.test_name = "snapshot::restores_snapshot".to_owned();
    assert!(!test_identity_matches_source(
        &workspace_root(),
        path,
        source,
        "restores_snapshot",
        &identity,
    ));
}

#[test]
fn registered_identity_includes_inline_module_nesting() {
    let path = "crates/rafter/src/lib.rs";
    let source = "mod nested { #[test] fn oracle() {} }";
    let mut identity = rafter_invariants::TestIdentity {
        package: "rafter".to_owned(),
        target_kind: "lib".to_owned(),
        target: "rafter".to_owned(),
        test_name: "nested::oracle".to_owned(),
    };
    assert!(test_identity_matches_source(
        &workspace_root(),
        path,
        source,
        "oracle",
        &identity,
    ));

    identity.test_name = "oracle".to_owned();
    assert!(!test_identity_matches_source(
        &workspace_root(),
        path,
        source,
        "oracle",
        &identity,
    ));
}

fn workspace_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..")
}

#[test]
fn rust_symbol_guard_requires_a_real_declaration() {
    let path = Path::new("fixture.rs");
    assert!(!source_declares_symbol(
        path,
        "// fn claimed_symbol() {}\nconst NOTE: &str = \"claimed_symbol\";",
        "claimed_symbol"
    ));
    assert!(source_declares_symbol(
        path,
        "fn claimed_symbol() {}",
        "claimed_symbol"
    ));
}

#[test]
fn rust_symbol_guard_rejects_imported_names_and_aliases() {
    let path = Path::new("fixture.rs");
    assert!(!source_declares_symbol(
        path,
        "use crate::claimed_symbol;",
        "claimed_symbol"
    ));
    assert!(!source_declares_symbol(
        path,
        "use crate::actual_symbol as claimed_symbol;",
        "claimed_symbol"
    ));
    assert!(!source_declares_symbol(
        path,
        "pub use crate::{actual_symbol as claimed_symbol, neighbor};",
        "claimed_symbol"
    ));
}

#[test]
fn registered_test_guard_rejects_non_tests_and_should_panic() {
    let declarations = |source: &str| {
        let file = syn::parse_file(source).expect("parse test fixture");
        let mut visitor = RegisteredTestVisitor {
            symbol: "oracle",
            module: Vec::new(),
            inline_modules: Vec::new(),
            declarations: Vec::new(),
        };
        visitor.visit_file(&file);
        visitor.declarations
    };
    assert_eq!(
        declarations("#[test]\nfn oracle() {}"),
        [("oracle".to_owned(), true, false)]
    );
    assert_eq!(
        declarations("fn oracle() {}"),
        [("oracle".to_owned(), false, false)]
    );
    assert_eq!(
        declarations("#[test]\n#[should_panic]\nfn oracle() {}"),
        [("oracle".to_owned(), true, true)]
    );
}

#[test]
fn typed_oracle_guard_requires_the_helper_crate_binding() {
    let trusted = syn::parse_file(
        "use rafter_invariant_test::oracle_assert; fn oracle() { oracle_assert!(true); }",
    )
    .expect("trusted source parses");
    let trusted_imports = trusted_oracle_imports(&trusted);
    let mut trusted_visitor = OracleMacroVisitor {
        trusted_macros: &trusted_imports,
        qualified_crate_trusted: true,
        found: false,
        untrusted: false,
    };
    trusted_visitor.visit_file(&trusted);
    assert!(trusted_visitor.found && !trusted_visitor.untrusted);

    let shadowed = syn::parse_file(
        "macro_rules! oracle_assert { ($value:expr) => {}; } fn oracle() { oracle_assert!(true); }",
    )
    .expect("shadowed source parses");
    assert!(declares_local_oracle_macro(&shadowed));
    let shadowed_imports = trusted_oracle_imports(&shadowed);
    let mut shadowed_visitor = OracleMacroVisitor {
        trusted_macros: &shadowed_imports,
        qualified_crate_trusted: true,
        found: false,
        untrusted: false,
    };
    shadowed_visitor.visit_file(&shadowed);
    assert!(!shadowed_visitor.found && shadowed_visitor.untrusted);

    let qualified = syn::parse_file("fn oracle() { rafter_invariant_test::oracle_assert!(true); }")
        .expect("qualified source parses");
    let qualified_imports = trusted_oracle_imports(&qualified);
    let mut qualified_visitor = OracleMacroVisitor {
        trusted_macros: &qualified_imports,
        qualified_crate_trusted: true,
        found: false,
        untrusted: false,
    };
    qualified_visitor.visit_file(&qualified);
    assert!(qualified_visitor.found && !qualified_visitor.untrusted);

    let aliased = syn::parse_file(
        "use crate::fake as rafter_invariant_test; use rafter_invariant_test::oracle_assert; fn oracle() { oracle_assert!(true); }",
    )
    .expect("aliased source parses");
    assert!(trusted_oracle_imports(&aliased).is_empty());
}
