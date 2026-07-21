//! Scenarios: verifier-owned source locations for trusted detector support macros.

use std::path::Path;

use syn::parse_quote;

use super::policy::OracleSourcePolicy;

#[test]
fn support_item_macros_are_accepted_only_at_their_reviewed_sources() {
    let policy = OracleSourcePolicy::new("rafter_invariant_test", Path::new("/workspace"), &[]);
    let oracle_adapter = parse_quote!(impl_oracle_call!(() => ()););
    let detector_state = parse_quote!(std::thread_local! { static STATE: usize = 0; });

    assert!(policy.reviewed_support_item_macro(
        &oracle_adapter,
        Path::new("/workspace/crates/rafter-invariant-test/src/oracle/call.rs")
    ));
    assert!(policy.reviewed_support_item_macro(
        &detector_state,
        Path::new("/workspace/crates/rafter-invariant-test/src/detector/session.rs")
    ));
    for wrong_source in [
        "/workspace/crates/rafter-invariant-test/src/lib.rs",
        "/workspace/crates/rafter-invariant-test/src/oracle/macros.rs",
        "/workspace/crates/rafter-invariant-test/src/detector/mod.rs",
    ] {
        assert!(!policy.reviewed_support_item_macro(&oracle_adapter, Path::new(wrong_source)));
        assert!(!policy.reviewed_support_item_macro(&detector_state, Path::new(wrong_source)));
    }
}

#[test]
fn exported_oracle_macros_are_canonical_only_in_the_wire_reviewed_module() {
    let policy = OracleSourcePolicy::new("rafter_invariant_test", Path::new("/workspace"), &[]);
    let canonical = Path::new("/workspace/crates/rafter-invariant-test/src/oracle/macros.rs");
    let module = vec!["oracle".to_owned(), "macros".to_owned()];

    assert!(policy.canonical_oracle_macro_definition(&module, canonical));
    assert!(!policy.canonical_oracle_macro_definition(&[], canonical));
    assert!(!policy.canonical_oracle_macro_definition(
        &module,
        Path::new("/workspace/crates/rafter-invariant-test/src/lib.rs")
    ));
}
