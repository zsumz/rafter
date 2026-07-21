//! Tests for source-environment identity.

use std::collections::BTreeMap;

use super::{source_environment, source_environment_matches_digest};

#[test]
fn source_identity_excludes_cache_paths_already_bound_by_cargo_configuration() {
    let first = BTreeMap::from([
        ("CARGO_HOME".to_owned(), "/producer/cargo".to_owned()),
        ("HOME".to_owned(), "/producer".to_owned()),
        ("PATH".to_owned(), "/producer/bin".to_owned()),
        ("SDKROOT".to_owned(), "/sdk".to_owned()),
    ]);
    let second = BTreeMap::from([
        ("CARGO_HOME".to_owned(), "/verifier/cargo".to_owned()),
        ("HOME".to_owned(), "/verifier".to_owned()),
        ("PATH".to_owned(), "/verifier/bin".to_owned()),
        ("SDKROOT".to_owned(), "/sdk".to_owned()),
    ]);

    assert_eq!(source_environment(&first), source_environment(&second));
}

#[test]
fn source_identity_retains_platform_compiler_selection() {
    let first = BTreeMap::from([("DEVELOPER_DIR".to_owned(), "/xcode/a".to_owned())]);
    let second = BTreeMap::from([("DEVELOPER_DIR".to_owned(), "/xcode/b".to_owned())]);

    assert_ne!(source_environment(&first), source_environment(&second));
}

#[test]
fn source_digest_matches_only_the_reviewed_environment_subset() {
    let producer = BTreeMap::from([
        ("CARGO_HOME".to_owned(), "/producer/cargo".to_owned()),
        ("SDKROOT".to_owned(), "/sdk".to_owned()),
    ]);
    let verifier = BTreeMap::from([
        ("CARGO_HOME".to_owned(), "/verifier/cargo".to_owned()),
        ("SDKROOT".to_owned(), "/sdk".to_owned()),
    ]);
    let digest = super::source_environment_sha256_from(&producer).unwrap();

    assert!(source_environment_matches_digest(&verifier, &digest));
    assert!(!source_environment_matches_digest(
        &BTreeMap::from([("SDKROOT".to_owned(), "/other-sdk".to_owned())]),
        &digest
    ));
}
