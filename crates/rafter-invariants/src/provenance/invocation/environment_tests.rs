//! Tests for invocation environment identity.

use std::collections::BTreeMap;

use super::{digest_environment, environment_matches_digest, EnvironmentIdentityError};

#[test]
fn environment_identity_is_stable_for_sorted_os_compatible_entries() {
    let environment = BTreeMap::from([
        ("A".to_owned(), "B".to_owned()),
        ("C".to_owned(), "D".to_owned()),
    ]);
    assert_eq!(
        digest_environment(&environment).expect("valid environment"),
        "5be5cd3db08d216c2cb995a93758c5f9d7f263854aba4d65cf825b6e3407f1cc"
    );
}

#[test]
fn invalid_environment_domain_cannot_collide_with_a_valid_map() {
    let valid = BTreeMap::from([
        ("A".to_owned(), "B".to_owned()),
        ("C".to_owned(), "D".to_owned()),
    ]);
    let forged = BTreeMap::from([("A".to_owned(), "B\0C=D".to_owned())]);
    assert!(digest_environment(&valid).is_ok());
    assert_eq!(
        digest_environment(&forged),
        Err(EnvironmentIdentityError::InvalidValue("A".to_owned()))
    );
    assert!(!environment_matches_digest(
        &forged,
        &digest_environment(&valid).expect("valid environment")
    ));
}

#[test]
fn empty_equal_and_nul_names_are_rejected() {
    for name in ["", "A=B", "A\0B"] {
        let environment = BTreeMap::from([(name.to_owned(), "value".to_owned())]);
        assert!(matches!(
            digest_environment(&environment),
            Err(EnvironmentIdentityError::InvalidName(_))
        ));
    }
}
