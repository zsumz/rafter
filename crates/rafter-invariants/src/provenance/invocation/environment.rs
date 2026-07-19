//! Collision-free identity for OS-compatible environment maps.

use std::{collections::BTreeMap, error::Error, fmt};

use sha2::{Digest, Sha256};

pub(crate) fn digest_environment(
    environment: &BTreeMap<String, String>,
) -> Result<String, EnvironmentIdentityError> {
    for (name, value) in environment {
        if name.is_empty() || name.contains('=') || name.contains('\0') {
            return Err(EnvironmentIdentityError::InvalidName(name.clone()));
        }
        if value.contains('\0') {
            return Err(EnvironmentIdentityError::InvalidValue(name.clone()));
        }
    }
    let encoded = environment
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join("\0");
    Ok(format!("{:x}", Sha256::digest(encoded)))
}

pub(crate) fn environment_matches_digest(
    environment: &BTreeMap<String, String>,
    expected: &str,
) -> bool {
    digest_environment(environment).is_ok_and(|observed| observed == expected)
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) enum EnvironmentIdentityError {
    InvalidName(String),
    InvalidValue(String),
}

impl fmt::Display for EnvironmentIdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName(name) => {
                write!(
                    formatter,
                    "environment contains invalid variable name {name:?}"
                )
            }
            Self::InvalidValue(name) => {
                write!(formatter, "environment variable {name:?} contains NUL")
            }
        }
    }
}

impl Error for EnvironmentIdentityError {}

#[cfg(test)]
mod tests {
    use super::*;

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
}
