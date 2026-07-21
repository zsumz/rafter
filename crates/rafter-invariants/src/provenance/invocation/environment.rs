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
#[path = "environment_tests.rs"]
mod tests;
