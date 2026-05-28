use std::{error::Error, fmt};

/// Stable identifier for a Raft snapshot group.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SnapshotGroupId(String);

impl SnapshotGroupId {
    /// Constructs an opaque Raft snapshot group identity.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotIdError`] when the identifier is empty or contains
    /// characters outside the stable snapshot-id alphabet.
    pub fn new(value: impl Into<String>) -> Result<Self, SnapshotIdError> {
        let value = value.into();
        validate_snapshot_id("snapshot group id", &value)?;
        Ok(Self(value))
    }

    /// Returns the identifier as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SnapshotGroupId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Application-defined snapshot kind used to choose a payload decoder.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ApplicationSnapshotKind(String);

impl ApplicationSnapshotKind {
    /// Constructs an opaque application snapshot kind.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotIdError`] when the kind is empty or contains
    /// characters outside the stable snapshot-id alphabet.
    pub fn new(value: impl Into<String>) -> Result<Self, SnapshotIdError> {
        let value = value.into();
        validate_snapshot_id("application snapshot kind", &value)?;
        Ok(Self(value))
    }

    /// Returns the kind as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ApplicationSnapshotKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Snapshot identifier validation error.
///
/// This enum is exhaustive because snapshot id validation is closed over
/// length, emptiness, and character-set checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SnapshotIdError {
    Empty {
        field: &'static str,
    },
    TooLong {
        field: &'static str,
        len: usize,
    },
    InvalidCharacter {
        field: &'static str,
        character: char,
    },
}

impl fmt::Display for SnapshotIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { field } => write!(formatter, "{field} cannot be empty"),
            Self::TooLong { field, len } => {
                write!(
                    formatter,
                    "{field} cannot be longer than 128 bytes; got {len}"
                )
            }
            Self::InvalidCharacter { field, character } => {
                write!(
                    formatter,
                    "{field} contains invalid character {character:?}"
                )
            }
        }
    }
}

impl Error for SnapshotIdError {}

fn validate_snapshot_id(field: &'static str, value: &str) -> Result<(), SnapshotIdError> {
    if value.is_empty() {
        return Err(SnapshotIdError::Empty { field });
    }
    if value.len() > 128 {
        return Err(SnapshotIdError::TooLong {
            field,
            len: value.len(),
        });
    }
    if let Some(character) = value.chars().find(
        |character| !matches!(character, 'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | ':'),
    ) {
        return Err(SnapshotIdError::InvalidCharacter { field, character });
    }
    Ok(())
}
