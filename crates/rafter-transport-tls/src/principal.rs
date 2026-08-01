//! Stable transport and deployment identities.

use std::{borrow::Borrow, error::Error, fmt, str::FromStr, sync::Arc};

/// Maximum UTF-8 byte length of a peer or cluster identity.
pub const MAX_ID_BYTES: usize = 128;

/// The identity whose validation failed.
///
/// This enum is exhaustive: transport principals are either peers or the
/// cluster boundary shared by those peers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityKind {
    /// A stable authenticated transport principal.
    Peer,
    /// A deployment boundary shared by one transport cluster.
    Cluster,
}

impl fmt::Display for IdentityKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Peer => formatter.write_str("peer identity"),
            Self::Cluster => formatter.write_str("cluster identity"),
        }
    }
}

/// Validation failure for [`PeerId`] or [`ClusterId`].
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum IdentityError {
    /// The identity was empty.
    Empty {
        /// Which identity was being validated.
        kind: IdentityKind,
    },
    /// The UTF-8 representation exceeded [`MAX_ID_BYTES`].
    TooLong {
        /// Which identity was being validated.
        kind: IdentityKind,
        /// Actual UTF-8 byte length.
        len: usize,
        /// Maximum accepted UTF-8 byte length.
        max: usize,
    },
    /// The identity contained a Unicode control character.
    ControlCharacter {
        /// Which identity was being validated.
        kind: IdentityKind,
        /// Byte offset of the refused character.
        byte_index: usize,
        /// Refused character.
        character: char,
    },
}

impl fmt::Display for IdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty { kind } => write!(formatter, "{kind} must not be empty"),
            Self::TooLong { kind, len, max } => write!(
                formatter,
                "{kind} is {len} UTF-8 bytes, exceeding the maximum {max}"
            ),
            Self::ControlCharacter {
                kind,
                byte_index,
                character,
            } => write!(
                formatter,
                "{kind} contains control character {character:?} at byte {byte_index}"
            ),
        }
    }
}

impl Error for IdentityError {}

/// Stable authenticated identity of one physical transport peer.
///
/// A `PeerId` is independent of [`rafter::NodeId`]. It remains stable across
/// certificate rotation and may map to different node IDs in different groups,
/// but to at most one live node ID in any one group.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PeerId {
    value: Arc<str>,
    wire_len: u8,
}

impl PeerId {
    /// Validates and creates a peer identity.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError`] when `value` is empty, longer than
    /// [`MAX_ID_BYTES`], or contains a control character.
    pub fn new(value: &str) -> Result<Self, IdentityError> {
        let (value, wire_len) = validate_identity(IdentityKind::Peer, value)?;
        Ok(Self { value, wire_len })
    }

    /// Returns the exact identity bytes as UTF-8 text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub(crate) const fn wire_len(&self) -> u8 {
        self.wire_len
    }
}

impl fmt::Display for PeerId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AsRef<str> for PeerId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for PeerId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for PeerId {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<&str> for PeerId {
    type Error = IdentityError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Exact deployment boundary carried by the transport handshake.
///
/// Matching credentials are insufficient to cross a `ClusterId` boundary. Both
/// peers must also present the same exact cluster identity before any peer frame
/// is accepted.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ClusterId {
    value: Arc<str>,
    wire_len: u8,
}

impl ClusterId {
    /// Validates and creates a cluster identity.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError`] when `value` is empty, longer than
    /// [`MAX_ID_BYTES`], or contains a control character.
    pub fn new(value: &str) -> Result<Self, IdentityError> {
        let (value, wire_len) = validate_identity(IdentityKind::Cluster, value)?;
        Ok(Self { value, wire_len })
    }

    /// Returns the exact identity bytes as UTF-8 text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.value
    }

    pub(crate) const fn wire_len(&self) -> u8 {
        self.wire_len
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl AsRef<str> for ClusterId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Borrow<str> for ClusterId {
    fn borrow(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for ClusterId {
    type Err = IdentityError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<&str> for ClusterId {
    type Error = IdentityError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

fn validate_identity(kind: IdentityKind, value: &str) -> Result<(Arc<str>, u8), IdentityError> {
    if value.is_empty() {
        return Err(IdentityError::Empty { kind });
    }
    if value.len() > MAX_ID_BYTES {
        return Err(IdentityError::TooLong {
            kind,
            len: value.len(),
            max: MAX_ID_BYTES,
        });
    }
    let wire_len = u8::try_from(value.len()).map_err(|_| IdentityError::TooLong {
        kind,
        len: value.len(),
        max: MAX_ID_BYTES,
    })?;
    if let Some((byte_index, character)) = value
        .char_indices()
        .find(|(_, character)| character.is_control())
    {
        return Err(IdentityError::ControlCharacter {
            kind,
            byte_index,
            character,
        });
    }
    Ok((Arc::from(value), wire_len))
}
