use std::{fmt, ops::Deref, sync::Arc};

/// Immutable application payload bytes with shared ownership: cloning shares
/// the allocation instead of copying it, so batching a log suffix and
/// fanning it out to every follower never duplicates payload content.
///
/// Equality compares content. [`SharedPayload::shares_allocation`] observes
/// sharing itself, which is how the zero-copy guarantee is asserted rather
/// than assumed.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SharedPayload(Arc<[u8]>);

impl SharedPayload {
    /// Returns the shared payload bytes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Whether `self` and `other` are the same allocation, not merely equal
    /// content.
    #[must_use]
    pub fn shares_allocation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Deref for SharedPayload {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for SharedPayload {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for SharedPayload {
    fn from(payload: Vec<u8>) -> Self {
        Self(payload.into())
    }
}

impl From<&[u8]> for SharedPayload {
    fn from(payload: &[u8]) -> Self {
        Self(payload.into())
    }
}

impl PartialEq<[u8]> for SharedPayload {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_slice() == other
    }
}

impl PartialEq<Vec<u8>> for SharedPayload {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl<const N: usize> PartialEq<[u8; N]> for SharedPayload {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.as_slice() == other
    }
}

impl<const N: usize> PartialEq<&[u8; N]> for SharedPayload {
    fn eq(&self, other: &&[u8; N]) -> bool {
        self.as_slice() == *other
    }
}

impl fmt::Debug for SharedPayload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(formatter)
    }
}
