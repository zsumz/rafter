//! Immutable shared application payload bytes.
//!
//! Payload views preserve content semantics while allowing codecs and
//! replication batches to share one backing allocation.

use std::{
    cmp::Ordering,
    fmt,
    hash::{Hash, Hasher},
    ops::{Deref, Range},
    sync::Arc,
};

/// Immutable application payload bytes with shared ownership: cloning shares
/// the allocation instead of copying it, so batching a log suffix and
/// fanning it out to every follower never duplicates payload content.
///
/// Equality compares content. [`SharedPayload::shares_allocation`] observes
/// sharing itself, which is how the zero-copy guarantee is asserted rather
/// than assumed.
#[derive(Clone, Eq)]
pub struct SharedPayload {
    bytes: Arc<[u8]>,
    range: Range<usize>,
}

impl SharedPayload {
    /// Builds a payload view over a range inside an immutable shared byte
    /// buffer.
    ///
    /// This is useful for codecs that decode several payloads from one owned
    /// frame. Equality, ordering, hashing, and [`SharedPayload::as_slice`]
    /// remain based only on this payload's range, while
    /// [`SharedPayload::shares_allocation`] can observe the common backing
    /// frame.
    ///
    /// Range-backed payloads may retain bytes outside the visible payload
    /// range until every payload view sharing the same backing allocation is
    /// dropped.
    #[must_use]
    pub fn from_shared_range(bytes: Arc<[u8]>, range: Range<usize>) -> Option<Self> {
        if range.start <= range.end && range.end <= bytes.len() {
            Some(Self { bytes, range })
        } else {
            None
        }
    }

    /// Returns the shared payload bytes.
    #[must_use]
    pub fn as_slice(&self) -> &[u8] {
        &self.bytes[self.range.clone()]
    }

    /// Whether `self` and `other` are the same allocation, not merely equal
    /// content.
    #[must_use]
    pub fn shares_allocation(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.bytes, &other.bytes)
    }
}

impl Deref for SharedPayload {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl AsRef<[u8]> for SharedPayload {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl From<Vec<u8>> for SharedPayload {
    fn from(payload: Vec<u8>) -> Self {
        let len = payload.len();
        Self {
            bytes: payload.into(),
            range: 0..len,
        }
    }
}

impl From<&[u8]> for SharedPayload {
    fn from(payload: &[u8]) -> Self {
        let bytes: Arc<[u8]> = payload.into();
        let len = bytes.len();
        Self {
            bytes,
            range: 0..len,
        }
    }
}

impl Hash for SharedPayload {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_slice().hash(state);
    }
}

impl PartialEq for SharedPayload {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}

impl Ord for SharedPayload {
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

impl PartialOrd for SharedPayload {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
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
        self.as_slice().fmt(formatter)
    }
}
