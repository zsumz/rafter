//! Caller-owned canonical group routing.

use std::error::Error;

/// Canonical bounded encoding for caller-defined Raft group identities.
///
/// The transport does not impose Serde, JSON, or another application schema.
/// Implementations must emit one canonical byte representation for each
/// logical group. The inbound frame decoder decodes and re-encodes every group
/// ID and refuses alternate spellings.
pub trait GroupIdCodec<G>: Send + Sync + 'static {
    /// Typed encoding or decoding error.
    type Error: Error + Send + Sync + 'static;

    /// Maximum number of bytes this codec can emit for one group identity.
    fn max_encoded_len(&self) -> usize;

    /// Encodes `group_id` canonically into `output`.
    ///
    /// Implementations must clear `output` before writing and must not emit an
    /// empty representation.
    ///
    /// # Errors
    ///
    /// Returns the implementation's typed error when the group cannot be
    /// encoded.
    fn encode(&self, group_id: &G, output: &mut Vec<u8>) -> Result<(), Self::Error>;

    /// Decodes one complete canonical group identity candidate.
    ///
    /// The transport re-encodes the result and requires exact byte equality, so
    /// accepting a noncanonical candidate here does not make it routable.
    ///
    /// # Errors
    ///
    /// Returns the implementation's typed error when `input` is not a valid
    /// group identity.
    fn decode(&self, input: &[u8]) -> Result<G, Self::Error>;
}
