//! Compatibility facade for the version-1 RFPT manifest grammar.
//!
//! The byte grammar lives in `format::v1::pending_transfer`; this module keeps
//! the snapshot store's established internal module path stable.

pub(super) use crate::format::v1::pending_transfer::{
    decode_pending_snapshot_transfer_manifest, encode_pending_snapshot_transfer_manifest,
};
