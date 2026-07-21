//! Compatibility facade for the version-1 hard-state storage format.
//!
//! The RFHS byte grammar lives in `format::v1::hard_state`; this module keeps
//! the established crate module path and flat public API stable.

pub use crate::format::v1::hard_state::{
    decode_raft_hard_state, encode_raft_hard_state, DecodeRaftHardStateError, RaftHardState,
    RAFT_HARD_STATE_MAGIC, RAFT_HARD_STATE_VERSION,
};
