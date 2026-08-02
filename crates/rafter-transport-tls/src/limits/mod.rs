//! Validated finite resource limits.

mod aggregate;
mod directory;
mod error;
mod runtime;
mod session;
mod wire;

use error::{require_at_most, require_nonzero};

pub use aggregate::TransportLimits;
pub use directory::{CertificateDirectoryLimits, DirectoryLimits, EndpointBookLimits};
pub use error::{LimitError, LimitKind};
pub use runtime::{
    InboundQueueLimits, OutboundQueueLimits, ReceiveMemoryLimits, RuntimeLimitError,
    RuntimeLimitKind, RuntimeLimits, MIN_SAFE_DECODE_AMPLIFICATION,
};
pub use session::{SessionStoreLimits, DEFAULT_MAX_SESSION_PEER_RECORDS, MAX_SESSION_PEER_RECORDS};
pub use wire::{
    WireLimits, DEFAULT_MAX_APPEND_ENTRIES_BYTES, DEFAULT_MAX_FRAME_BODY_BYTES,
    DEFAULT_MAX_FRAME_BYTES, DEFAULT_MAX_GROUP_ID_BYTES,
};
