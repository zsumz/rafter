//! Length-delimited, group-aware peer-frame codec.

mod codec;
mod encode;
mod error;
mod prepared;
mod types;

pub use codec::PeerFrameCodec;
pub use error::{
    DecodePeerFrameError, EncodePeerFrameError, PeerFrameCodecConfigError, PeerFrameError,
};
pub(crate) use types::PeerFrameRoute;
pub use types::{
    PeerFrame, PeerFrameScratch, PEER_FRAME_FIXED_BODY_BYTES, PEER_FRAME_KIND_MESSAGE,
    PEER_FRAME_LENGTH_PREFIX_BYTES,
};

#[cfg(test)]
pub(crate) use encode::EncodedLengths;
pub(crate) use prepared::PreparedPeerFrame;
