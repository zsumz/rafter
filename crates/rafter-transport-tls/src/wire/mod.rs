//! Versioned pure handshake and peer-frame codecs.

mod frame;
mod handshake;
mod read;

pub use frame::{
    DecodePeerFrameError, EncodePeerFrameError, PeerFrame, PeerFrameCodec,
    PeerFrameCodecConfigError, PeerFrameError, PeerFrameScratch, PEER_FRAME_FIXED_BODY_BYTES,
    PEER_FRAME_KIND_MESSAGE, PEER_FRAME_LENGTH_PREFIX_BYTES,
};
pub use handshake::{
    decode_client_hello, decode_server_hello, encode_client_hello_into, encode_server_hello_into,
    highest_common_version, ClientHello, DecodeHandshakeError, HandshakeField, ServerHello,
    ServerHelloStatus, ServerRefusal, VersionRange, VersionRangeError, CURRENT_TRANSPORT_VERSION,
    HANDSHAKE_MAGIC, MAX_CLIENT_HELLO_BYTES, MAX_SERVER_HELLO_BYTES,
};

#[cfg(test)]
pub(crate) use frame::EncodedLengths;
pub(crate) use frame::PreparedPeerFrame;
