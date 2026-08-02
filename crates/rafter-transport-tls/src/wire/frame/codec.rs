//! Pure bounded peer-frame encoder and decoder.

use std::marker::PhantomData;

use rafter::NodeId;
use rafter_codec::decode_message;
use rafter_service::transport::message_sender;

use crate::{ConnectionSequence, GroupIdCodec, WireLimits};

use super::{
    DecodePeerFrameError, PeerFrame, PeerFrameCodecConfigError, PeerFrameRoute, PeerFrameScratch,
    PEER_FRAME_KIND_MESSAGE, PEER_FRAME_LENGTH_PREFIX_BYTES,
};
use crate::wire::read::{Reader, UnexpectedEnd};

/// Pure version-1 peer-frame encoder and decoder.
#[derive(Clone, Debug)]
pub struct PeerFrameCodec<G, C> {
    pub(super) group_codec: C,
    pub(super) limits: WireLimits,
    pub(super) group_id_bound: usize,
    marker: PhantomData<fn() -> G>,
}

impl<G, C> PeerFrameCodec<G, C>
where
    C: GroupIdCodec<G>,
{
    /// Validates and creates a peer-frame codec.
    ///
    /// # Errors
    ///
    /// Returns [`PeerFrameCodecConfigError`] when the group codec's declared
    /// bound is zero or exceeds `limits`.
    pub fn new(group_codec: C, limits: WireLimits) -> Result<Self, PeerFrameCodecConfigError> {
        let group_id_bound = group_codec.max_encoded_len();
        if group_id_bound == 0 {
            return Err(PeerFrameCodecConfigError::ZeroGroupIdBound);
        }
        if group_id_bound > limits.max_group_id_bytes() {
            return Err(PeerFrameCodecConfigError::GroupIdBoundTooLarge {
                codec_maximum: group_id_bound,
                wire_maximum: limits.max_group_id_bytes(),
            });
        }
        Ok(Self {
            group_codec,
            limits,
            group_id_bound,
            marker: PhantomData,
        })
    }

    /// Returns the configured wire limits.
    #[must_use]
    pub const fn limits(&self) -> WireLimits {
        self.limits
    }

    /// Returns the caller-supplied group codec.
    #[must_use]
    pub const fn group_codec(&self) -> &C {
        &self.group_codec
    }

    /// Decodes exactly one complete length-prefixed peer frame.
    ///
    /// The declared body limit is checked before any caller-owned group decoder
    /// runs. The decoded group is re-encoded into `scratch` and must reproduce
    /// the exact input bytes, enforcing caller-defined canonical routing.
    ///
    /// # Errors
    ///
    /// Returns [`DecodePeerFrameError`] for malformed framing, an unknown kind,
    /// invalid sequence, noncanonical group routing, malformed inner messages,
    /// sender disagreement, or trailing bytes.
    pub fn decode(
        &self,
        input: &[u8],
        scratch: &mut PeerFrameScratch,
    ) -> Result<PeerFrame<G>, DecodePeerFrameError<C::Error>> {
        let outer = self.decode_outer(input, scratch)?;
        let message = decode_message(outer.message).map_err(DecodePeerFrameError::MessageDecode)?;
        let embedded = message_sender(&message);
        if embedded != outer.from {
            return Err(DecodePeerFrameError::SenderMismatch {
                envelope_from: outer.from,
                message_from: embedded,
            });
        }

        PeerFrame::new(
            outer.sequence,
            outer.group_id,
            outer.from,
            outer.to,
            message,
        )
        .map_err(|_| DecodePeerFrameError::SenderMismatch {
            envelope_from: outer.from,
            message_from: embedded,
        })
    }

    /// Decodes only bounded outer routing fields and canonical group identity.
    pub(crate) fn decode_route(
        &self,
        input: &[u8],
        scratch: &mut PeerFrameScratch,
    ) -> Result<PeerFrameRoute<G>, DecodePeerFrameError<C::Error>> {
        let outer = self.decode_outer(input, scratch)?;
        Ok(PeerFrameRoute {
            sequence: outer.sequence,
            group_id: outer.group_id,
            from: outer.from,
            to: outer.to,
        })
    }

    fn decode_outer<'a>(
        &self,
        input: &'a [u8],
        scratch: &mut PeerFrameScratch,
    ) -> Result<DecodedOuter<'a, G>, DecodePeerFrameError<C::Error>> {
        let body = self.read_bounded_body(input)?;
        let mut reader = Reader::new(body);
        let kind = read_u8(&mut reader)?;
        if kind != PEER_FRAME_KIND_MESSAGE {
            return Err(DecodePeerFrameError::UnknownFrameKind(kind));
        }
        let sequence = ConnectionSequence::new(read_u64(&mut reader)?)
            .map_err(|_| DecodePeerFrameError::ZeroSequence)?;
        let group_len = usize::from(read_u16(&mut reader)?);
        validate_decoded_group_bound(
            group_len,
            self.group_id_bound,
            self.limits.max_group_id_bytes(),
        )?;
        let group_bytes = reader.bytes(group_len).map_err(map_body_end)?;
        let from = NodeId(read_u64(&mut reader)?);
        let to = NodeId(read_u64(&mut reader)?);
        let message_len_wire = read_u32(&mut reader)?;
        let message_len = usize::try_from(message_len_wire)
            .map_err(|_| DecodePeerFrameError::MessageLengthUnsupported(message_len_wire))?;
        if message_len != reader.remaining() {
            return Err(DecodePeerFrameError::MessageLengthMismatch {
                declared: message_len,
                remaining: reader.remaining(),
            });
        }
        let message = reader.bytes(message_len).map_err(map_body_end)?;

        let group_id = self.decode_canonical_group(group_bytes, scratch)?;
        Ok(DecodedOuter {
            sequence,
            group_id,
            from,
            to,
            message,
        })
    }

    fn read_bounded_body<'a>(
        &self,
        input: &'a [u8],
    ) -> Result<&'a [u8], DecodePeerFrameError<C::Error>> {
        if input.len() < PEER_FRAME_LENGTH_PREFIX_BYTES {
            return Err(DecodePeerFrameError::TruncatedLengthPrefix);
        }

        let body_len_wire = u32::from_be_bytes([input[0], input[1], input[2], input[3]]);
        let body_len = usize::try_from(body_len_wire)
            .map_err(|_| DecodePeerFrameError::FrameLengthUnsupported(body_len_wire))?;
        if body_len > self.limits.max_frame_body_bytes() {
            return Err(DecodePeerFrameError::FrameTooLarge {
                declared: body_len,
                maximum: self.limits.max_frame_body_bytes(),
            });
        }

        let expected = PEER_FRAME_LENGTH_PREFIX_BYTES
            .checked_add(body_len)
            .ok_or(DecodePeerFrameError::FrameLengthUnsupported(body_len_wire))?;
        if input.len() < expected {
            return Err(DecodePeerFrameError::TruncatedFrame {
                declared: expected,
                actual: input.len(),
            });
        }
        if input.len() > expected {
            return Err(DecodePeerFrameError::TrailingBytes {
                remaining: input.len() - expected,
            });
        }
        Ok(&input[PEER_FRAME_LENGTH_PREFIX_BYTES..])
    }

    fn decode_canonical_group(
        &self,
        group_bytes: &[u8],
        scratch: &mut PeerFrameScratch,
    ) -> Result<G, DecodePeerFrameError<C::Error>> {
        let group_id = self
            .group_codec
            .decode(group_bytes)
            .map_err(DecodePeerFrameError::GroupDecode)?;
        scratch.group_id.clear();
        self.group_codec
            .encode(&group_id, &mut scratch.group_id)
            .map_err(DecodePeerFrameError::GroupReencode)?;
        validate_decoded_group_bound(
            scratch.group_id.len(),
            self.group_id_bound,
            self.limits.max_group_id_bytes(),
        )?;
        if scratch.group_id.as_slice() != group_bytes {
            return Err(DecodePeerFrameError::NonCanonicalGroupId);
        }
        Ok(group_id)
    }
}

struct DecodedOuter<'a, G> {
    sequence: ConnectionSequence,
    group_id: G,
    from: NodeId,
    to: NodeId,
    message: &'a [u8],
}

fn validate_decoded_group_bound<E>(
    actual: usize,
    codec_maximum: usize,
    wire_maximum: usize,
) -> Result<(), DecodePeerFrameError<E>> {
    if actual == 0 {
        return Err(DecodePeerFrameError::EmptyGroupId);
    }
    let maximum = codec_maximum.min(wire_maximum);
    if actual > maximum {
        return Err(DecodePeerFrameError::GroupIdTooLarge { actual, maximum });
    }
    Ok(())
}

fn read_u8<E>(reader: &mut Reader<'_>) -> Result<u8, DecodePeerFrameError<E>> {
    reader.u8().map_err(map_body_end)
}

fn read_u16<E>(reader: &mut Reader<'_>) -> Result<u16, DecodePeerFrameError<E>> {
    reader.u16().map_err(map_body_end)
}

fn read_u32<E>(reader: &mut Reader<'_>) -> Result<u32, DecodePeerFrameError<E>> {
    reader.u32().map_err(map_body_end)
}

fn read_u64<E>(reader: &mut Reader<'_>) -> Result<u64, DecodePeerFrameError<E>> {
    reader.u64().map_err(map_body_end)
}

const fn map_body_end<E>(_: UnexpectedEnd) -> DecodePeerFrameError<E> {
    DecodePeerFrameError::TruncatedBody
}
