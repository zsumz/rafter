//! Connection-independent peer-frame encoding and queue preparation.

use rafter::{InstallSnapshotChunk, Message, NodeId, SnapshotChunkSend};
use rafter_codec::encode_message_into;

use crate::wire::read::{put_u16, put_u32, put_u64, put_u8};
use crate::{GroupIdCodec, PeerFrame, PeerFrameScratch};

use super::{
    EncodePeerFrameError, PeerFrameCodec, PreparedPeerFrame, PEER_FRAME_FIXED_BODY_BYTES,
    PEER_FRAME_KIND_MESSAGE, PEER_FRAME_LENGTH_PREFIX_BYTES,
};

impl<G, C> PeerFrameCodec<G, C>
where
    C: GroupIdCodec<G>,
{
    /// Encodes exactly one complete length-prefixed peer frame.
    ///
    /// `output` is cleared first. On any error it is left empty. The reusable
    /// scratch buffers retain capacity.
    ///
    /// # Errors
    ///
    /// Returns [`EncodePeerFrameError`] when group encoding, inner-message
    /// encoding, canonical bounds, or the total frame bound is violated.
    pub fn encode_into(
        &self,
        output: &mut Vec<u8>,
        scratch: &mut PeerFrameScratch,
        frame: &PeerFrame<G>,
    ) -> Result<(), EncodePeerFrameError<C::Error>> {
        output.clear();
        let lengths = self.encode_fields(frame.group_id(), frame.message(), scratch)?;
        output.reserve(lengths.complete);
        put_u32(output, lengths.body);
        put_u8(output, PEER_FRAME_KIND_MESSAGE);
        put_u64(output, frame.sequence().get());
        put_u16(output, lengths.group);
        output.extend_from_slice(&scratch.group_id);
        put_u64(output, frame.from().0);
        put_u64(output, frame.to().0);
        put_u32(output, lengths.message);
        output.extend_from_slice(&scratch.message);
        Ok(())
    }

    /// Encodes connection-independent fields for bounded queue admission.
    pub(crate) fn prepare(
        &self,
        frame: &PeerFrame<G>,
        scratch: &mut PeerFrameScratch,
    ) -> Result<PreparedPeerFrame, EncodePeerFrameError<C::Error>> {
        self.prepare_message(
            frame.group_id(),
            frame.from(),
            frame.to(),
            frame.message(),
            scratch,
        )
    }

    pub(crate) fn prepare_message(
        &self,
        group_id: &G,
        from: NodeId,
        to: NodeId,
        message: &Message,
        scratch: &mut PeerFrameScratch,
    ) -> Result<PreparedPeerFrame, EncodePeerFrameError<C::Error>> {
        let lengths = self.encode_fields(group_id, message, scratch)?;
        Ok(PreparedPeerFrame::new(
            std::mem::take(&mut scratch.group_id),
            std::mem::take(&mut scratch.message),
            from,
            to,
            lengths.body,
            lengths.complete,
            lengths.group,
            lengths.message,
        ))
    }

    /// Returns the exact complete-frame bytes a directive will occupy.
    ///
    /// The codec frame for an empty chunk already contains the chunk's `u32`
    /// length and trailing checksum. Materializing the directive changes only
    /// the number of opaque chunk bytes, so adding `chunk.len` is exact.
    pub(crate) fn snapshot_wire_len(
        &self,
        group_id: &G,
        chunk: &SnapshotChunkSend,
        scratch: &mut PeerFrameScratch,
    ) -> Result<usize, EncodePeerFrameError<C::Error>> {
        scratch.group_id.clear();
        scratch.message.clear();
        self.encode_group(group_id, scratch)?;

        let placeholder = Message::InstallSnapshotChunk(InstallSnapshotChunk {
            term: chunk.term,
            leader_id: chunk.leader_id,
            transfer_id: chunk.transfer_id,
            metadata: chunk.metadata.clone(),
            total_payload_len: chunk.total_payload_len,
            application_payload_crc32: chunk.application_payload_crc32,
            offset: chunk.offset,
            chunk: Vec::new(),
            done: chunk.done,
        });
        encode_message_into(&mut scratch.message, &placeholder)
            .map_err(EncodePeerFrameError::MessageEncode)?;
        let chunk_len =
            usize::try_from(chunk.len).map_err(|_| EncodePeerFrameError::MessageLengthOverflow)?;
        let message_len = scratch
            .message
            .len()
            .checked_add(chunk_len)
            .ok_or(EncodePeerFrameError::MessageLengthOverflow)?;
        self.validate_lengths(scratch.group_id.len(), message_len)
            .map(|lengths| lengths.complete)
    }

    fn encode_fields(
        &self,
        group_id: &G,
        message: &Message,
        scratch: &mut PeerFrameScratch,
    ) -> Result<EncodedLengths, EncodePeerFrameError<C::Error>> {
        scratch.group_id.clear();
        scratch.message.clear();
        self.encode_group(group_id, scratch)?;
        encode_message_into(&mut scratch.message, message)
            .map_err(EncodePeerFrameError::MessageEncode)?;
        self.validate_lengths(scratch.group_id.len(), scratch.message.len())
    }

    fn encode_group(
        &self,
        group_id: &G,
        scratch: &mut PeerFrameScratch,
    ) -> Result<(), EncodePeerFrameError<C::Error>> {
        self.group_codec
            .encode(group_id, &mut scratch.group_id)
            .map_err(EncodePeerFrameError::GroupEncode)?;
        validate_encoded_group(
            scratch.group_id.len(),
            self.group_id_bound,
            self.limits.max_group_id_bytes(),
        )
    }

    fn validate_lengths(
        &self,
        group_id_len: usize,
        message_len: usize,
    ) -> Result<EncodedLengths, EncodePeerFrameError<C::Error>> {
        let body_len = PEER_FRAME_FIXED_BODY_BYTES
            .checked_add(group_id_len)
            .and_then(|len| len.checked_add(message_len))
            .ok_or(EncodePeerFrameError::FrameLengthOverflow)?;
        if body_len > self.limits.max_frame_body_bytes() {
            return Err(EncodePeerFrameError::FrameTooLarge {
                actual: body_len,
                maximum: self.limits.max_frame_body_bytes(),
            });
        }

        let complete_len = PEER_FRAME_LENGTH_PREFIX_BYTES
            .checked_add(body_len)
            .ok_or(EncodePeerFrameError::FrameLengthOverflow)?;
        let body_len =
            u32::try_from(body_len).map_err(|_| EncodePeerFrameError::FrameLengthOverflow)?;
        let group_len =
            u16::try_from(group_id_len).map_err(|_| EncodePeerFrameError::GroupIdTooLarge {
                actual: group_id_len,
                maximum: usize::from(u16::MAX),
            })?;
        let message_len =
            u32::try_from(message_len).map_err(|_| EncodePeerFrameError::MessageLengthOverflow)?;

        Ok(EncodedLengths {
            body: body_len,
            complete: complete_len,
            group: group_len,
            message: message_len,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct EncodedLengths {
    body: u32,
    complete: usize,
    group: u16,
    message: u32,
}

fn validate_encoded_group<E>(
    actual: usize,
    codec_maximum: usize,
    wire_maximum: usize,
) -> Result<(), EncodePeerFrameError<E>> {
    if actual == 0 {
        return Err(EncodePeerFrameError::EmptyGroupId);
    }
    let maximum = codec_maximum.min(wire_maximum);
    if actual > maximum {
        return Err(EncodePeerFrameError::GroupIdTooLarge { actual, maximum });
    }
    Ok(())
}
