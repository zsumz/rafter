//! Connection-independent encoded peer-frame fields.

use rafter::NodeId;

use crate::wire::read::{put_u16, put_u32, put_u64, put_u8};
use crate::ConnectionSequence;

use super::{PEER_FRAME_FIXED_BODY_BYTES, PEER_FRAME_KIND_MESSAGE, PEER_FRAME_LENGTH_PREFIX_BYTES};

/// One validated frame whose live connection sequence is not assigned yet.
///
/// Group and message bytes are encoded before queue admission. A sender worker
/// adds the sequence belonging to the connection on which the frame is actually
/// attempted. Exact boxed slices keep byte accounting independent of spare
/// `Vec` capacity.
#[derive(Debug)]
pub(crate) struct PreparedPeerFrame {
    group_id: Box<[u8]>,
    message: Box<[u8]>,
    from: NodeId,
    to: NodeId,
    body_len: u32,
    complete_len: usize,
    group_len: u16,
    message_len: u32,
}

impl PreparedPeerFrame {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        group_id: Vec<u8>,
        message: Vec<u8>,
        from: NodeId,
        to: NodeId,
        body_len: u32,
        complete_len: usize,
        group_len: u16,
        message_len: u32,
    ) -> Self {
        Self {
            group_id: group_id.into_boxed_slice(),
            message: message.into_boxed_slice(),
            from,
            to,
            body_len,
            complete_len,
            group_len,
            message_len,
        }
    }

    pub(crate) const fn wire_len(&self) -> usize {
        self.complete_len
    }

    pub(crate) fn encode_into(&self, sequence: ConnectionSequence, output: &mut Vec<u8>) {
        output.clear();
        output.reserve(self.wire_len());
        put_u32(output, self.body_len);
        put_u8(output, PEER_FRAME_KIND_MESSAGE);
        put_u64(output, sequence.get());
        put_u16(output, self.group_len);
        output.extend_from_slice(&self.group_id);
        put_u64(output, self.from.0);
        put_u64(output, self.to.0);
        put_u32(output, self.message_len);
        output.extend_from_slice(&self.message);
        debug_assert_eq!(
            output.len(),
            PEER_FRAME_LENGTH_PREFIX_BYTES
                + PEER_FRAME_FIXED_BODY_BYTES
                + self.group_id.len()
                + self.message.len()
        );
    }
}
