//! Connection-lifetime accounting for canonical group scratch storage.

use std::collections::TryReserveError;

use crate::queue::{ReceiveMemoryBudget, ReceiveMemoryPermit};
use crate::{GroupIdCodec, PeerFrameCodec, PeerFrameScratch};

pub(super) struct ReceiverScratch {
    frame: PeerFrameScratch,
    _declared_memory: ReceiveMemoryPermit,
    _allocator_excess_memory: Option<ReceiveMemoryPermit>,
}

#[derive(Debug)]
pub(super) enum ReceiverScratchError {
    MemoryFull,
    Allocation(TryReserveError),
}

impl ReceiverScratch {
    pub(super) fn acquire<G, C>(
        memory: &ReceiveMemoryBudget,
        codec: &PeerFrameCodec<G, C>,
    ) -> Result<Self, ReceiverScratchError>
    where
        C: GroupIdCodec<G>,
    {
        let declared = codec.max_encoded_group_bytes();
        let declared_memory = memory
            .try_acquire_scratch(declared)
            .map_err(|_| ReceiverScratchError::MemoryFull)?;
        let frame = PeerFrameScratch::try_with_group_id_capacity(declared)
            .map_err(ReceiverScratchError::Allocation)?;
        let allocator_excess = frame.group_id_capacity().saturating_sub(declared);
        let allocator_excess_memory = if allocator_excess == 0 {
            None
        } else {
            Some(
                memory
                    .try_acquire_scratch(allocator_excess)
                    .map_err(|_| ReceiverScratchError::MemoryFull)?,
            )
        };
        Ok(Self {
            frame,
            _declared_memory: declared_memory,
            _allocator_excess_memory: allocator_excess_memory,
        })
    }

    pub(super) const fn frame_mut(&mut self) -> &mut PeerFrameScratch {
        &mut self.frame
    }
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, mem::size_of};

    use rafter::{Message, NodeId, RequestVote, Term};

    use super::*;
    use crate::{
        ConnectionSequence, PeerFrame, ReceiveMemoryLimits, WireLimits,
        MIN_SAFE_DECODE_AMPLIFICATION, PEER_FRAME_FIXED_BODY_BYTES, PEER_FRAME_LENGTH_PREFIX_BYTES,
    };

    #[derive(Clone, Copy, Debug)]
    struct FullReserveCodec {
        maximum: usize,
    }

    impl GroupIdCodec<u8> for FullReserveCodec {
        type Error = Infallible;

        fn max_encoded_len(&self) -> usize {
            self.maximum
        }

        fn max_decoded_heap_bytes(&self) -> usize {
            0
        }

        fn encode(&self, group_id: &u8, output: &mut Vec<u8>) -> Result<(), Self::Error> {
            output.clear();
            output.reserve(self.maximum);
            output.push(*group_id);
            Ok(())
        }

        fn decode(&self, input: &[u8]) -> Result<u8, Self::Error> {
            Ok(*input.first().expect("test route contains one byte"))
        }
    }

    #[test]
    fn full_reserve_canonicalization_stays_charged_while_scratch_is_retained() {
        let maximum = usize::from(u16::MAX);
        let codec = codec(maximum);
        let frame = PeerFrame::new(
            ConnectionSequence::FIRST,
            7,
            NodeId(1),
            NodeId(2),
            request_vote(NodeId(1)),
        )
        .expect("matching sender");
        let mut encoded = Vec::new();
        let mut sender_scratch = PeerFrameScratch::new();
        codec
            .encode_into(&mut encoded, &mut sender_scratch, &frame)
            .expect("frame encodes");
        let group_length_at = PEER_FRAME_LENGTH_PREFIX_BYTES + 1 + size_of::<u64>();
        assert_eq!(
            u16::from_be_bytes([encoded[group_length_at], encoded[group_length_at + 1]]),
            1
        );
        drop(sender_scratch);

        let limits =
            ReceiveMemoryLimits::new(maximum.saturating_mul(4), MIN_SAFE_DECODE_AMPLIFICATION)
                .expect("test receive-memory limits");
        let memory = ReceiveMemoryBudget::new(limits, codec.max_decoded_group_bytes());
        let mut scratch = ReceiverScratch::acquire(&memory, &codec).expect("receiver scratch");
        let retained = scratch.frame.group_id_capacity();
        assert!(retained >= maximum);
        assert_eq!(memory.used(), retained);

        let frame_memory = memory
            .try_acquire_frame(encoded.len())
            .expect("frame memory");
        assert_eq!(
            memory.used(),
            retained + limits.charge(encoded.len(), codec.max_decoded_group_bytes())
        );
        assert_eq!(
            codec
                .decode(&encoded, scratch.frame_mut())
                .expect("frame decodes"),
            frame
        );
        drop(frame_memory);

        assert_eq!(scratch.frame.group_id_capacity(), retained);
        assert_eq!(memory.used(), retained);
        drop(scratch);
        assert_eq!(memory.used(), 0);
    }

    #[test]
    fn idle_receiver_scratch_reservations_cannot_exceed_the_global_budget() {
        let maximum = usize::from(u16::MAX);
        let codec = codec(maximum);
        let probe = PeerFrameScratch::try_with_group_id_capacity(maximum)
            .expect("capacity probe allocation");
        let per_connection = probe.group_id_capacity();
        drop(probe);
        let limits = ReceiveMemoryLimits::new(
            per_connection.saturating_mul(2),
            MIN_SAFE_DECODE_AMPLIFICATION,
        )
        .expect("two-connection receive-memory limit");
        let memory = ReceiveMemoryBudget::new(limits, codec.max_decoded_group_bytes());

        let first = ReceiverScratch::acquire(&memory, &codec).expect("first receiver scratch");
        let second = ReceiverScratch::acquire(&memory, &codec).expect("second receiver scratch");
        assert_eq!(memory.used(), limits.bytes_global());
        assert!(matches!(
            ReceiverScratch::acquire(&memory, &codec),
            Err(ReceiverScratchError::MemoryFull)
        ));
        assert_eq!(memory.used(), limits.bytes_global());

        drop(first);
        assert_eq!(memory.used(), per_connection);
        let third = ReceiverScratch::acquire(&memory, &codec).expect("replacement scratch");
        assert_eq!(memory.used(), limits.bytes_global());
        drop((second, third));
        assert_eq!(memory.used(), 0);
    }

    fn codec(maximum: usize) -> PeerFrameCodec<u8, FullReserveCodec> {
        let frame_body = PEER_FRAME_FIXED_BODY_BYTES
            .saturating_add(maximum)
            .saturating_add(1);
        let wire = WireLimits::new(frame_body, maximum).expect("maximum-width group route");
        PeerFrameCodec::new(FullReserveCodec { maximum }, wire).expect("compatible codec")
    }

    fn request_vote(sender: NodeId) -> Message {
        Message::RequestVote(RequestVote {
            term: Term(3),
            candidate_id: sender,
            last_log_index: rafter::LogIndex(42),
            last_log_term: Term(2),
        })
    }
}
