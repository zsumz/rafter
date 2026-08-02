use std::{
    convert::Infallible,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use rafter::{Message, NodeId, RequestVote, Term};

use super::*;

#[derive(Debug)]
struct CountingGroupCodec {
    decodes: Arc<AtomicUsize>,
}

impl GroupIdCodec<u64> for CountingGroupCodec {
    type Error = Infallible;

    fn max_encoded_len(&self) -> usize {
        size_of::<u64>()
    }

    fn max_decoded_heap_bytes(&self) -> usize {
        0
    }

    fn encode(&self, group_id: &u64, output: &mut Vec<u8>) -> Result<(), Self::Error> {
        output.clear();
        output.extend_from_slice(&group_id.to_be_bytes());
        Ok(())
    }

    fn decode(&self, input: &[u8]) -> Result<u64, Self::Error> {
        let _ = self.decodes.fetch_add(1, Ordering::Relaxed);
        let encoded: [u8; 8] = input.try_into().expect("test group is fixed-width");
        Ok(u64::from_be_bytes(encoded))
    }
}

#[test]
fn staged_inbound_decode_reuses_the_canonical_group() {
    let decodes = Arc::new(AtomicUsize::new(0));
    let codec = PeerFrameCodec::new(
        CountingGroupCodec {
            decodes: Arc::clone(&decodes),
        },
        WireLimits::default(),
    )
    .expect("valid codec");
    let frame = PeerFrame::new(
        ConnectionSequence::FIRST,
        7,
        NodeId(1),
        NodeId(2),
        Message::RequestVote(RequestVote {
            term: Term(3),
            candidate_id: NodeId(1),
            last_log_index: rafter::LogIndex(4),
            last_log_term: Term(2),
        }),
    )
    .expect("matching sender");
    let mut encoded = Vec::new();
    let mut scratch = PeerFrameScratch::new();
    codec
        .encode_into(&mut encoded, &mut scratch, &frame)
        .expect("frame encodes");

    let routed = codec
        .decode_route(&encoded, &mut scratch)
        .expect("route decodes");
    assert_eq!(routed.route().group_id, 7);
    let decoded_frame =
        PeerFrameCodec::<u64, CountingGroupCodec>::decode_routed(routed).expect("message decodes");

    assert_eq!(decoded_frame, frame);
    assert_eq!(decodes.load(Ordering::Relaxed), 1);
    assert_eq!(codec.max_decoded_group_bytes(), size_of::<u64>());
}
