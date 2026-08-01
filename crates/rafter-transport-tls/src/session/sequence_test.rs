use super::{
    ConnectionSequence, InboundSequence, OutboundSequence, SequenceError, SequenceExhausted,
};

#[test]
fn outbound_sequence_exhausts_only_after_allocating_maximum() {
    let maximum = ConnectionSequence::new(u64::MAX).expect("nonzero");
    let mut sequence = OutboundSequence {
        next: Some(maximum),
    };

    assert_eq!(sequence.take_next(), Ok(maximum));
    assert_eq!(sequence.take_next(), Err(SequenceExhausted));
}

#[test]
fn inbound_sequence_exhausts_after_accepting_maximum() {
    let maximum = ConnectionSequence::new(u64::MAX).expect("nonzero");
    let mut sequence = InboundSequence {
        expected: Some(maximum),
    };

    assert_eq!(sequence.accept(maximum), Ok(()));
    assert_eq!(sequence.accept(maximum), Err(SequenceError::Exhausted));
}
