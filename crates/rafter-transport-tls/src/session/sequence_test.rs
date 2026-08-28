//! The exact frame at which a connection's sequence space runs out.
//!
//! Outbound must hand out the maximum before reporting exhaustion, and inbound
//! must accept that maximum once and refuse afterwards — an off-by-one either
//! wastes a legal frame or admits a replay at the top of the space.

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
