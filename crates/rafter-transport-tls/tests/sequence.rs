use rafter_transport_tls::{ConnectionSequence, InboundSequence, OutboundSequence, SequenceError};

#[test]
fn outbound_and_inbound_sequences_begin_at_one() {
    let mut outbound = OutboundSequence::new();
    let mut inbound = InboundSequence::new();

    let first = outbound.take_next().expect("first sequence");
    let second = outbound.take_next().expect("second sequence");
    assert_eq!(first.get(), 1);
    assert_eq!(second.get(), 2);
    inbound.accept(first).expect("accept first");
    inbound.accept(second).expect("accept second");
    assert_eq!(inbound.expected().expect("third expected").get(), 3);
}

#[test]
fn duplicate_and_skipped_sequences_do_not_advance_expectation() {
    let mut inbound = InboundSequence::new();
    let one = ConnectionSequence::new(1).expect("nonzero");
    let two = ConnectionSequence::new(2).expect("nonzero");

    assert_eq!(
        inbound.accept(two),
        Err(SequenceError::Unexpected {
            expected: one,
            actual: two,
        })
    );
    assert_eq!(inbound.expected(), Some(one));
    inbound.accept(one).expect("first remains acceptable");
    assert_eq!(
        inbound.accept(one),
        Err(SequenceError::Unexpected {
            expected: two,
            actual: one,
        })
    );
    assert_eq!(inbound.expected(), Some(two));
}
