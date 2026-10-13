//! Numeric categories already exposed by the published 0.0.2-alpha.1 API.
//!
//! This guards ordinary downstream casts and ordering, not a Rust memory
//! layout or a new wire-encoding contract.
use rafter_service::{ReadErrorKind, TransferLeadershipErrorKind, WriteErrorKind};

fn published_order<T: Copy + Ord + std::fmt::Debug>(kinds: &[T], code: impl Fn(T) -> usize) {
    for (expected, kind) in kinds.iter().enumerate() {
        assert_eq!(
            code(*kind),
            expected,
            "published numeric category: {kind:?}"
        );
    }
    assert!(kinds.windows(2).all(|pair| pair[0] < pair[1]));
}

#[test]
fn published_error_kind_values_survive_additive_categories() {
    published_order(
        &[
            WriteErrorKind::NotLeader,
            WriteErrorKind::Rejected,
            WriteErrorKind::PayloadTooLarge,
            WriteErrorKind::UnknownOutcome,
            WriteErrorKind::WrongGroup,
            WriteErrorKind::StateMachine,
            WriteErrorKind::Storage,
            WriteErrorKind::Transport,
            WriteErrorKind::Unavailable,
            WriteErrorKind::ShuttingDown,
            WriteErrorKind::Poisoned,
            WriteErrorKind::LocalProposalIdExhausted,
            WriteErrorKind::ManagedInvariantViolation,
        ],
        |kind| kind as usize,
    );
    published_order(
        &[
            ReadErrorKind::NotLeader,
            ReadErrorKind::Rejected,
            ReadErrorKind::Canceled,
            ReadErrorKind::UnsupportedConsistency,
            ReadErrorKind::FreshnessUnavailable,
            ReadErrorKind::Abandoned,
            ReadErrorKind::WrongGroup,
            ReadErrorKind::StateMachine,
            ReadErrorKind::Storage,
            ReadErrorKind::Transport,
            ReadErrorKind::Unavailable,
            ReadErrorKind::ShuttingDown,
            ReadErrorKind::Poisoned,
            ReadErrorKind::ReadIdExhausted,
            ReadErrorKind::ManagedInvariantViolation,
        ],
        |kind| kind as usize,
    );
    published_order(
        &[
            TransferLeadershipErrorKind::NotLeader,
            TransferLeadershipErrorKind::Rejected,
            TransferLeadershipErrorKind::WrongGroup,
            TransferLeadershipErrorKind::Storage,
            TransferLeadershipErrorKind::Transport,
            TransferLeadershipErrorKind::ShuttingDown,
            TransferLeadershipErrorKind::Poisoned,
        ],
        |kind| kind as usize,
    );
}
