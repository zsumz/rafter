//! Unit tests for the managed service error surface.

use std::collections::BTreeSet;

use super::*;

#[derive(Debug)]
struct TestCause(&'static str);

impl fmt::Display for TestCause {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TestCause {}

fn every_write_error() -> Vec<WriteError> {
    vec![
        WriteError::NotLeader {
            leader_hint: Some(NodeId(2)),
            term: Term(7),
        },
        WriteError::Rejected {
            reason: ProposalRejection::LeadershipTransferInProgress { target: NodeId(2) },
        },
        WriteError::PayloadTooLarge { max: 4, actual: 9 },
        WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::EmptyNetwork,
        },
        WriteError::WrongGroup,
        WriteError::StateMachine {
            operation: StateMachineOperation::EncodeCommand,
            fate: WriteFate::NotAppended,
            cause: ErrorCause::new(TestCause("encode failed")),
        },
        WriteError::Storage {
            fate: WriteFate::NotAppended,
            cause: ErrorCause::new(TestCause("disk full")),
        },
        WriteError::Transport {
            fate: WriteFate::Unresolved,
            cause: ErrorCause::new(TestCause("link down")),
        },
        WriteError::ShuttingDown,
        WriteError::Poisoned {
            fate: WriteFate::Unresolved,
            reason: "ApplyBatch failed".to_owned(),
            cause: Some(ErrorCause::new(TestCause("apply failed"))),
        },
        WriteError::LocalProposalIdExhausted,
        WriteError::ManagedInvariantViolation {
            fate: WriteFate::NotAppended,
            message: "generated a non-monotonic local proposal id".to_owned(),
        },
    ]
}

fn every_read_error() -> Vec<ReadError> {
    vec![
        ReadError::NotLeader {
            leader_hint: None,
            term: Term(1),
        },
        ReadError::Rejected {
            read_id: Some(ReadId(1)),
            reason: ReadIndexRejection::NoCommitInCurrentTerm,
            leader_hint: None,
        },
        ReadError::Canceled {
            read_id: ReadId(1),
            reason: ReadIndexCancelReason::LeadershipLost,
            leader_hint: None,
        },
        ReadError::UnsupportedConsistency {
            consistency: ReadConsistency::Local,
        },
        ReadError::FreshnessUnavailable {
            read_id: None,
            required_applied_index: LogIndex(3),
            local_applied_index: LogIndex(2),
        },
        ReadError::Abandoned {
            read_id: ReadId(1),
            reason: ReadAbandonReason::DriveBoundReached,
        },
        ReadError::WrongGroup,
        ReadError::StateMachine {
            operation: StateMachineOperation::Read,
            cause: ErrorCause::new(TestCause("query evaluation broke")),
        },
        ReadError::Storage {
            cause: ErrorCause::new(TestCause("disk full")),
        },
        ReadError::Transport {
            cause: ErrorCause::new(TestCause("link down")),
        },
        ReadError::ShuttingDown,
        ReadError::Poisoned {
            reason: "ApplyBatch failed".to_owned(),
            cause: None,
        },
        ReadError::ReadIdExhausted,
        ReadError::ManagedInvariantViolation {
            message: "generated a duplicate read id".to_owned(),
        },
    ]
}

#[test]
fn write_error_exposes_the_preserved_error_as_its_source() {
    let error = WriteError::Storage {
        fate: WriteFate::NotAppended,
        cause: ErrorCause::new(TestCause("persisted Raft log diverges")),
    };

    let source = error.source().expect("the preserved cause is the source");

    assert_eq!(source.to_string(), "persisted Raft log diverges");
    assert!(
        source.source().is_none(),
        "the cause is a handle, so the chain has one link per real failure"
    );
}

#[test]
fn a_preserved_cause_downcasts_to_the_error_the_driver_kept() {
    let error = WriteError::Transport {
        fate: WriteFate::Unresolved,
        cause: ErrorCause::new(TestCause("link down")),
    };

    let WriteError::Transport { cause, .. } = &error else {
        panic!("expected a transport error, got {error:?}");
    };

    assert_eq!(
        cause
            .downcast_ref::<TestCause>()
            .expect("the driver's own error keeps its type")
            .0,
        "link down"
    );
}

#[test]
fn display_states_the_category_without_repeating_the_cause() {
    for error in every_write_error() {
        let rendered = error.to_string();
        assert!(
            !rendered.contains("disk full")
                && !rendered.contains("link down")
                && !rendered.contains("encode failed"),
            "a chain printer would print the cause twice: {rendered}"
        );
    }
    for error in every_read_error() {
        let rendered = error.to_string();
        assert!(
            !rendered.contains("disk full")
                && !rendered.contains("link down")
                && !rendered.contains("query evaluation broke"),
            "a chain printer would print the cause twice: {rendered}"
        );
    }
}

/// Property-style, so a new variant cannot silently share another's bucket.
#[test]
fn every_write_error_kind_is_distinct_from_every_other() {
    let errors = every_write_error();
    let kinds = errors.iter().map(WriteError::kind).collect::<BTreeSet<_>>();

    assert_eq!(kinds.len(), errors.len());
}

#[test]
fn every_read_error_kind_is_distinct_from_every_other() {
    let errors = every_read_error();
    let kinds = errors.iter().map(ReadError::kind).collect::<BTreeSet<_>>();

    assert_eq!(kinds.len(), errors.len());
}

fn every_transfer_leadership_error() -> Vec<TransferLeadershipError> {
    vec![
        TransferLeadershipError::NotLeader {
            leader_hint: Some(NodeId(2)),
            term: Term(7),
        },
        TransferLeadershipError::Rejected {
            reason: LeadershipTransferRejection::TargetNotVoter,
            leader_hint: None,
        },
        TransferLeadershipError::WrongGroup,
        TransferLeadershipError::Storage {
            cause: ErrorCause::new(TestCause("disk full")),
        },
        TransferLeadershipError::Transport {
            cause: ErrorCause::new(TestCause("link down")),
        },
        TransferLeadershipError::ShuttingDown,
        TransferLeadershipError::Poisoned {
            reason: "ApplyBatch failed".to_owned(),
            cause: None,
        },
    ]
}

fn every_shutdown_error() -> Vec<ShutdownError> {
    vec![
        ShutdownError::WrongGroup,
        ShutdownError::Transport {
            cause: ErrorCause::new(TestCause("link down")),
        },
        ShutdownError::AlreadyShutDown,
    ]
}

/// The same property the write and read surfaces carry, over the two that had
/// no projection at all: a category that collided with another would silently
/// merge two facts in an operator's aggregate.
#[test]
fn every_transfer_leadership_error_kind_is_distinct_from_every_other() {
    let errors = every_transfer_leadership_error();
    let kinds = errors
        .iter()
        .map(TransferLeadershipError::kind)
        .collect::<BTreeSet<_>>();

    assert_eq!(kinds.len(), errors.len());
}

#[test]
fn every_shutdown_error_kind_is_distinct_from_every_other() {
    let errors = every_shutdown_error();
    let kinds = errors
        .iter()
        .map(ShutdownError::kind)
        .collect::<BTreeSet<_>>();

    assert_eq!(kinds.len(), errors.len());
}

/// The point of the projection, asserted rather than described: all four
/// surfaces are aggregable by the same shape of key, so an operator counting
/// driver failures has four buckets to fill rather than two and two strings.
#[test]
fn all_four_operation_surfaces_project_to_a_category() {
    fn tally<K: Ord>(kinds: impl IntoIterator<Item = K>) -> BTreeSet<K> {
        kinds.into_iter().collect()
    }

    assert_eq!(
        tally(every_write_error().iter().map(WriteError::kind)).len(),
        every_write_error().len()
    );
    assert_eq!(
        tally(every_read_error().iter().map(ReadError::kind)).len(),
        every_read_error().len()
    );
    assert_eq!(
        tally(
            every_transfer_leadership_error()
                .iter()
                .map(TransferLeadershipError::kind)
        )
        .len(),
        every_transfer_leadership_error().len()
    );
    assert_eq!(
        tally(every_shutdown_error().iter().map(ShutdownError::kind)).len(),
        every_shutdown_error().len()
    );
}

/// Property-style, so a new refusal variant cannot silently join the
/// unresolved side and tell a client its request identity may be spent.
#[test]
fn fate_is_not_appended_for_every_refusal_variant() {
    for error in every_write_error() {
        let expected_refusal = matches!(
            error.kind(),
            WriteErrorKind::NotLeader
                | WriteErrorKind::Rejected
                | WriteErrorKind::PayloadTooLarge
                | WriteErrorKind::WrongGroup
                | WriteErrorKind::ShuttingDown
                | WriteErrorKind::LocalProposalIdExhausted
        );
        if expected_refusal {
            assert_eq!(
                error.fate(),
                WriteFate::NotAppended,
                "reaching {:?} is the proof the command was refused",
                error.kind()
            );
            assert!(!error.fate().may_commit());
        }
        if matches!(error.kind(), WriteErrorKind::UnknownOutcome) {
            assert!(error.fate().may_commit());
        }
    }
}

#[test]
fn write_error_is_a_standard_error_with_display_message() {
    let error = WriteError::Storage {
        fate: WriteFate::NotAppended,
        cause: ErrorCause::new(TestCause("disk full")),
    };
    let standard_error: &(dyn Error + 'static) = &error;

    assert_eq!(
        standard_error.to_string(),
        "write storage failed; the command was not appended"
    );
}

#[test]
fn read_error_formats_leader_hint_without_debug_dump() {
    let error = ReadError::NotLeader {
        leader_hint: Some(NodeId(2)),
        term: Term(7),
    };

    assert_eq!(
        error.to_string(),
        "read rejected: this node is not leader in term 7; leader hint is node-2"
    );
}

#[test]
fn shutdown_error_is_a_standard_error() {
    let error = ShutdownError::AlreadyShutDown;
    let standard_error: &(dyn Error + 'static) = &error;

    assert_eq!(standard_error.to_string(), "service is already shut down");
}
