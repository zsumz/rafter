//! Focused tests for the Rafter adapter surface itself.
//!
//! These exercise the seams the replicated histories depend on but cannot
//! isolate: the versioned frames, the applied-index boundary, the read
//! barrier, and the line between a lock rejection and an adapter error.

mod support;

use std::error::Error;

use rafter::{LocalProposalId, LogIndex, NodeId, ProposalRejection, Term};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyEntry, ReadBarrier,
    ReplicatedStateMachine, SnapshotSupport,
};
use rafter_reference_fenced_lock::{
    decode_command, decode_result, encode_command, encode_result, ApplyDisposition, ApplyOutcome,
    Command, HistoryEvent, LockAdapterError, LockCodecError, LockQuery, LockRejection,
    LockResponse, LockStateMachine, NonZeroField, Operation, OperationId, OperationResult,
    RequestFingerprint, RequestIdentity, RequestRejection, ResourceNameError, SubmitOutcome,
};
use rafter_service::{
    ErrorCause, StateMachineOperation, UnknownOutcomeReason, WriteError, WriteErrorKind, WriteFate,
};

use support::{
    acquire, client, config, epoch, expire_through, lease, open_session, release, renew, resource,
    sequence, submit, submit_with_fingerprint, time, token,
};

const RESOURCE: &str = "orders/shard-0";

#[test]
fn command_frames_round_trip_every_command_shape() {
    let commands = [
        open_session(0, 1),
        submit(0, 1, 1, acquire(RESOURCE, 10)),
        submit(1, 7, 3, renew(RESOURCE, 2, 5)),
        submit(2, 1, 9, release(RESOURCE, 4)),
        submit(3, 2, 1, expire_through(0)),
        Command::Submit {
            request: RequestIdentity {
                client_id: client(1),
                session_epoch: epoch(4),
                sequence: sequence(6),
                fingerprint: RequestFingerprint::of(&Operation::Renew {
                    resource: resource("a"),
                    token: token(u64::MAX),
                    lease: lease(1),
                }),
            },
            operation: Operation::Renew {
                resource: resource("a"),
                token: token(u64::MAX),
                lease: lease(1),
            },
        },
    ];

    for command in commands {
        let frame = encode_command(&command);
        assert_eq!(
            decode_command(&frame),
            Ok(command),
            "the frame did not round-trip {command:?}"
        );
    }
}

#[test]
fn a_fingerprint_that_contradicts_its_operation_survives_the_frame() {
    // Recomputing the digest on decode would silently repair the malformed
    // envelope that `FingerprintMismatch` exists to reject, so the frame has to
    // carry whatever the client actually sent.
    let claimed = RequestFingerprint::of(&acquire(RESOURCE, 1));
    let command = submit_with_fingerprint(0, 1, 1, claimed, release(RESOURCE, 1));

    let decoded = decode_command(&encode_command(&command)).expect("a well-formed frame");

    assert_eq!(decoded, command);
    let Command::Submit { request, operation } = decoded else {
        panic!("expected a submission");
    };
    assert_eq!(request.fingerprint, claimed);
    assert_ne!(request.fingerprint, RequestFingerprint::of(&operation));
}

#[test]
fn result_frames_round_trip_every_response_shape() {
    let responses = [
        LockResponse::SessionOpened {
            session_epoch: epoch(3),
        },
        LockResponse::Operation(OperationResult::Acquired {
            token: token(1),
            expiry: time(10),
        }),
        LockResponse::Operation(OperationResult::Renewed {
            token: token(9),
            expiry: time(u64::MAX),
        }),
        LockResponse::Operation(OperationResult::Released),
        LockResponse::Operation(OperationResult::Expired {
            released_locks: u32::MAX,
            logical_time: time(7),
        }),
        LockResponse::Operation(OperationResult::Rejected(LockRejection::LockHeld {
            owner: client(2),
            token: token(4),
            expiry: time(11),
        })),
        LockResponse::Operation(OperationResult::Rejected(LockRejection::LockNotHeld)),
        LockResponse::Operation(OperationResult::Rejected(LockRejection::NotLockHolder {
            owner: client(0),
        })),
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::FencingTokenMismatch { current: token(5) },
        )),
        LockResponse::Operation(OperationResult::Rejected(LockRejection::LeaseOverflow)),
        LockResponse::Operation(OperationResult::Rejected(LockRejection::TokenExhausted)),
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::ResourceCapacityExceeded,
        )),
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::LogicalTimeNotAdvanced { current: time(3) },
        )),
        LockResponse::Rejected(RequestRejection::ClientOutOfRange),
        LockResponse::Rejected(RequestRejection::SessionNotOpen),
        LockResponse::Rejected(RequestRejection::StaleSession { current: epoch(2) }),
        LockResponse::Rejected(RequestRejection::FutureSession { current: epoch(8) }),
        LockResponse::Rejected(RequestRejection::StaleSequence {
            highest: sequence(4),
        }),
        LockResponse::Rejected(RequestRejection::SequenceGap {
            expected: sequence(5),
        }),
        LockResponse::Rejected(RequestRejection::ConflictingRetry),
        LockResponse::Rejected(RequestRejection::FingerprintMismatch {
            expected: RequestFingerprint::of(&acquire(RESOURCE, 2)),
        }),
    ];
    let dispositions = [
        ApplyDisposition::SessionOpened,
        ApplyDisposition::SessionReplaced,
        ApplyDisposition::SessionAlreadyOpen,
        ApplyDisposition::Applied,
        ApplyDisposition::Replayed,
        ApplyDisposition::Rejected,
    ];

    for response in responses {
        for disposition in dispositions {
            let outcome = ApplyOutcome {
                response,
                disposition,
            };
            assert_eq!(
                decode_result(&encode_result(&outcome)),
                Ok(outcome),
                "the frame did not round-trip {outcome:?}"
            );
        }
    }
}

#[test]
fn malformed_frames_are_refused_rather_than_repaired() {
    let valid = encode_command(&submit(0, 1, 1, acquire(RESOURCE, 10)));

    let mut wrong_version = valid.clone();
    wrong_version[0] = 9;
    assert_eq!(
        decode_command(&wrong_version),
        Err(LockCodecError::UnsupportedCommandVersion { version: 9 })
    );

    let mut unknown_command = valid.clone();
    unknown_command[1] = 77;
    assert_eq!(
        decode_command(&unknown_command),
        Err(LockCodecError::UnknownCommandTag { tag: 77 })
    );

    let truncated = &valid[..valid.len() - 1];
    assert!(matches!(
        decode_command(truncated),
        Err(LockCodecError::TruncatedFrame { .. })
    ));

    let mut trailing = valid.clone();
    trailing.push(0);
    assert_eq!(
        decode_command(&trailing),
        Err(LockCodecError::TrailingBytes { remaining: 1 })
    );

    // A zero lease would create a lock that is born expired, so the frame must
    // fail rather than hand the model a value its constructor forbids.
    let mut zero_lease = valid.clone();
    let lease_start = zero_lease.len() - 8;
    zero_lease[lease_start..].fill(0);
    assert_eq!(
        decode_command(&zero_lease),
        Err(LockCodecError::ZeroValuedField {
            field: NonZeroField::LeaseDuration,
        })
    );

    // Names are re-decided from the bytes, not trusted, because a frame is the
    // one place a name can arrive without having passed `ResourceName::new`.
    let mut bad_name = valid.clone();
    let name_start = 1 + 1 + 4 + 8 + 8 + 8 + 1 + 1;
    bad_name[name_start] = b' ';
    assert_eq!(
        decode_command(&bad_name),
        Err(LockCodecError::InvalidResourceName {
            reason: ResourceNameError::InvalidByte,
        })
    );

    let mut long_name = valid.clone();
    long_name[name_start - 1] = 200;
    assert_eq!(
        decode_command(&long_name),
        Err(LockCodecError::ResourceNameTooLong { declared: 200 })
    );
}

#[test]
fn the_applied_floor_moves_with_the_data_it_reflects() {
    let mut app = LockStateMachine::new(config(4, 4));
    assert_eq!(app.applied_index(), Ok(LogIndex::ZERO));

    let results = apply(&mut app, 1, open_session(0, 1));
    assert_eq!(
        results[0].result.disposition,
        ApplyDisposition::SessionOpened
    );
    assert_eq!(app.applied_index(), Ok(LogIndex(1)));

    let results = apply(&mut app, 4, submit(0, 1, 1, acquire(RESOURCE, 10)));
    assert_eq!(results[0].index, LogIndex(4));
    assert_eq!(
        app.applied_index(),
        Ok(LogIndex(4)),
        "the floor jumps to the entry the state now reflects"
    );
}

#[test]
fn a_committed_entry_at_or_below_the_applied_floor_is_refused() {
    let mut app = LockStateMachine::new(config(4, 4));
    apply(&mut app, 1, open_session(0, 1));
    let acknowledged = submit(0, 1, 1, acquire(RESOURCE, 10));
    apply(&mut app, 2, acknowledged);

    // Re-executing an acknowledged command would reissue a fencing token that a
    // guarded resource has already accepted.
    assert_eq!(
        app.apply_batch(ApplyBatch {
            entries: vec![entry(2, acknowledged)],
        }),
        Err(LockAdapterError::AppliedIndexRegression {
            entry_index: LogIndex(2),
            applied_index: LogIndex(2),
        })
    );
    assert_eq!(app.applied_index(), Ok(LogIndex(2)));
}

#[test]
fn contract_rejections_replicate_as_results_rather_than_adapter_errors() {
    let mut app = LockStateMachine::new(config(1, 1));
    apply(&mut app, 1, open_session(0, 1));
    apply(&mut app, 2, submit(0, 1, 1, acquire(RESOURCE, 10)));

    let held = apply(&mut app, 3, submit(0, 1, 2, acquire(RESOURCE, 10)));
    assert_eq!(
        held[0].result.response,
        LockResponse::Operation(OperationResult::Rejected(LockRejection::LockHeld {
            owner: client(0),
            token: token(1),
            expiry: time(10),
        })),
        "an owner extends a tenure with Renew, never with a second Acquire"
    );

    let out_of_range = apply(&mut app, 4, open_session(9, 1));
    assert_eq!(
        out_of_range[0].result.response,
        LockResponse::Rejected(RequestRejection::ClientOutOfRange)
    );

    let stale_horizon = apply(&mut app, 5, submit(0, 1, 3, expire_through(0)));
    assert_eq!(
        stale_horizon[0].result.response,
        LockResponse::Operation(OperationResult::Rejected(
            LockRejection::LogicalTimeNotAdvanced { current: time(0) }
        ))
    );
}

#[test]
fn a_read_barrier_beyond_the_applied_floor_is_refused() {
    let mut app = LockStateMachine::new(config(4, 4));
    apply(&mut app, 1, open_session(0, 1));
    apply(&mut app, 2, submit(0, 1, 1, acquire(RESOURCE, 10)));

    let query = LockQuery::GetLock {
        resource: resource(RESOURCE),
    };
    let status = app
        .read(query, barrier(2, 2))
        .expect("a satisfied barrier reads")
        .status();
    assert_eq!(status.holder.map(|holder| holder.token), Some(token(1)));
    assert_eq!(status.token_floor, Some(token(1)));

    assert_eq!(
        app.read(query, barrier(3, 2)),
        Err(LockAdapterError::ReadBarrierUnsatisfied {
            required_applied_index: LogIndex(3),
            applied_index: LogIndex(2),
        }),
        "a replica that has not applied far enough answers nothing"
    );
}

#[test]
fn querying_an_unknown_name_never_tracks_it() {
    let app = LockStateMachine::new(config(4, 4));

    let status = app
        .read(
            LockQuery::GetLock {
                resource: resource("never/acquired"),
            },
            barrier(0, 0),
        )
        .expect("an unsatisfied floor of zero is satisfied")
        .status();

    assert_eq!(status.holder, None);
    assert_eq!(status.token_floor, None, "an untracked name has no mark");
    assert_eq!(app.service().summary().tracked_resources, 0);
}

#[test]
fn durable_snapshots_are_declared_undefined_rather_than_refused_as_a_fault() {
    let mut app = LockStateMachine::new(config(4, 4));
    apply(&mut app, 1, open_session(0, 1));

    assert_eq!(
        LockStateMachine::SNAPSHOT_SUPPORT,
        SnapshotSupport::Unsupported,
        "the durable slice has not defined a byte representation yet"
    );
    // The declaration is the whole statement, so both bodies are the trait's
    // provided ones. A limitation this application declared must not arrive as
    // an application error a reader has to interpret as "not really a fault".
    assert!(
        matches!(
            app.build_snapshot(LogIndex(1)),
            Err(ApplicationSnapshotError::Unsupported)
        ),
        "shipping a format now would be a format the durable slice has to break"
    );
    assert!(matches!(
        app.install_snapshot(ApplicationSnapshot {
            applied_index: LogIndex(5),
            payload: Vec::new(),
            raft_snapshot: None,
        }),
        Err(ApplicationSnapshotError::Unsupported)
    ));
    assert_eq!(
        app.applied_index(),
        Ok(LogIndex(1)),
        "a refused install moved nothing"
    );
}

/// Every remaining adapter error is a genuine fault, which is what the module
/// comment claims and what the deleted snapshot variant used to contradict.
#[test]
fn every_adapter_error_carries_a_fault_rather_than_a_declared_limitation() {
    let faults = [
        LockAdapterError::Codec(LockCodecError::UnsupportedCommandVersion { version: 9 }),
        LockAdapterError::AppliedIndexRegression {
            entry_index: LogIndex(2),
            applied_index: LogIndex(2),
        },
        LockAdapterError::ReadBarrierUnsatisfied {
            required_applied_index: LogIndex(3),
            applied_index: LogIndex(2),
        },
    ];

    for fault in faults {
        let carried = ErrorCause::new(fault);
        assert_eq!(
            carried.downcast_ref::<LockAdapterError>(),
            Some(&fault),
            "a preserved cause hands this application back its own type"
        );
    }
}

/// The client's three-way classification and the history's terminal vocabulary
/// answer one question, and this is the only place they are compared.
///
/// `HistoryEvent::NotCommitted` claims the attempt provably never entered the
/// replicated log, and the service layer proves exactly that as
/// `WriteFate::NotAppended`. If the two ever disagreed, the checker would be
/// told a refusal the cluster never observed and would linearize an operation
/// as never having happened while its entry sat in a durable log.
#[test]
fn a_refusal_is_recorded_as_not_committed_exactly_when_the_write_was_not_appended() {
    let operation_id = OperationId::new(1);
    let mut refusals = 0_usize;
    let mut unknowns = 0_usize;

    for error in write_errors_across_the_surface() {
        let fate = error.fate();
        let kind = error.kind();
        let outcome = SubmitOutcome::from_write_error(error);
        let event = outcome.history_event(operation_id);

        assert_eq!(
            matches!(event, HistoryEvent::NotCommitted { .. }),
            fate == WriteFate::NotAppended,
            "{kind:?} earned {event:?} while the driver reported {fate:?}"
        );
        assert_eq!(
            matches!(event, HistoryEvent::Unknown { .. }),
            fate.may_commit(),
            "{kind:?} earned {event:?} while the driver reported {fate:?}"
        );
        match event {
            HistoryEvent::NotCommitted { .. } => refusals += 1,
            HistoryEvent::Unknown { .. } => unknowns += 1,
            other => panic!("a failed write is never {other:?}"),
        }
    }

    assert!(refusals > 0 && unknowns > 0, "the check saw both answers");
}

/// A state-machine failure reaches this application as its own type.
///
/// The write path carries the adapter's typed error rather than a rendered
/// message, so the recovery an embedder would write is expressible: match the
/// category, then recover the exact fault through the error chain.
#[test]
fn a_state_machine_failure_reaches_the_client_as_this_adapters_own_error() {
    let fault = LockAdapterError::AppliedIndexRegression {
        entry_index: LogIndex(2),
        applied_index: LogIndex(2),
    };
    let error = WriteError::StateMachine {
        operation: StateMachineOperation::ApplyBatch,
        fate: WriteFate::Unresolved,
        cause: ErrorCause::new(fault),
    };

    assert_eq!(error.kind(), WriteErrorKind::StateMachine);
    let source = error
        .source()
        .expect("a state-machine failure has a source");
    assert_eq!(
        source.downcast_ref::<LockAdapterError>(),
        Some(&fault),
        "`source()` reaches the preserved fault in one link, not the wrapper"
    );
    assert!(
        !format!("{error}").contains(&fault.to_string()),
        "the category's message does not repeat what the chain already prints"
    );
    assert!(
        SubmitOutcome::from_write_error(error).is_unknown(),
        "an apply failure after replication began leaves the outcome open"
    );
}

/// One representative error per public [`WriteErrorKind`], so a new variant
/// cannot join the surface without this file being updated.
fn write_errors_across_the_surface() -> Vec<WriteError> {
    let cause = || {
        ErrorCause::new(LockAdapterError::ReadBarrierUnsatisfied {
            required_applied_index: LogIndex(3),
            applied_index: LogIndex(2),
        })
    };
    vec![
        WriteError::NotLeader {
            leader_hint: Some(NodeId(2)),
            term: Term(4),
        },
        WriteError::Rejected {
            reason: ProposalRejection::LeadershipTransferInProgress { target: NodeId(3) },
        },
        WriteError::PayloadTooLarge { max: 8, actual: 64 },
        WriteError::UnknownOutcome {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            reason: UnknownOutcomeReason::DriveBoundReached,
        },
        WriteError::WrongGroup,
        WriteError::StateMachine {
            operation: StateMachineOperation::EncodeCommand,
            fate: WriteFate::NotAppended,
            cause: cause(),
        },
        WriteError::Storage {
            fate: WriteFate::Unresolved,
            cause: cause(),
        },
        WriteError::Transport {
            fate: WriteFate::NotAppended,
            cause: cause(),
        },
        WriteError::ShuttingDown,
        WriteError::Poisoned {
            fate: WriteFate::Unresolved,
            reason: "ApplyBatch failed".to_owned(),
            cause: Some(cause()),
        },
        WriteError::LocalProposalIdExhausted,
        WriteError::ManagedInvariantViolation {
            fate: WriteFate::NotAppended,
            message: "a driver reporting its own bug".to_owned(),
        },
    ]
}

#[test]
fn the_state_machine_encodes_and_decodes_through_its_own_frames() {
    let app = LockStateMachine::new(config(4, 4));
    let command = submit(0, 1, 1, acquire(RESOURCE, 10));

    let payload = app.encode_command(&command).expect("encoding never fails");

    assert_eq!(payload, encode_command(&command));
    assert_eq!(app.decode_command(&payload), Ok(command));
    assert!(matches!(
        app.decode_command(&[0]),
        Err(LockAdapterError::Codec(
            LockCodecError::UnsupportedCommandVersion { version: 0 }
        ))
    ));
}

fn apply(
    app: &mut LockStateMachine,
    index: u64,
    command: Command,
) -> Vec<rafter_app::state_machine::ApplyResult<ApplyOutcome>> {
    app.apply_batch(ApplyBatch {
        entries: vec![entry(index, command)],
    })
    .expect("well-formed commands apply")
}

fn entry(index: u64, command: Command) -> ApplyEntry<Command> {
    ApplyEntry {
        index: LogIndex(index),
        term: Term(1),
        command,
        local_proposal_id: None,
    }
}

const fn barrier(required: u64, local: u64) -> ReadBarrier {
    ReadBarrier {
        required_applied_index: LogIndex(required),
        local_applied_index: LogIndex(local),
    }
}
