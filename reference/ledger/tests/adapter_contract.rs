mod support;

use rafter::{LogIndex, Term};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyEntry, ReadBarrier,
    ReplicatedStateMachine,
};
use rafter_reference_ledger::{
    AccountId, ApplyDisposition, BusinessRejection, Command, LedgerAdapterError, LedgerCodecError,
    LedgerQuery, LedgerQueryResult, LedgerResponse, LedgerStateMachine, Mutation, MutationResult,
    NonZeroField, RequestRejection,
};
use support::{amount, config, execute, open_session};

const ACCOUNT: AccountId = AccountId::new(4);

#[test]
fn every_command_shape_round_trips_through_its_versioned_frame() {
    let app = LedgerStateMachine::new(config(2, 4));
    for command in [
        open_session(0, 1),
        open_session(1, u64::MAX),
        execute(
            0,
            1,
            1,
            Mutation::OpenAccount {
                account_id: ACCOUNT,
            },
        ),
        execute(
            0,
            2,
            3,
            Mutation::Deposit {
                account_id: ACCOUNT,
                amount: amount(u64::MAX),
            },
        ),
        execute(
            1,
            1,
            u64::MAX,
            Mutation::Transfer {
                from: ACCOUNT,
                to: AccountId::new(5),
                amount: amount(1),
            },
        ),
        execute(
            1,
            1,
            2,
            Mutation::CloseAccount {
                account_id: ACCOUNT,
            },
        ),
    ] {
        let payload = app.encode_command(&command).expect("commands encode");
        assert_eq!(
            app.decode_command(&payload),
            Ok(command),
            "frame did not round trip"
        );
    }
}

#[test]
fn malformed_frames_are_rejected_rather_than_guessed_at() {
    let app = LedgerStateMachine::new(config(2, 4));
    let payload = app
        .encode_command(&open_session(0, 1))
        .expect("commands encode");

    assert_eq!(
        app.decode_command(&[]),
        Err(LedgerAdapterError::Codec(
            LedgerCodecError::TruncatedFrame {
                required: 1,
                available: 0
            }
        ))
    );
    assert_eq!(
        app.decode_command(&payload[..payload.len() - 1]),
        Err(LedgerAdapterError::Codec(
            LedgerCodecError::TruncatedFrame {
                required: 8,
                available: 7
            }
        ))
    );

    let mut trailing = payload.clone();
    trailing.push(0);
    assert_eq!(
        app.decode_command(&trailing),
        Err(LedgerAdapterError::Codec(LedgerCodecError::TrailingBytes {
            remaining: 1
        }))
    );

    let mut wrong_version = payload.clone();
    wrong_version[0] = 9;
    assert_eq!(
        app.decode_command(&wrong_version),
        Err(LedgerAdapterError::Codec(
            LedgerCodecError::UnsupportedCommandVersion { version: 9 }
        ))
    );

    let mut unknown_command = payload.clone();
    unknown_command[1] = 7;
    assert_eq!(
        app.decode_command(&unknown_command),
        Err(LedgerAdapterError::Codec(
            LedgerCodecError::UnknownCommandTag { tag: 7 }
        ))
    );

    let mut zero_epoch = payload;
    for byte in &mut zero_epoch[6..] {
        *byte = 0;
    }
    assert_eq!(
        app.decode_command(&zero_epoch),
        Err(LedgerAdapterError::Codec(
            LedgerCodecError::ZeroValuedField {
                field: NonZeroField::SessionEpoch
            }
        ))
    );
}

#[test]
fn contract_rejections_are_results_and_advance_the_applied_index() {
    let mut app = LedgerStateMachine::new(config(1, 4));
    let results = apply(
        &mut app,
        1,
        &[
            open_session(0, 1),
            // A session rejection: client slot 1 is outside the configured
            // bound, so this never reaches ledger semantics.
            open_session(1, 1),
            execute(
                0,
                1,
                1,
                Mutation::OpenAccount {
                    account_id: ACCOUNT,
                },
            ),
            // A business rejection: admitted under its identity, then refused
            // by ledger rules.
            execute(
                0,
                1,
                2,
                Mutation::CloseAccount {
                    account_id: AccountId::new(9),
                },
            ),
        ],
    );

    assert_eq!(
        results[1].response,
        LedgerResponse::Rejected(RequestRejection::ClientOutOfRange)
    );
    assert_eq!(results[1].disposition, ApplyDisposition::Rejected);
    assert_eq!(
        results[3].response,
        LedgerResponse::Mutation(MutationResult::Rejected(BusinessRejection::AccountNotFound))
    );
    assert_eq!(results[3].disposition, ApplyDisposition::Applied);
    assert_eq!(
        app.applied_index(),
        Ok(LogIndex(4)),
        "rejected commands still consumed their log entries"
    );
}

#[test]
fn reads_require_the_freshness_their_barrier_declares() {
    let mut app = LedgerStateMachine::new(config(1, 4));
    apply(
        &mut app,
        1,
        &[
            open_session(0, 1),
            execute(
                0,
                1,
                1,
                Mutation::OpenAccount {
                    account_id: ACCOUNT,
                },
            ),
            execute(
                0,
                1,
                2,
                Mutation::Deposit {
                    account_id: ACCOUNT,
                    amount: amount(6),
                },
            ),
        ],
    );

    assert_eq!(
        app.read(
            LedgerQuery::GetAccount {
                account_id: ACCOUNT
            },
            barrier(3),
        ),
        Ok(LedgerQueryResult::Account {
            account_id: ACCOUNT,
            balance: Some(6),
        })
    );
    assert_eq!(
        app.read(LedgerQuery::GetLedgerSummary, barrier(3)),
        Ok(LedgerQueryResult::Summary(app.ledger().summary()))
    );
    assert_eq!(
        app.read(LedgerQuery::GetLedgerSummary, barrier(4)),
        Err(LedgerAdapterError::ReadBarrierUnsatisfied {
            required_applied_index: LogIndex(4),
            applied_index: LogIndex(3),
        })
    );
}

#[test]
fn snapshots_bind_their_payload_to_one_applied_index() {
    let mut app = LedgerStateMachine::new(config(1, 4));
    apply(
        &mut app,
        1,
        &[
            open_session(0, 1),
            execute(
                0,
                1,
                1,
                Mutation::OpenAccount {
                    account_id: ACCOUNT,
                },
            ),
        ],
    );

    assert!(
        matches!(
            app.build_snapshot(LogIndex(1)),
            Err(ApplicationSnapshotError::StateMachine(
                LedgerAdapterError::SnapshotIndexUnavailable {
                    requested_index: LogIndex(1),
                    applied_index: LogIndex(2),
                }
            ))
        ),
        "state only exists at the current applied index"
    );

    let snapshot = app.build_snapshot(LogIndex(2)).expect("snapshot builds");
    let mut restored = LedgerStateMachine::new(config(1, 4));
    restored
        .install_snapshot(ApplicationSnapshot {
            applied_index: LogIndex(2),
            payload: Vec::new(),
            raft_snapshot: None,
        })
        .expect_err("a descriptor-only install has no payload to read here");
    restored
        .install_snapshot(ApplicationSnapshot {
            applied_index: LogIndex(3),
            payload: snapshot.payload.clone(),
            raft_snapshot: None,
        })
        .expect_err("the payload's own index must match the declared index");

    restored
        .install_snapshot(snapshot)
        .expect("a matching snapshot installs");
    assert_eq!(restored.ledger().view(), app.ledger().view());
    assert!(
        matches!(
            restored.install_snapshot(ApplicationSnapshot {
                applied_index: LogIndex(1),
                payload: vec![1],
                raft_snapshot: None,
            }),
            Err(ApplicationSnapshotError::StateMachine(
                LedgerAdapterError::SnapshotBehindAppliedIndex {
                    snapshot_index: LogIndex(1),
                    applied_index: LogIndex(2),
                }
            ))
        ),
        "an older snapshot would make acknowledged commands executable again"
    );
}

const fn barrier(required_applied_index: u64) -> ReadBarrier {
    ReadBarrier {
        required_applied_index: LogIndex(required_applied_index),
        local_applied_index: LogIndex(required_applied_index),
    }
}

/// Applies commands at consecutive log indexes starting at `first_index`.
fn apply(
    app: &mut LedgerStateMachine,
    first_index: u64,
    commands: &[Command],
) -> Vec<rafter_reference_ledger::ApplyOutcome> {
    let entries = commands
        .iter()
        .zip(first_index..)
        .map(|(command, index)| ApplyEntry {
            index: LogIndex(index),
            term: Term(1),
            command: command.clone(),
            local_proposal_id: None,
        })
        .collect();
    app.apply_batch(ApplyBatch { entries })
        .expect("well-formed batches apply")
        .into_iter()
        .map(|result| result.result)
        .collect()
}
