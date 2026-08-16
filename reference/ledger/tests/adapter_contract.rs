mod support;

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId, Term,
};
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
        .expect_err("no bytes and no descriptor is nothing to install");
    restored
        .install_snapshot(promoted(&descriptor(LogIndex(2), &snapshot.payload)))
        .expect_err("a descriptor this replica's source has never held serves nothing");
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

/// The shape Rafter's own install path actually produces.
///
/// A Raft-driven install hands the application a descriptor and an empty
/// payload, because the bytes were staged and promoted into the replica's
/// snapshot store before the application was asked anything. This is the case
/// the declaration on `SNAPSHOT_SUPPORT` is about: a machine that declared
/// `Supported` and refused this shape would poison its group the first time a
/// follower fell behind a compaction, and no local round-trip test would notice.
#[test]
fn a_promoted_snapshot_installs_through_the_validation_an_inline_one_takes() {
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
                    amount: amount(50),
                },
            ),
        ],
    );

    let expected_view = app.ledger().view();
    let built = app.build_snapshot(LogIndex(3)).expect("snapshot builds");
    let promoted_descriptor = descriptor(LogIndex(3), &built.payload);

    let mut restored = LedgerStateMachine::new(config(1, 4));
    restored
        .register_promoted_snapshot(&promoted_descriptor, built.payload.clone())
        .expect("the registered bytes are the descriptor's own");
    restored
        .install_snapshot(promoted(&promoted_descriptor))
        .expect("a promoted payload installs");

    assert_eq!(
        restored.ledger().view(),
        expected_view,
        "every account, session, and cached result survives the promoted form too"
    );
    assert_eq!(restored.applied_index(), Ok(LogIndex(3)));
}

/// What the source says when it cannot serve the transfer, and what the machine
/// does about it.
///
/// The refusals here are what keep the fix from being a way around the
/// discipline rather than a subject of it: bytes that are not the descriptor's
/// bytes are refused at registration, a descriptor the source does not hold is
/// refused at install, and an install that would lower the applied floor is
/// refused whether its bytes came inline or off a store.
#[test]
fn a_promoted_install_the_source_cannot_serve_is_refused_and_changes_nothing() {
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

    let built = app.build_snapshot(LogIndex(2)).expect("snapshot builds");
    let promoted_descriptor = descriptor(LogIndex(2), &built.payload);

    let mut restored = LedgerStateMachine::new(config(1, 4));
    // Registration is where a payload that contradicts its descriptor stops.
    // Letting it through would leave the source answering for a transfer with
    // bytes nobody checked.
    let mut truncated = built.payload.clone();
    truncated.pop();
    assert!(matches!(
        restored.register_promoted_snapshot(&promoted_descriptor, truncated),
        Err(LedgerAdapterError::SnapshotPayloadUnavailable { .. })
    ));

    assert!(matches!(
        restored.install_snapshot(promoted(&promoted_descriptor)),
        Err(ApplicationSnapshotError::StateMachine(
            LedgerAdapterError::SnapshotPayloadUnavailable {
                applied_index: LogIndex(2)
            }
        ))
    ));
    assert_eq!(
        restored.applied_index(),
        Ok(LogIndex::ZERO),
        "a refused promoted install moved nothing"
    );
    assert_eq!(
        restored.ledger().view(),
        LedgerStateMachine::new(config(1, 4)).ledger().view(),
        "and installed nothing"
    );

    // A registration under a different transfer does not answer for this one:
    // the source is keyed by transfer id, not by "some snapshot arrived".
    let other = descriptor(LogIndex(2), &[9, 9, 9]);
    restored
        .register_promoted_snapshot(&other, vec![9, 9, 9])
        .expect("the registered bytes are that descriptor's own");
    assert!(matches!(
        restored.install_snapshot(promoted(&promoted_descriptor)),
        Err(ApplicationSnapshotError::StateMachine(
            LedgerAdapterError::SnapshotPayloadUnavailable { .. }
        ))
    ));

    restored
        .register_promoted_snapshot(&promoted_descriptor, built.payload.clone())
        .expect("the registered bytes are the descriptor's own");
    restored
        .install_snapshot(promoted(&promoted_descriptor))
        .expect("a promoted payload installs");
    apply(
        &mut restored,
        3,
        &[execute(
            0,
            1,
            2,
            Mutation::Deposit {
                account_id: ACCOUNT,
                amount: amount(10),
            },
        )],
    );
    assert!(
        matches!(
            restored.install_snapshot(promoted(&promoted_descriptor)),
            Err(ApplicationSnapshotError::StateMachine(
                LedgerAdapterError::SnapshotBehindAppliedIndex { .. }
            ))
        ),
        "a promoted install is refused behind the floor exactly as an inline one is"
    );
}

/// The Raft-visible descriptor a leader's compaction would publish for
/// `payload`.
///
/// `writer_id` is deliberately a node other than the one installing: a promoted
/// snapshot is by definition one this replica did not write.
fn descriptor(at: LogIndex, payload: &[u8]) -> RaftSnapshot {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("ledger").expect("a stable group id"),
        NodeId(2),
        at,
        Term(1),
        Term(1),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("ledger").expect("a stable kind"),
            ApplicationSnapshotVersion::new(1).expect("a non-zero version"),
        ),
    )
    .expect("a snapshot boundary above zero in a visible term");
    RaftSnapshot::from_payload(metadata, payload)
}

/// The install Rafter itself produces: the descriptor, and no bytes.
fn promoted(descriptor: &RaftSnapshot) -> ApplicationSnapshot {
    ApplicationSnapshot {
        applied_index: descriptor.metadata.last_included_index,
        payload: Vec::new(),
        raft_snapshot: Some(descriptor.clone()),
    }
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
