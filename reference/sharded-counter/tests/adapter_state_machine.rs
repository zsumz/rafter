use rafter::{LocalProposalId, LogIndex, Term};
use rafter_app::state_machine::{ApplyBatch, ApplyEntry, ReadBarrier, ReplicatedStateMachine};
use rafter_reference_sharded_counter::{
    adapter::{
        CounterApplyResult, CounterStateMachine, ReplicatedCounterCommand, SessionApplyResult,
        MAX_COMMAND_BYTES, MAX_SNAPSHOT_BYTES,
    },
    ClientId, CounterCommand, CounterResult, Delta, RequestFingerprint, RequestIdentity, Sequence,
    SessionEpoch,
};

fn session() -> ReplicatedCounterCommand {
    ReplicatedCounterCommand::OpenSession {
        client_id: ClientId::new(3),
        epoch: SessionEpoch::new(7).expect("session epoch is nonzero"),
    }
}

fn add() -> ReplicatedCounterCommand {
    let command = CounterCommand::Add {
        delta: Delta::new(5).expect("delta is nonzero"),
    };
    ReplicatedCounterCommand::Counter {
        request: RequestIdentity {
            client_id: ClientId::new(3),
            session_epoch: SessionEpoch::new(7).expect("session epoch is nonzero"),
            sequence: Sequence::first(),
            fingerprint: RequestFingerprint::of(&command),
        },
        command,
    }
}

fn entry(
    index: u64,
    proposal: u64,
    command: ReplicatedCounterCommand,
) -> ApplyEntry<ReplicatedCounterCommand> {
    ApplyEntry {
        index: LogIndex(index),
        term: Term(2),
        command,
        local_proposal_id: Some(LocalProposalId(proposal)),
    }
}

#[test]
fn command_codec_is_versioned_bounded_and_exact() {
    let machine = CounterStateMachine::new(8);
    for command in [session(), add(), ReplicatedCounterCommand::Faulty] {
        let encoded = machine
            .encode_command(&command)
            .expect("bounded command encodes");
        assert!(encoded.len() <= MAX_COMMAND_BYTES);
        assert_eq!(
            machine
                .decode_command(&encoded)
                .expect("encoded command decodes"),
            command
        );

        let mut trailing = encoded;
        trailing.push(0);
        assert!(
            machine.decode_command(&trailing).is_err(),
            "trailing bytes are never reinterpreted"
        );
    }
    assert!(
        machine.decode_command(&[0; MAX_COMMAND_BYTES + 1]).is_err(),
        "oversized frames fail before parsing"
    );
}

#[test]
fn snapshot_round_trip_keeps_applied_index_value_and_dedup_cache() {
    let mut machine = CounterStateMachine::new(8);
    let results = machine
        .apply_batch(ApplyBatch {
            entries: vec![
                entry(1, 1, session()),
                entry(2, 2, add()),
                entry(3, 3, add()),
            ],
        })
        .expect("session, mutation, and exact replay apply");
    assert_eq!(
        results[0].result,
        CounterApplyResult::Session(SessionApplyResult::Opened)
    );
    assert_eq!(
        results[1].result,
        CounterApplyResult::Counter(CounterResult::Added { value: 5 })
    );
    assert_eq!(
        results[2].result,
        CounterApplyResult::Counter(CounterResult::Added { value: 5 }),
        "exact replay returns the cache without applying twice"
    );

    let snapshot = machine
        .build_snapshot(LogIndex(3))
        .expect("snapshot builds at the applied index");
    assert!(snapshot.payload.len() <= MAX_SNAPSHOT_BYTES);
    let mut restored = CounterStateMachine::new(8);
    restored
        .install_snapshot(snapshot)
        .expect("bounded snapshot installs");
    assert_eq!(
        restored
            .read(
                (),
                ReadBarrier {
                    required_applied_index: LogIndex(3),
                    local_applied_index: LogIndex(3),
                },
            )
            .expect("restored state meets the barrier"),
        5
    );

    let replay = restored
        .apply_batch(ApplyBatch {
            entries: vec![entry(4, 4, add())],
        })
        .expect("restored dedup cache remains usable");
    assert_eq!(
        replay[0].result,
        CounterApplyResult::Counter(CounterResult::Added { value: 5 })
    );
}
