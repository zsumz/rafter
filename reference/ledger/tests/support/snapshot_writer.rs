//! Shared setup for public streaming-encoder checks.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshotMetadata, SnapshotGroupId, Term,
};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplyBatch, ApplyEntry, ReplicatedStateMachine,
};
use rafter_reference_ledger::{AccountId, Command, LedgerStateMachine, Mutation};

use crate::support::{amount, config, execute, open_session};

pub fn metadata(at: LogIndex) -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("ledger-writer").unwrap(),
        NodeId(1),
        at,
        Term(1),
        Term(1),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("ledger").unwrap(),
            ApplicationSnapshotVersion::new(1).unwrap(),
        ),
    )
    .unwrap()
}

// Independent v1 image: floor 7, zero supply, account 5 with zero balance,
// and client 0 at epoch 1 with no cached completion.
pub fn golden() -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&7_u64.to_be_bytes());
    bytes.extend_from_slice(&0_u128.to_be_bytes());
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&5_u64.to_be_bytes());
    bytes.extend_from_slice(&0_u64.to_be_bytes());
    bytes.extend_from_slice(&1_u32.to_be_bytes());
    bytes.extend_from_slice(&0_u32.to_be_bytes());
    bytes.extend_from_slice(&1_u64.to_be_bytes());
    bytes.push(0);
    bytes
}

pub fn small() -> LedgerStateMachine {
    let mut app = LedgerStateMachine::new(config(1, 1));
    app.install_snapshot(ApplicationSnapshot {
        applied_index: LogIndex(7),
        payload: golden(),
        raft_snapshot: None,
    })
    .unwrap();
    app
}

pub fn batch(at: u64, commands: impl IntoIterator<Item = Command>) -> ApplyBatch<Command> {
    ApplyBatch {
        entries: commands
            .into_iter()
            .enumerate()
            .map(|(position, command)| ApplyEntry {
                index: LogIndex(at + u64::try_from(position).unwrap()),
                term: Term(1),
                command,
                local_proposal_id: None,
            })
            .collect(),
    }
}

pub fn large(accounts: usize) -> LedgerStateMachine {
    let mut app = LedgerStateMachine::new(config(1, accounts));
    app.apply_batch(batch(1, [open_session(0, 1)])).unwrap();
    // Bounded setup batches do not build a second complete command/image vector.
    for start in (0..accounts).step_by(256) {
        let commands = (start..(start + 256).min(accounts)).flat_map(|id| {
            let id = u64::try_from(id).unwrap();
            let account_id = AccountId::new(id);
            [
                execute(0, 1, id * 2 + 1, Mutation::OpenAccount { account_id }),
                execute(
                    0,
                    1,
                    id * 2 + 2,
                    Mutation::Deposit {
                        account_id,
                        amount: amount(1),
                    },
                ),
            ]
        });
        app.apply_batch(batch(u64::try_from(start).unwrap() * 2 + 2, commands))
            .unwrap();
    }
    app.apply_batch(batch(
        u64::try_from(accounts).unwrap() * 2 + 2,
        [open_session(0, 2)],
    ))
    .unwrap();
    app
}
