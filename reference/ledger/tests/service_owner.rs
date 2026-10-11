//! Public owner-loop adoption over real Raft files and the ledger's journal.
//! The package lane compiles this unchanged against extracted archive paths.

#[path = "support/scratch.rs"]
mod scratch;
#[allow(dead_code)]
mod support;

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, Input,
    LocalProposalId, LogIndex, NodeConfig, NodeId, Output, RaftSnapshotMetadata, SnapshotGroupId,
};
use rafter_app::state_machine::{ApplicationSnapshot, ApplyEntry, ReplicatedStateMachine};
use rafter_reference_ledger::{
    store::LedgerStore, AccountId, Command, DurableLedgerStateMachine, LedgerApplicationEntry,
    LedgerStateMachine, Mutation,
};
use rafter_runtime::{
    service::{
        CommittedCommand, DurableService, ServiceAdapterError, ServiceError, ServiceEvent,
        ServiceInput, ServiceOptions, ServiceState,
    },
    DurableRaftNode, DurableRaftNodeStorage,
};
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
    PersistedRaftSnapshot,
};
use scratch::ScratchDir;
use std::{
    path::Path,
    time::{Duration, Instant},
};
use support::{amount, config, execute, open_session};

const ACCOUNT: AccountId = AccountId::new(19);
type Owner = DurableService<
    FileRaftHardStateStore,
    FileRaftLogSegment,
    FileRaftSnapshotStore,
    LedgerApplicationEntry,
    DurableLedgerStateMachine,
>;
type Event = ServiceEvent<LedgerApplicationEntry, DurableLedgerStateMachine>;

fn raft_config() -> NodeConfig {
    NodeConfig::new(NodeId(1), vec![], 1).unwrap()
}
fn stores(
    root: &Path,
) -> DurableRaftNodeStorage<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore> {
    std::fs::create_dir_all(root.join("raft")).unwrap();
    let (hard_state_store, log_segment, snapshot_store) =
        FileRaftNodeStores::open(root.join("raft"))
            .unwrap()
            .into_parts();
    DurableRaftNodeStorage {
        hard_state_store,
        log_segment,
        snapshot_store,
    }
}
fn app(root: &Path, directory: &str) -> DurableLedgerStateMachine {
    std::fs::create_dir_all(root.join(directory)).unwrap();
    DurableLedgerStateMachine::new(
        LedgerStore::open(&root.join(directory), config(1, 2)).unwrap(),
        root.join("raft/snapshots"),
    )
}
fn adapt(command: CommittedCommand) -> Result<LedgerApplicationEntry, ServiceAdapterError> {
    let codec = LedgerStateMachine::new(config(1, 2));
    let decoded = codec.decode_command(command.payload.as_slice())?;
    drop(command.payload);
    Ok(LedgerApplicationEntry::new(ApplyEntry {
        index: command.index,
        term: command.term,
        command: decoded,
        local_proposal_id: command.local_proposal_id,
    })
    .with_protocol_prefix(command.first_index))
}
fn open(root: &Path, directory: &str) -> Owner {
    let mut options = ServiceOptions::new();
    options.application = options.application.with_limits(1, 4096, 1, 4096);
    Owner::recover(
        raft_config(),
        stores(root),
        app(root, directory),
        options,
        adapt,
    )
    .unwrap()
}
fn payload(command: &Command) -> Vec<u8> {
    LedgerStateMachine::new(config(1, 2))
        .encode_command(command)
        .unwrap()
}
fn poll_until(owner: &mut Owner, mut done: impl FnMut(&Owner) -> bool) -> Vec<Event> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut events = Vec::new();
    while !done(owner) {
        assert!(Instant::now() < deadline, "owner stalled: {owner:?}");
        if let Some(event) = owner.poll().unwrap() {
            events.push(event);
        }
        std::thread::yield_now();
    }
    events
}
fn shutdown(owner: &mut Owner) -> DurableLedgerStateMachine {
    owner.begin_shutdown();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < deadline);
        // This helper is called after all expected completions were observed.
        assert!(owner.poll().unwrap().is_none());
        match owner.finish_shutdown() {
            Ok(application) => return application,
            Err(ServiceError::NotReady(_)) => std::thread::yield_now(),
            other => panic!("unexpected shutdown: {other:?}"),
        }
    }
}

#[test]
fn owner_replays_bounded_work_acks_after_durability_and_installs_snapshot_on_reopen() {
    let scratch = ScratchDir::new("durable-owner-package-consumer");
    let root = scratch.path();
    seed_recovery(root);

    // Three committed commands exceed the owner's single application credit.
    let mut owner = open(root, "application");
    let events = poll_until(&mut owner, |o| o.state() == ServiceState::Ready);
    let applied: Vec<_> = events
        .into_iter()
        .map(|event| match event {
            ServiceEvent::Applied(done) => {
                assert!(done
                    .outcomes()
                    .iter()
                    .all(|outcome| outcome.local_proposal_id.is_none()));
                done.entries()[0].as_apply_entry().index
            }
            other => panic!("unexpected recovery event: {other:?}"),
        })
        .collect();
    assert_eq!(applied, [LogIndex(2), LogIndex(3), LogIndex(4)]);
    owner.try_step(Input::Tick.into()).unwrap();
    poll_until(&mut owner, |o| o.state() == ServiceState::Ready);
    owner
        .try_step(ServiceInput::from(Input::TrackedClientProposal {
            proposal_id: LocalProposalId(42),
            payload: payload(&execute(
                0,
                1,
                3,
                Mutation::Deposit {
                    account_id: ACCOUNT,
                    amount: amount(2),
                },
            )),
        }))
        .unwrap();
    owner.begin_shutdown();
    assert!(matches!(
        owner.finish_shutdown(),
        Err(ServiceError::NotReady(_))
    ));
    let deadline = Instant::now() + Duration::from_secs(10);
    let completion = loop {
        assert!(Instant::now() < deadline);
        match owner.poll().unwrap() {
            Some(ServiceEvent::Applied(done)) => break done,
            Some(ServiceEvent::Output(Output::LocalProposalAppended { .. })) | None => {}
            other => panic!("unexpected write event: {other:?}"),
        }
        std::thread::yield_now();
    };
    assert_eq!(
        completion.outcomes()[0].local_proposal_id,
        Some(LocalProposalId(42))
    );
    assert_eq!(owner.durable_through(), Some(LogIndex(6)));
    let mut application = shutdown(&mut owner);
    assert_eq!(application.store().ledger().view().accounts, [(ACCOUNT, 9)]);
    let snapshot = application.build_snapshot(LogIndex(6)).unwrap();
    drop(application);
    drop(owner);

    compact(root, snapshot);

    // A separate blank application recovers only from the promoted descriptor.
    let mut owner = open(root, "restored-application");
    let Some(ServiceEvent::InstallSnapshot {
        snapshot,
        mut application,
    }) = owner.poll().unwrap()
    else {
        panic!("snapshot required");
    };
    assert_eq!(owner.state(), ServiceState::InstallingSnapshot);
    application
        .install_snapshot(ApplicationSnapshot {
            applied_index: snapshot.metadata.last_included_index,
            payload: Vec::new(),
            raft_snapshot: Some(snapshot),
        })
        .unwrap();
    owner.resume_snapshot(application).unwrap();
    assert!(poll_until(&mut owner, |o| o.state() == ServiceState::Ready).is_empty());
    assert_eq!(
        shutdown(&mut owner).store().ledger().view().accounts,
        [(ACCOUNT, 9)]
    );
    drop(owner);

    // Previously acknowledged bytes survive reopening the original journal too.
    let mut owner = open(root, "application");
    assert!(poll_until(&mut owner, |o| o.state() == ServiceState::Ready).is_empty());
    let application = shutdown(&mut owner);
    assert_eq!(application.store().applied_index(), LogIndex(6));
    assert_eq!(application.store().ledger().view().accounts, [(ACCOUNT, 9)]);
}

fn seed_recovery(root: &Path) {
    let storage = stores(root);
    let mut node = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(),
        storage.hard_state_store,
        storage.log_segment,
        storage.snapshot_store,
    )
    .unwrap();
    node.step(Input::Tick).unwrap();
    for command in [
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
                amount: amount(7),
            },
        ),
    ] {
        node.step(Input::ClientProposal {
            payload: payload(&command),
        })
        .unwrap();
    }
    drop(node);
}

fn compact(root: &Path, snapshot: ApplicationSnapshot) {
    let storage = stores(root);
    let mut node = DurableRaftNode::with_storage_and_snapshot_store_applied_through(
        raft_config(),
        storage.hard_state_store,
        storage.log_segment,
        storage.snapshot_store,
        LogIndex(6),
    )
    .unwrap();
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("ledger-owner").unwrap(),
        NodeId(1),
        LogIndex(6),
        node.term_at_index(LogIndex(6)).unwrap(),
        node.current_term(),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("ledger").unwrap(),
            ApplicationSnapshotVersion::new(1).unwrap(),
        ),
    )
    .unwrap();
    node.compact_log_with_snapshot(PersistedRaftSnapshot {
        metadata,
        application_payload: snapshot.payload,
    })
    .unwrap();
    drop(node);
}
