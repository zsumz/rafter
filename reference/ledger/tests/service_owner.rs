//! Public owner-loop adoption over real Raft files and the ledger's journal.
//! The package lane compiles this unchanged against extracted archive paths.

#[path = "support/scratch.rs"]
mod scratch;
#[path = "support/snapshot_source.rs"]
mod snapshot_source;
#[allow(dead_code)]
mod support;

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, Input,
    LocalProposalId, LogIndex, NodeConfig, NodeId, Output, RaftSnapshotMetadata, ReadId,
    SnapshotGroupId,
};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplyEntry, ReadBarrier, ReplicatedStateMachine,
};
use rafter_reference_ledger::{
    store::LedgerStore, AccountId, Command, DurableLedgerStateMachine, LedgerApplicationEntry,
    LedgerQuery, LedgerQueryResult, LedgerStateMachine, Mutation,
};
use rafter_runtime::{
    service::{
        CommittedCommand, DurableService, ServiceAdapterError, ServiceError, ServiceEvent,
        ServiceInput, ServiceOptions, ServicePoll, ServiceReadRejection, ServiceState,
    },
    snapshot_io::SnapshotCancellation,
    DurableRaftNode, DurableRaftNodeStorage,
};
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
};
use scratch::ScratchDir;
use std::{
    path::Path,
    sync::mpsc,
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
fn open(root: &Path, directory: &str) -> (Owner, mpsc::Receiver<()>) {
    let (notify, wake) = mpsc::sync_channel(1);
    let mut options = ServiceOptions::new();
    options.application = options.application.with_limits(1, 4096, 1, 4096);
    let owner = Owner::recover_with_wake(
        raft_config(),
        stores(root),
        app(root, directory),
        options,
        adapt,
        move || {
            let _ = notify.try_send(());
        },
    )
    .unwrap();
    (owner, wake)
}
fn payload(command: &Command) -> Vec<u8> {
    LedgerStateMachine::new(config(1, 2))
        .encode_command(command)
        .unwrap()
}
fn poll_until(
    owner: &mut Owner,
    wake: &mpsc::Receiver<()>,
    mut done: impl FnMut(&Owner) -> bool,
) -> Vec<Event> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut events = Vec::new();
    while !done(owner) {
        assert!(Instant::now() < deadline, "owner stalled: {owner:?}");
        match owner.poll_next().unwrap() {
            ServicePoll::Event(event) => events.push(event),
            ServicePoll::Progress => {}
            ServicePoll::Pending if done(owner) => break,
            ServicePoll::Pending => wait(wake, deadline),
            other => panic!("unknown scheduling disposition: {other:?}"),
        }
    }
    events
}
fn shutdown(owner: &mut Owner, wake: &mpsc::Receiver<()>) -> DurableLedgerStateMachine {
    owner.begin_shutdown();
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(Instant::now() < deadline);
        // This helper is called after all expected completions were observed.
        let pending = match owner.poll_next().unwrap() {
            ServicePoll::Progress => false,
            ServicePoll::Pending => true,
            other => panic!("unexpected remaining event: {other:?}"),
        };
        match owner.finish_shutdown() {
            Ok(application) => return application,
            Err(ServiceError::NotReady(_)) => {
                if pending {
                    wait(wake, deadline);
                }
            }
            other => panic!("unexpected shutdown: {other:?}"),
        }
    }
}

fn wait(wake: &mpsc::Receiver<()>, deadline: Instant) {
    wake.recv_timeout(deadline.saturating_duration_since(Instant::now()))
        .expect("worker wake required; no periodic poll fallback");
}

fn assert_read_refused(owner: &mut Owner, state: ServiceState) {
    let error = owner
        .try_read_application(LogIndex::ZERO, |_| panic!("closed owner ran query"))
        .unwrap_err();
    assert_eq!(error.rejection(), ServiceReadRejection::NotReady(state));
}

fn assert_balance(owner: &mut Owner, required: LogIndex, balance: u64) {
    let read = owner
        .try_read_application(required, |application| {
            application.read(
                LedgerQuery::GetAccount {
                    account_id: ACCOUNT,
                },
                ReadBarrier {
                    required_applied_index: required,
                    local_applied_index: application.store().applied_index(),
                },
            )
        })
        .unwrap();
    assert_eq!(read.durable_through(), required);
    assert_eq!(
        read.into_output().unwrap(),
        LedgerQueryResult::Account {
            account_id: ACCOUNT,
            balance: Some(balance),
        }
    );
}

fn assert_granted_balance(
    owner: &mut Owner,
    wake: &mpsc::Receiver<()>,
    required: LogIndex,
    balance: u64,
) {
    let read_id = ReadId(1);
    owner.try_step(Input::ReadIndex { read_id }.into()).unwrap();
    let events = poll_until(owner, wake, |o| o.state() == ServiceState::Ready);
    let granted = events
        .into_iter()
        .find_map(|event| match event {
            ServiceEvent::Output(Output::ReadIndexGranted {
                read_id: actual,
                read_index,
            }) if actual == read_id => Some(read_index),
            _ => None,
        })
        .expect("the real Raft read grant must be delivered");
    let application_floor = owner
        .ready_node()
        .unwrap()
        .committed_application_index_through(granted);
    assert_eq!(application_floor, required);
    assert!(
        granted > required,
        "leadership no-op needs no phantom application update"
    );
    assert_balance(owner, application_floor, balance);
}

#[test]
fn owner_replays_bounded_work_acks_after_durability_and_installs_snapshot_on_reopen() {
    let scratch = ScratchDir::new("durable-owner-package-consumer");
    let root = scratch.path();
    seed_recovery(root);

    // Three committed commands exceed the owner's single application credit.
    let (mut owner, wake) = open(root, "application");
    assert_read_refused(&mut owner, ServiceState::Recovering);
    let events = poll_until(&mut owner, &wake, |o| o.state() == ServiceState::Ready);
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
    poll_until(&mut owner, &wake, |o| o.state() == ServiceState::Ready);
    assert_granted_balance(&mut owner, &wake, LogIndex(4), 7);
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
    assert_read_refused(&mut owner, ServiceState::Draining);
    assert!(matches!(
        owner.finish_shutdown(),
        Err(ServiceError::NotReady(_))
    ));
    let deadline = Instant::now() + Duration::from_secs(10);
    let completion = loop {
        assert!(Instant::now() < deadline);
        match owner.poll_next().unwrap() {
            ServicePoll::Event(ServiceEvent::Applied(done)) => break done,
            ServicePoll::Event(ServiceEvent::Output(Output::LocalProposalAppended { .. }))
            | ServicePoll::Progress => {}
            ServicePoll::Pending => wait(&wake, deadline),
            other => panic!("unexpected write event: {other:?}"),
        }
    };
    assert_eq!(
        completion.outcomes()[0].local_proposal_id,
        Some(LocalProposalId(42))
    );
    assert_eq!(owner.durable_through(), Some(LogIndex(6)));
    let application = shutdown(&mut owner, &wake);
    assert_read_refused(&mut owner, ServiceState::Stopped);
    assert_eq!(application.store().ledger().view().accounts, [(ACCOUNT, 9)]);
    drop(application);
    drop(owner);
    let (mut owner, wake) = open(root, "application");
    assert!(poll_until(&mut owner, &wake, |o| o.state() == ServiceState::Ready).is_empty());
    owner.try_step(Input::Tick.into()).unwrap();
    poll_until(&mut owner, &wake, |o| o.state() == ServiceState::Ready);
    assert_granted_balance(&mut owner, &wake, LogIndex(6), 9);
    compact(&mut owner);
    assert_eq!(owner.ready_node().unwrap().snapshot_index(), LogIndex(6));
    assert_eq!(owner.durable_through(), Some(LogIndex(6)));
    assert_balance(&mut owner, LogIndex(6), 9);
    assert_eq!(
        shutdown(&mut owner, &wake).store().ledger().view().accounts,
        [(ACCOUNT, 9)]
    );
    drop(owner);

    restore_and_read_snapshot(root);

    // Previously acknowledged bytes survive reopening the original journal too.
    let (mut owner, wake) = open(root, "application");
    assert!(poll_until(&mut owner, &wake, |o| o.state() == ServiceState::Ready).is_empty());
    let application = shutdown(&mut owner, &wake);
    assert_eq!(application.store().applied_index(), LogIndex(6));
    assert_eq!(application.store().ledger().view().accounts, [(ACCOUNT, 9)]);
}

fn restore_and_read_snapshot(root: &Path) {
    // A separate blank application recovers only from the promoted descriptor.
    let (mut owner, wake) = open(root, "restored-application");
    let Some(ServiceEvent::InstallSnapshot {
        snapshot,
        mut application,
    }) = owner.poll().unwrap()
    else {
        panic!("snapshot required");
    };
    assert_eq!(owner.state(), ServiceState::InstallingSnapshot);
    assert_read_refused(&mut owner, ServiceState::InstallingSnapshot);
    application
        .install_snapshot(ApplicationSnapshot {
            applied_index: snapshot.metadata.last_included_index,
            payload: Vec::new(),
            raft_snapshot: Some(snapshot),
        })
        .unwrap();
    owner.resume_snapshot(application).unwrap();
    assert!(poll_until(&mut owner, &wake, |o| o.state() == ServiceState::Ready).is_empty());
    assert_balance(&mut owner, LogIndex(6), 9);
    assert_eq!(
        shutdown(&mut owner, &wake).store().ledger().view().accounts,
        [(ACCOUNT, 9)]
    );
    drop(owner);
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

fn compact(owner: &mut Owner) {
    let retired = owner
        .compact_snapshot_with(|application, node| {
            let at = application.store().applied_index();
            let metadata = RaftSnapshotMetadata::new(
                SnapshotGroupId::new("ledger-owner")?,
                node.id(),
                at,
                node.term_at_index(at).unwrap(),
                node.current_term(),
                ApplicationSnapshotMetadata::new(
                    ApplicationSnapshotKind::new("ledger")?,
                    ApplicationSnapshotVersion::new(1)?,
                ),
            )?;
            let source = snapshot_source::FileSource::build(|file| {
                let (_, snapshot) =
                    application.write_snapshot_to(metadata, file, SnapshotCancellation::new())?;
                Ok(snapshot)
            })?;
            Ok((source.snapshot().clone(), source))
        })
        .unwrap();
    assert!(!retired.is_empty());
    let mut retirement = rafter_runtime::LogRetirementWorker::start(
        rafter_runtime::LogRetirementWorkerOptions::new(64, 256 * 1024),
    )
    .unwrap();
    retirement.try_submit(retired).unwrap();
    retirement.shutdown().unwrap();
}
