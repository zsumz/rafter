//! Streamed owner maintenance and deterministic nonempty genesis provisioning.
use super::{application::Application, config, node::Replica};
use crate::{
    snapshot_source::FileSource,
    support::{execute, open_session},
};
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, Input,
    LogIndex, RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId,
};
use rafter_app::state_machine::{ApplicationSnapshot, ReplicatedStateMachine};
use rafter_reference_ledger::{store::LedgerStore, AccountId, Ledger, Mutation};
use rafter_runtime::{
    service::{ServiceReadRejection, ServiceState, ServiceStepError},
    snapshot_io::SnapshotCancellation,
    LogRetirementWorker, LogRetirementWorkerOptions,
};
use std::path::Path;

/// Provision identical nonempty application genesis before any Raft owner is
/// opened. These accounts are fixture input, not claimed replicated history.
/// Their valid unchanged v1 encoding exceeds the kernel's 64 KiB chunk size.
pub fn seed_genesis(root: &Path) {
    let mut ledger = Ledger::new(config());
    ledger.apply(open_session(0, 1));
    for id in 0..4097 {
        ledger.apply(execute(
            0,
            1,
            id + 1,
            Mutation::OpenAccount {
                account_id: AccountId::new(id),
            },
        ));
    }
    assert_eq!(ledger.view().accounts.len(), 4097);
    for id in 1..=3 {
        let directory = root.join(format!("node-{id}/application"));
        assert!(!directory.exists());
        std::fs::create_dir_all(&directory).unwrap();
        LedgerStore::open(&directory, config())
            .unwrap()
            .replace(&ledger, LogIndex::ZERO)
            .unwrap();
    }
}
impl Replica {
    pub(super) fn observe_staged(&self, chunk: &rafter::StagedSnapshotChunk) {
        // The runtime already persisted this effect. It is an observation,
        // not a request for the embedding to write the same chunk again.
        assert!(chunk.bytes.len() <= 64 * 1024);
        let node = self.owner.ready_node().unwrap();
        assert!(
            node.snapshot_index() >= chunk.metadata.last_included_index
                || node
                    .snapshot_transfer_status()
                    .follower
                    .is_some_and(|transfer| transfer.received_bytes
                        >= chunk.offset + u64::try_from(chunk.bytes.len()).unwrap())
        );
    }

    pub fn compact(&mut self) -> RaftSnapshot {
        let retired = self
            .owner
            .compact_snapshot_with(|application, node| {
                let at = application.ledger.store().applied_index();
                let metadata = RaftSnapshotMetadata::new(
                    SnapshotGroupId::new("ledger-owner-tls")?,
                    node.id(),
                    at,
                    node.term_at_index(at).unwrap(),
                    node.current_term(),
                    ApplicationSnapshotMetadata::new(
                        ApplicationSnapshotKind::new("ledger")?,
                        ApplicationSnapshotVersion::new(1)?,
                    ),
                )?;
                let source = FileSource::build(|file| {
                    let (_, snapshot) = application.ledger.write_snapshot_to(
                        metadata,
                        file,
                        SnapshotCancellation::new(),
                    )?;
                    Ok(snapshot)
                })?;
                Ok((source.snapshot().clone(), source))
            })
            .unwrap();
        assert!(!retired.is_empty());
        let mut retirement =
            LogRetirementWorker::start(LogRetirementWorkerOptions::new(64, 256 * 1024)).unwrap();
        retirement.try_submit(retired).unwrap();
        retirement.shutdown().unwrap();
        self.owner.ready_node().unwrap().snapshot().unwrap().clone()
    }
    pub(super) fn install(&mut self, snapshot: RaftSnapshot, mut application: Application) {
        assert_eq!(self.owner.state(), ServiceState::InstallingSnapshot);
        assert_eq!(self.owner.durable_through(), None);
        assert!(matches!(
            self.owner.try_step(Input::Tick.into()),
            Err(ServiceStepError::Unavailable {
                state: ServiceState::InstallingSnapshot,
                ..
            })
        ));
        assert_eq!(
            self.owner
                .try_read_application(LogIndex::ZERO, |_| panic!("query during snapshot handoff"))
                .unwrap_err()
                .rejection(),
            ServiceReadRejection::NotReady(ServiceState::InstallingSnapshot)
        );
        let at = snapshot.metadata.last_included_index;
        application
            .ledger
            .install_snapshot(ApplicationSnapshot {
                applied_index: at,
                payload: Vec::new(),
                raft_snapshot: Some(snapshot),
            })
            .unwrap();
        assert_eq!(application.ledger.store().applied_index(), at);
        self.owner.resume_snapshot(application).unwrap();
        self.snapshot_installs += 1;
    }
}
