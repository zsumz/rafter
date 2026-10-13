//! Real workers and genuine peer messages, with a partition around voter 1.
use super::super::support::*;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    LocalProposalId, RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkRequest, SnapshotChunkSource,
    SnapshotGroupId,
};
use rafter_storage::{InMemoryRaftSnapshotStore, PersistedRaftSnapshot, RaftSnapshotStore};
use std::time::Instant;

pub(super) struct Partition {
    pub cluster: Cluster,
    pub partitioned: bool,
    pub appended: Vec<(LocalProposalId, LogIndex)>,
    pub installs: usize,
}
impl Partition {
    pub fn new() -> Self {
        Self::with_cluster(Cluster::new(2), true)
    }
    pub fn with_paused_leader() -> Self {
        Self::with_cluster(Cluster::new_with_paused_leader(2, true), false)
    }
    fn with_cluster(cluster: Cluster, partitioned: bool) -> Self {
        Self {
            cluster,
            partitioned,
            appended: Vec::new(),
            installs: 0,
        }
    }
    #[track_caller]
    pub fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done(self) {
            self.turn();
            assert!(Instant::now() < deadline, "partition scenario stalled");
            std::thread::yield_now();
        }
    }
    pub fn tick(&mut self, i: usize) {
        self.send_tick(i);
        self.until(|c| {
            c.cluster.network.is_empty()
                && c.cluster
                    .nodes
                    .iter()
                    .all(|n| n.owner.state() == ServiceState::Ready)
        });
    }
    pub fn send_tick(&mut self, i: usize) {
        self.until(|c| c.cluster.nodes[i].owner.state() == ServiceState::Ready);
        self.cluster.nodes[i].owner.try_step(Input::Tick).unwrap();
    }
    pub fn heal(&mut self) {
        self.cluster
            .network
            .retain(|(from, to, _)| *from != NodeId(1) && *to != NodeId(1));
        self.partitioned = false;
    }
    pub fn elect_replacement(&mut self) {
        for _ in 0..16 {
            self.tick(1);
            if self.cluster.nodes[1].owner.ready_node().is_some_and(|n| {
                n.role() == Role::Leader
                    && n.term_at_index(n.commit_index()) == Some(n.current_term())
            }) {
                return;
            }
        }
        panic!("the connected majority did not elect voter 2");
    }
    pub fn compact(&mut self) {
        let retired = self.cluster.nodes[1]
            .owner
            .compact_snapshot_with(|app, node| {
                let metadata = RaftSnapshotMetadata::new(
                    SnapshotGroupId::new("replacement")?,
                    node.id(),
                    app.floor,
                    node.term_at_index(app.floor).unwrap(),
                    node.current_term(),
                    ApplicationSnapshotMetadata::new(
                        ApplicationSnapshotKind::new("counter")?,
                        ApplicationSnapshotVersion::new(1)?,
                    ),
                )?;
                let store = InMemoryRaftSnapshotStore::with_snapshot(PersistedRaftSnapshot {
                    metadata,
                    application_payload: vec![app.value],
                });
                Ok((store.current_snapshot().unwrap(), store))
            })
            .unwrap();
        drop(retired);
    }
    fn pump(&mut self, i: usize) {
        let owner = &mut self.cluster.nodes[i].owner;
        match owner.poll_next().unwrap() {
            ServicePoll::Event(ServiceEvent::Output(Output::Send { to, message })) => {
                self.cluster
                    .network
                    .push_back((NodeId(i as u64 + 1), to, message));
            }
            ServicePoll::Event(ServiceEvent::Output(Output::LocalProposalAppended {
                proposal_id,
                index,
                ..
            })) if i == 0 => self.appended.push((proposal_id, index)),
            ServicePoll::Event(ServiceEvent::InstallSnapshot {
                snapshot,
                mut application,
            }) => {
                assert_eq!(i, 0);
                assert_eq!(owner.state(), ServiceState::InstallingSnapshot);
                assert_eq!(snapshot.application_payload_len, 1);
                let store = owner.ready_node().unwrap().snapshot_store();
                assert_eq!(store.current_snapshot().as_ref(), Some(&snapshot));
                let bytes = store
                    .snapshot_chunk(SnapshotChunkRequest {
                        transfer_id: snapshot.transfer_id(),
                        metadata: &snapshot.metadata,
                        total_payload_len: snapshot.application_payload_len,
                        application_payload_crc32: snapshot.application_payload_crc32,
                        offset: 0,
                        len: 1,
                    })
                    .unwrap();
                assert_eq!(
                    snapshot,
                    RaftSnapshot::from_payload(snapshot.metadata.clone(), &bytes)
                );
                // The in-memory fixture installs the bytes actually persisted
                // by this receiver, rather than assuming the leader's value.
                application.value = bytes[0];
                application.floor = snapshot.metadata.last_included_index;
                owner.resume_snapshot(application).unwrap();
                self.installs += 1;
            }
            ServicePoll::Event(ServiceEvent::ApplicationFailed(failure)) => {
                panic!("application failed: {failure:?}");
            }
            _ => {}
        }
    }
    pub fn turn(&mut self) {
        for i in 0..self.cluster.nodes.len() {
            self.pump(i);
        }
        for _ in 0..self.cluster.network.len() {
            let (from, to, message) = self.cluster.network.pop_front().unwrap();
            if self.partitioned && (from == NodeId(1) || to == NodeId(1)) {
                continue;
            }
            let owner = &mut self.cluster.nodes[usize::try_from(to.0).unwrap() - 1].owner;
            if owner.state() == ServiceState::Ready {
                owner.try_step(Input::Message { from, message }).unwrap();
                break;
            }
            self.cluster.network.push_back((from, to, message));
        }
    }
}
