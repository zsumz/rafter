//! Real service snapshots and deliberately fallible source disposal.
pub(super) use crate::group_support::*;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    RaftSnapshotMetadata, SnapshotGroupId,
};
pub(super) use rafter::{RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource};
pub(super) use rafter_runtime::{
    service::{ServiceAdapterError, ServiceError},
    DurableRaftNode,
};
pub(super) use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftSnapshot, RaftSnapshotStore,
};
pub type Node =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>;
pub struct Source {
    pub store: InMemoryRaftSnapshotStore,
    pub panic_drop: bool,
}
impl SnapshotChunkSource for Source {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        self.store.snapshot_chunk(request)
    }
}
impl Drop for Source {
    fn drop(&mut self) {
        assert!(!self.panic_drop, "injected snapshot source drop panic");
    }
}
pub fn snapshot(app: &mut App, node: &Node) -> Result<(RaftSnapshot, Source), ServiceAdapterError> {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("managed-compaction")?,
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
    Ok((
        store.current_snapshot().unwrap(),
        Source {
            store,
            panic_drop: false,
        },
    ))
}
pub fn ready() -> Replica {
    let mut node = Replica::single(false);
    let mut write = node.writes.write(vec![7], WriteOptions::default());
    node.until(|_| matches!(poll(&mut write), Poll::Ready(Ok(_))));
    node.drain();
    assert_eq!(node.driver.durable_through(), Some(LogIndex(2)));
    node
}
pub fn ready_owner() -> crate::reads_support::Replica {
    let mut node = crate::reads_support::Replica::single(4, false);
    let mut write = node.owner.sender().write(vec![7], WriteOptions::default());
    crate::write_support::until(&mut node.owner, |_| {
        matches!(poll(&mut write), Poll::Ready(Ok(_)))
    });
    crate::write_support::drain(&mut node.owner);
    node
}
