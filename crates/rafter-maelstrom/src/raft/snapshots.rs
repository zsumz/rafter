use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    RaftSnapshot, RaftSnapshotMetadata, Role, SnapshotGroupId,
};
use rafter_storage::PersistedRaftSnapshot;

use crate::{
    app::{decode_snapshot_payload, encode_snapshot_payload, persist_app_state},
    raft_node::read_snapshot_payload,
    InitializedNode,
};

const SNAPSHOT_GROUP_ID: &str = "rafter-maelstrom-lin-kv";
const SNAPSHOT_KIND: &str = "lin-kv-v1";

impl InitializedNode {
    pub(super) fn apply_snapshot(&mut self, snapshot: &RaftSnapshot) {
        let payload = match read_snapshot_payload(&self.node, snapshot) {
            Ok(payload) => payload,
            Err(error) => {
                eprintln!("failed to read applied snapshot payload: {error}");
                return;
            }
        };
        let kv = match decode_snapshot_payload(&payload) {
            Ok(kv) => kv,
            Err(error) => {
                eprintln!("failed to decode applied snapshot payload: {error}");
                return;
            }
        };
        self.app.kv = kv;
        self.app.applied = snapshot.metadata.last_included_index;
        self.last_snapshot_index = self
            .last_snapshot_index
            .max(snapshot.metadata.last_included_index);
        if let Err(error) = persist_app_state(&self.root, &self.app) {
            eprintln!("failed to persist app state after snapshot: {error}");
            return;
        }
        eprintln!(
            "rafter-maelstrom applied snapshot node={} index={}",
            self.name, self.app.applied
        );
        self.flush_reads();
    }

    pub(super) fn maybe_compact_snapshot(&mut self) {
        if self.snapshot_every == 0 || self.node.role() != Role::Leader {
            return;
        }
        if self
            .app
            .applied
            .0
            .saturating_sub(self.last_snapshot_index.0)
            < self.snapshot_every
        {
            return;
        }
        match self.compact_snapshot() {
            Ok(index) => eprintln!(
                "rafter-maelstrom compacted snapshot node={} index={}",
                self.name, index
            ),
            Err(error) => eprintln!("failed to compact snapshot: {error}"),
        }
    }

    fn compact_snapshot(&mut self) -> Result<LogIndex, String> {
        let applied = self.app.applied;
        if applied == LogIndex::ZERO {
            return Ok(LogIndex::ZERO);
        }
        let term = self
            .node
            .term_at_index(applied)
            .ok_or_else(|| format!("snapshot boundary term at index {applied} is not retained"))?;
        let metadata = RaftSnapshotMetadata::new(
            SnapshotGroupId::new(SNAPSHOT_GROUP_ID).expect("valid snapshot group id"),
            self.node.id(),
            applied,
            term,
            self.node.current_term(),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new(SNAPSHOT_KIND).expect("valid snapshot kind"),
                ApplicationSnapshotVersion::new(1).expect("valid snapshot version"),
            ),
        )
        .map_err(|error| error.to_string())?;
        self.node
            .compact_log_with_snapshot(PersistedRaftSnapshot {
                metadata,
                application_payload: encode_snapshot_payload(&self.app.kv)?,
            })
            .map_err(|error| error.to_string())?;
        self.last_snapshot_index = applied;
        Ok(applied)
    }
}
