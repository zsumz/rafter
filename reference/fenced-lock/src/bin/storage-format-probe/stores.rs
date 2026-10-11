use std::{error::Error, path::Path};

use rafter::{LogIndex, NodeId, SnapshotChunkRequest, SnapshotChunkSource, Term};
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
    RaftHardStateStore, RaftLogSegment, RaftSnapshotStore,
};

use crate::fixture::{chunk, descriptor, entries, hard_state, snapshot, PAYLOAD, PREFIX};

pub(super) struct Stores {
    hard: FileRaftHardStateStore,
    log: FileRaftLogSegment,
    snapshots: FileRaftSnapshotStore,
}

impl Stores {
    pub(super) fn open(directory: &Path) -> Result<Self, Box<dyn Error>> {
        let (hard, log, snapshots) = FileRaftNodeStores::open(directory)?.into_parts();
        Ok(Self {
            hard,
            log,
            snapshots,
        })
    }

    pub(super) fn write(&mut self, case: &str) -> Result<(), Box<dyn Error>> {
        assert_eq!(self.hard.current().current_term, Term(0));
        self.log.append_entries(&entries(1, 4))?;
        self.hard.write_hard_state(hard_state(3))?;
        if case != "log" {
            self.snapshots.write_snapshot(snapshot(2))?;
            self.log.compact_prefix_through(LogIndex(2))?;
        }
        if case == "pending" {
            self.snapshots.stage_snapshot_chunk(&chunk(0, PREFIX))?;
        }
        Ok(())
    }

    pub(super) fn check(&self, case: &str) {
        assert_eq!(self.hard.current(), hard_state(3));
        let boundary = if case == "log" { 0 } else { 2 };
        assert_eq!(self.log.compacted_through(), LogIndex(boundary));
        assert_eq!(self.log.next_index(), LogIndex(5));
        assert_eq!(self.log.replay_entries(), entries(boundary + 1, 4));
        if case == "log" {
            assert!(self.snapshots.current_snapshot().is_none());
        } else {
            self.check_snapshot(2);
        }
        if case == "pending" {
            let pending = self.snapshots.current_pending_snapshot_transfer().unwrap();
            let expected = descriptor(5);
            assert_eq!(pending.leader_id, NodeId(2));
            assert_eq!(pending.transfer_id, expected.transfer_id());
            assert_eq!(pending.metadata, expected.metadata);
            assert_eq!(pending.received_len, PREFIX as u64);
            assert_eq!(pending.total_payload_len, PAYLOAD.len() as u64);
            assert_eq!(
                pending.application_payload_crc32,
                expected.application_payload_crc32
            );
        } else {
            assert!(self.snapshots.current_pending_snapshot_transfer().is_none());
        }
    }

    pub(super) fn extend(&mut self, case: &str) -> Result<(), Box<dyn Error>> {
        self.log.append_entries(&entries(5, 5))?;
        if case == "pending" {
            self.snapshots
                .stage_snapshot_chunk(&chunk(PREFIX, PAYLOAD.len()))?;
            self.snapshots.promote_staged_snapshot(&descriptor(5))?;
        } else if case == "snapshot" {
            self.snapshots.write_snapshot(snapshot(5))?;
        }
        self.hard.write_hard_state(hard_state(5))?;
        if case != "log" {
            self.log.compact_prefix_through(LogIndex(5))?;
        }
        Ok(())
    }

    pub(super) fn check_extended(&self, case: &str) {
        assert_eq!(self.hard.current(), hard_state(5));
        assert_eq!(self.log.next_index(), LogIndex(6));
        if case == "log" {
            assert_eq!(self.log.replay_entries(), entries(1, 5));
            assert_eq!(self.log.compacted_through(), LogIndex::ZERO);
            assert!(self.snapshots.current_snapshot().is_none());
        } else {
            assert_eq!(self.log.compacted_through(), LogIndex(5));
            assert!(self.log.replay_entries().is_empty());
            self.check_snapshot(5);
        }
        assert!(self.snapshots.current_pending_snapshot_transfer().is_none());
    }

    fn check_snapshot(&self, index: u64) {
        let expected = descriptor(index);
        assert_eq!(self.snapshots.current_snapshot(), Some(expected.clone()));
        let mut bytes = Vec::new();
        let mut offset = 0;
        while offset < expected.application_payload_len {
            let len = u32::try_from((expected.application_payload_len - offset).min(13)).unwrap();
            bytes.extend(
                self.snapshots
                    .snapshot_chunk(SnapshotChunkRequest {
                        transfer_id: expected.transfer_id(),
                        metadata: &expected.metadata,
                        total_payload_len: expected.application_payload_len,
                        application_payload_crc32: expected.application_payload_crc32,
                        offset,
                        len,
                    })
                    .expect("snapshot chunk survives recovery"),
            );
            offset += u64::from(len);
        }
        assert_eq!(bytes, PAYLOAD);
    }
}
