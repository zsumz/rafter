//! Constant-memory source with observed requests and deterministic faults.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, SnapshotChunkRequest, SnapshotChunkSource,
    SnapshotGroupId, Term,
};
use rafter_runtime::snapshot_io::SnapshotCancellation;
use std::cell::Cell;

#[derive(Clone, Copy)]
pub enum Fault {
    None,
    Missing,
    Short,
    Long,
    Overallocated,
    Corrupt,
    Panic,
}

pub struct Source {
    pub descriptor: RaftSnapshot,
    pub calls: Cell<usize>,
    pub largest: Cell<u32>,
    pub fault: Fault,
    pub cancel: Option<SnapshotCancellation>,
}

impl Source {
    pub fn small() -> Self {
        Self::new(9, rafter_crc32::crc32(&[0x5a; 9]))
    }
    pub fn new(len: u64, checksum: u32) -> Self {
        let metadata = RaftSnapshotMetadata::new(
            SnapshotGroupId::new("reader-tests").unwrap(),
            NodeId(1),
            LogIndex(42),
            Term(3),
            Term(3),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("opaque").unwrap(),
                ApplicationSnapshotVersion::new(1).unwrap(),
            ),
        )
        .unwrap();
        Self {
            descriptor: RaftSnapshot::new(metadata, len, checksum),
            calls: Cell::new(0),
            largest: Cell::new(0),
            fault: Fault::None,
            cancel: None,
        }
    }
}

impl SnapshotChunkSource for Source {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        assert_eq!(request.metadata, &self.descriptor.metadata);
        assert_eq!(request.transfer_id, self.descriptor.transfer_id());
        assert_eq!(
            request.total_payload_len,
            self.descriptor.application_payload_len
        );
        assert_eq!(
            request.application_payload_crc32,
            self.descriptor.application_payload_crc32
        );
        assert!(request.offset + u64::from(request.len) <= request.total_payload_len);
        self.calls.set(self.calls.get() + 1);
        self.largest.set(self.largest.get().max(request.len));
        if let Some(cancel) = &self.cancel {
            cancel.cancel();
        }
        match self.fault {
            Fault::Missing => None,
            Fault::Short => Some(vec![0x5a; request.len as usize - 1]),
            Fault::Long => Some(vec![0x5a; request.len as usize + 1]),
            Fault::Overallocated => {
                let mut bytes = Vec::with_capacity(request.len as usize + 32);
                bytes.resize(request.len as usize, 0x5a);
                Some(bytes)
            }
            Fault::Corrupt => Some(vec![0x5b; request.len as usize]),
            Fault::Panic => panic!("injected source panic"),
            Fault::None => Some(vec![0x5a; request.len as usize]),
        }
    }
}
