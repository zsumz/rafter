//! Independent byte fixtures and observed sources for streamed schema admission.

use std::cell::Cell;

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, NodeId,
    RaftSnapshotMetadata, SnapshotChunkRequest, SnapshotGroupId, Term,
};

use super::super::{ApplicationSnapshot, LogIndex, RaftSnapshot, SnapshotChunkSource};

pub(super) const FLOOR: LogIndex = LogIndex(42);
pub(super) const ACCOUNTS: u32 = 131_072;
pub(super) const LENGTH: u64 = 2_097_198;

pub(super) fn descriptor(bytes: &[u8]) -> RaftSnapshot {
    RaftSnapshot::from_payload(metadata(), bytes)
}

fn metadata() -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("ledger").unwrap(),
        NodeId(2),
        FLOOR,
        Term(1),
        Term(1),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("ledger").unwrap(),
            ApplicationSnapshotVersion::new(1).unwrap(),
        ),
    )
    .unwrap()
}

pub(super) fn promoted(snapshot: RaftSnapshot) -> ApplicationSnapshot {
    ApplicationSnapshot {
        applied_index: FLOOR,
        payload: Vec::new(),
        raft_snapshot: Some(snapshot),
    }
}

pub(super) fn payload(accounts: &[(u64, u64)], sessions: &[(u32, u64)]) -> Vec<u8> {
    let mut bytes = vec![1];
    bytes.extend_from_slice(&FLOOR.0.to_be_bytes());
    bytes.extend_from_slice(
        &accounts
            .iter()
            .map(|(_, balance)| u128::from(*balance))
            .sum::<u128>()
            .to_be_bytes(),
    );
    bytes.extend_from_slice(&u32::try_from(accounts.len()).unwrap().to_be_bytes());
    for (id, balance) in accounts {
        bytes.extend_from_slice(&id.to_be_bytes());
        bytes.extend_from_slice(&balance.to_be_bytes());
    }
    bytes.extend_from_slice(&u32::try_from(sessions.len()).unwrap().to_be_bytes());
    for (id, epoch) in sessions {
        bytes.extend_from_slice(&id.to_be_bytes());
        bytes.extend_from_slice(&epoch.to_be_bytes());
        bytes.push(0);
    }
    bytes
}

pub(super) struct Source {
    pub(super) bytes: Vec<u8>,
    pub(super) calls: Cell<usize>,
    pub(super) extra_capacity: bool,
    pub(super) short: bool,
}

impl Source {
    pub(super) fn new(bytes: Vec<u8>) -> Self {
        Self {
            bytes,
            calls: Cell::new(0),
            extra_capacity: false,
            short: false,
        }
    }
}

impl SnapshotChunkSource for Source {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        self.calls.set(self.calls.get() + 1);
        let start = usize::try_from(request.offset).ok()?;
        let end = start.checked_add(request.len as usize)?;
        let mut bytes = if self.extra_capacity {
            Vec::with_capacity(32 * 1024)
        } else {
            Vec::new()
        };
        bytes.extend_from_slice(self.bytes.get(start..end)?);
        if self.short {
            bytes.pop();
        }
        Some(bytes)
    }
}

pub(super) struct Generated {
    pub(super) calls: Cell<usize>,
    pub(super) largest: Cell<u32>,
    pub(super) snapshot: RaftSnapshot,
}

impl Generated {
    pub(super) fn new() -> Self {
        Self {
            calls: Cell::new(0),
            largest: Cell::new(0),
            snapshot: RaftSnapshot {
                metadata: metadata(),
                application_payload_len: LENGTH,
                // Independently calculated with Python zlib over the schema below.
                application_payload_crc32: 0x7126_3406,
            },
        }
    }
}

impl SnapshotChunkSource for Generated {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        assert_eq!(request.metadata, &self.snapshot.metadata);
        assert_eq!(request.transfer_id, self.snapshot.transfer_id());
        assert_eq!(request.total_payload_len, LENGTH);
        assert_eq!(
            request.application_payload_crc32,
            self.snapshot.application_payload_crc32
        );
        assert!(request.len <= 16 * 1024);
        assert!(request.offset + u64::from(request.len) <= LENGTH);
        self.calls.set(self.calls.get() + 1);
        self.largest.set(self.largest.get().max(request.len));
        Some(
            (request.offset..request.offset + u64::from(request.len))
                .map(byte_at)
                .collect(),
        )
    }
}

fn byte_at(offset: u64) -> u8 {
    let accounts_end = 29 + 16 * u64::from(ACCOUNTS);
    match offset {
        0 => 1,
        1..=8 => FLOOR.0.to_be_bytes()[usize::try_from(offset).unwrap() - 1],
        9..=24 => u128::from(ACCOUNTS).to_be_bytes()[usize::try_from(offset).unwrap() - 9],
        25..=28 => ACCOUNTS.to_be_bytes()[usize::try_from(offset).unwrap() - 25],
        value if value < accounts_end => {
            let id = (value - 29) / 16;
            let position = (value - 29) % 16;
            if position < 8 {
                id.to_be_bytes()[position as usize]
            } else {
                1_u64.to_be_bytes()[position as usize - 8]
            }
        }
        value => {
            // One session: client 0, epoch 1, no cached completion.
            [0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0]
                [usize::try_from(value - accounts_end).unwrap()]
        }
    }
}
