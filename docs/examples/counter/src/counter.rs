//! The application's counter and the last command included in its saved value.
use crate::disk;
use rafter::{LogIndex, SnapshotChunkRequest, SnapshotChunkSource};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyResult, ReadBarrier,
    ReplicatedStateMachine, SnapshotSupport,
};
use rafter_storage::FileRaftSnapshotStore;
use serde::{Deserialize, Serialize};
use std::{
    io,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct State {
    pub applied: u64,
    pub value: u64,
}

#[derive(Debug)]
pub struct Counter {
    state: State,
    file: PathBuf,
    snapshots: PathBuf,
}

impl Counter {
    pub fn open(directory: &Path) -> io::Result<Self> {
        let file = directory.join("counter");
        Ok(Self {
            state: disk::load(&file)?,
            file,
            snapshots: directory.join("raft/snapshots"),
        })
    }

    pub fn value(&self) -> u64 {
        self.state.value
    }
}

impl ReplicatedStateMachine for Counter {
    type Command = u64;
    type CommandResult = u64;
    type Query = ();
    type QueryResult = u64;
    type Error = io::Error;
    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;

    fn applied_index(&self) -> io::Result<LogIndex> {
        Ok(LogIndex(self.state.applied))
    }

    fn encode_command(&self, amount: &u64) -> io::Result<Vec<u8>> {
        Ok(amount.to_be_bytes().to_vec())
    }

    fn decode_command(&self, bytes: &[u8]) -> io::Result<u64> {
        Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
            disk::invalid("an increment must be eight bytes")
        })?))
    }

    fn apply_batch(&mut self, batch: ApplyBatch<u64>) -> io::Result<Vec<ApplyResult<u64>>> {
        let mut next = self.state.clone();
        let mut results = Vec::new();
        for entry in batch.entries {
            if entry.index.0 <= next.applied {
                return Err(disk::invalid("command was already applied"));
            }
            // This counter stops at u64::MAX rather than wrapping back to zero.
            next.value = next.value.saturating_add(entry.command);
            next.applied = entry.index.0;
            results.push(ApplyResult {
                index: entry.index,
                term: entry.term,
                result: next.value,
                local_proposal_id: entry.local_proposal_id,
            });
        }
        if !results.is_empty() {
            // Publish data and progress together, before acknowledging the write.
            disk::save(&self.file, &next)?;
            self.state = next;
        }
        Ok(results)
    }

    fn read(&self, _: (), barrier: ReadBarrier) -> io::Result<u64> {
        if self.state.applied < barrier.required_applied_index.0 {
            return Err(disk::invalid("counter has not caught up yet"));
        }
        Ok(self.state.value)
    }

    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<io::Error>> {
        if at.0 != self.state.applied {
            return Err(disk::invalid("snapshot must match saved progress").into());
        }
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: serde_json::to_vec(&self.state).map_err(io::Error::from)?,
            raft_snapshot: None,
        })
    }

    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<io::Error>> {
        if snapshot.applied_index.0 < self.state.applied {
            return Err(disk::invalid("snapshot would move the counter backwards").into());
        }
        let payload = if let Some(descriptor) = &snapshot.raft_snapshot {
            if descriptor.application_payload_len > 65_536 {
                return Err(disk::invalid("counter snapshot is too large").into());
            }
            let store = FileRaftSnapshotStore::open(&self.snapshots)
                .map_err(|error| disk::invalid(&error.to_string()))?;
            store
                .snapshot_chunk(SnapshotChunkRequest {
                    transfer_id: descriptor.transfer_id(),
                    metadata: &descriptor.metadata,
                    total_payload_len: descriptor.application_payload_len,
                    application_payload_crc32: descriptor.application_payload_crc32,
                    offset: 0,
                    len: descriptor.application_payload_len as u32,
                })
                .ok_or_else(|| disk::invalid("snapshot payload is unavailable"))?
        } else {
            snapshot.payload
        };
        let next: State = serde_json::from_slice(&payload).map_err(io::Error::from)?;
        if next.applied != snapshot.applied_index.0 {
            return Err(disk::invalid("snapshot progress does not match its data").into());
        }
        if next.applied == self.state.applied && next.value != self.state.value {
            return Err(disk::invalid("snapshot conflicts with already saved data").into());
        }
        disk::save(&self.file, &next)?;
        self.state = next;
        Ok(())
    }
}
