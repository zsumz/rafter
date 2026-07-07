//! Application snapshot install paths for `rafter-app`.
//!
//! `ApplicationSnapshot::payload` carries inline bytes when the state machine
//! builds and installs its own snapshot directly. During a Raft-driven install,
//! `RaftGroup` passes an empty inline payload plus `raft_snapshot: Some(...)`;
//! the application resolves the already-promoted payload from its own snapshot
//! store by `raft_snapshot.transfer_id()`.
//! This example focuses on the app-layer callback contract; it does not
//! implement production snapshot storage, transfer authentication, or durable
//! app-state persistence.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter-app --example snapshot_install
//! ```

use std::collections::BTreeMap;

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    InMemorySnapshotChunkSource, LogIndex, NodeId, RaftSnapshot, RaftSnapshotMetadata,
    SnapshotGroupId, Term,
};
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplyBatch, ApplyResult, ReadBarrier, ReplicatedStateMachine,
};

fn main() {
    let mut leader = KvStateMachine::default();
    leader.insert_for_example("alpha", "one");
    leader.insert_for_example("beta", "two");
    leader.applied_index = LogIndex(12);

    let inline_snapshot = leader
        .build_snapshot(LogIndex(12))
        .expect("leader builds snapshot payload");
    assert!(!inline_snapshot.payload.is_empty());

    let mut inline_follower = KvStateMachine::default();
    inline_follower
        .install_snapshot(inline_snapshot.clone())
        .expect("inline payload installs");
    assert_eq!(inline_follower.get("alpha"), Some("one"));
    assert_eq!(inline_follower.applied_index(), Ok(LogIndex(12)));

    let raft_snapshot = raft_snapshot_for_payload(LogIndex(12), &inline_snapshot.payload);
    let mut promoted_payloads = InMemorySnapshotChunkSource::new();
    promoted_payloads
        .insert(&raft_snapshot, inline_snapshot.payload.clone())
        .expect("payload length matches snapshot descriptor");

    let mut raft_follower = KvStateMachine::with_promoted_payloads(promoted_payloads);
    raft_follower
        .install_snapshot(ApplicationSnapshot {
            applied_index: LogIndex(12),
            payload: Vec::new(),
            raft_snapshot: Some(raft_snapshot),
        })
        .expect("descriptor-based payload installs");
    assert_eq!(raft_follower.get("beta"), Some("two"));
    assert_eq!(raft_follower.applied_index(), Ok(LogIndex(12)));
    assert_eq!(
        raft_follower
            .read(
                KvQuery::Get {
                    key: "alpha".to_owned(),
                },
                ReadBarrier {
                    required_applied_index: LogIndex(12),
                    local_applied_index: LogIndex(12),
                },
            )
            .expect("restored state serves reads"),
        Some("one".to_owned())
    );

    println!("snapshot payload restored application state through both install paths");
}

fn raft_snapshot_for_payload(index: LogIndex, payload: &[u8]) -> RaftSnapshot {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("snapshot-install-example").expect("valid snapshot group id"),
        NodeId(1),
        index,
        Term(3),
        Term(3),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("kv-snapshot-v1").expect("valid snapshot kind"),
            ApplicationSnapshotVersion::new(1).expect("valid snapshot version"),
        ),
    )
    .expect("snapshot metadata is valid");

    RaftSnapshot::from_payload(metadata, payload)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvCommand {
    Put { key: String, value: String },
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum KvQuery {
    Get { key: String },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct KvStateMachine {
    applied_index: LogIndex,
    values: BTreeMap<String, String>,
    promoted_payloads: InMemorySnapshotChunkSource,
}

impl KvStateMachine {
    fn with_promoted_payloads(promoted_payloads: InMemorySnapshotChunkSource) -> Self {
        Self {
            promoted_payloads,
            ..Self::default()
        }
    }

    fn insert_for_example(&mut self, key: &str, value: &str) {
        self.values.insert(key.to_owned(), value.to_owned());
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    fn encode_snapshot(&self, at: LogIndex) -> Result<Vec<u8>, String> {
        let mut text = format!("index {}\n", at.0);
        for (key, value) in &self.values {
            if key.contains(['\n', '=']) || value.contains('\n') {
                return Err("example snapshot keys and values must be line-safe".to_owned());
            }
            text.push_str(key);
            text.push('=');
            text.push_str(value);
            text.push('\n');
        }
        Ok(text.into_bytes())
    }

    fn decode_snapshot(payload: &[u8]) -> Result<(LogIndex, BTreeMap<String, String>), String> {
        let text = String::from_utf8(payload.to_vec()).map_err(|error| error.to_string())?;
        let mut lines = text.lines();
        let header = lines
            .next()
            .ok_or_else(|| "snapshot payload is missing index header".to_owned())?;
        let index = header
            .strip_prefix("index ")
            .ok_or_else(|| "snapshot payload has invalid index header".to_owned())?
            .parse::<u64>()
            .map(LogIndex)
            .map_err(|error| error.to_string())?;

        let mut values = BTreeMap::new();
        for line in lines {
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| "snapshot payload has invalid key/value row".to_owned())?;
            values.insert(key.to_owned(), value.to_owned());
        }

        Ok((index, values))
    }
}

impl ReplicatedStateMachine for KvStateMachine {
    type Command = KvCommand;
    type CommandResult = ();
    type Query = KvQuery;
    type QueryResult = Option<String>;
    type Error = String;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        match command {
            KvCommand::Put { key, value } => Ok(format!("put\n{key}\n{value}").into_bytes()),
        }
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        let text = String::from_utf8(payload.to_vec()).map_err(|error| error.to_string())?;
        let mut lines = text.lines();
        if lines.next() != Some("put") {
            return Err("invalid command kind".to_owned());
        }
        let key = lines
            .next()
            .ok_or_else(|| "missing command key".to_owned())?
            .to_owned();
        let value = lines
            .next()
            .ok_or_else(|| "missing command value".to_owned())?
            .to_owned();
        Ok(KvCommand::Put { key, value })
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        let mut results = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            match entry.command {
                KvCommand::Put { key, value } => {
                    self.values.insert(key, value);
                }
            }
            self.applied_index = entry.index;
            results.push(ApplyResult {
                index: entry.index,
                term: entry.term,
                result: (),
                local_proposal_id: entry.local_proposal_id,
            });
        }
        Ok(results)
    }

    fn read(
        &self,
        query: Self::Query,
        barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        if self.applied_index < barrier.required_applied_index {
            return Err("read barrier has not been reached".to_owned());
        }
        match query {
            KvQuery::Get { key } => Ok(self.values.get(&key).cloned()),
        }
    }

    fn build_snapshot(&mut self, at: LogIndex) -> Result<ApplicationSnapshot, Self::Error> {
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: self.encode_snapshot(at)?,
            raft_snapshot: None,
        })
    }

    fn install_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<(), Self::Error> {
        let payload = if snapshot.payload.is_empty() {
            let raft_snapshot = snapshot.raft_snapshot.as_ref().ok_or_else(|| {
                "empty inline payload requires a Raft snapshot descriptor".to_owned()
            })?;
            self.promoted_payloads
                .payload(raft_snapshot.transfer_id())
                .ok_or_else(|| "promoted snapshot payload is missing".to_owned())?
                .to_vec()
        } else {
            snapshot.payload
        };

        let (payload_index, values) = Self::decode_snapshot(&payload)?;
        if payload_index != snapshot.applied_index {
            return Err("snapshot payload index does not match installed index".to_owned());
        }

        self.applied_index = snapshot.applied_index;
        self.values = values;
        Ok(())
    }
}
