//! Managed KV over a real in-memory Rafter service driver.
//!
//! This example builds three `rafter-app::RaftGroup`s, lets the managed
//! in-memory driver elect node 1, then uses the cloneable `RaftHandle` API.
//! The transport boundary is in-process for the example, but writes and reads
//! are driven through real Rafter groups.
//! It is not a production storage, authentication, transport, or app-state
//! durability template.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter-service --example replicated_kv_service
//! ```

use std::{
    collections::BTreeMap,
    future::Future,
    task::{Context, Poll, Waker},
};

use rafter::{LogIndex, NodeConfig, NodeId};
use rafter_app::{
    group::RaftGroup,
    state_machine::{
        ApplicationSnapshot, ApplyBatch, ApplyResult, ReadBarrier, ReplicatedStateMachine,
    },
};
use rafter_runtime::DurableRaftNode;
use rafter_service::{InMemoryRaftDriver, ReadConsistency};
use rafter_storage::InMemoryRaftHardStateStore;

type KvCommand = (String, String);
type KvQuery = String;
type KvGroup = RaftGroup<(), KvStateMachine, DurableRaftNode>;

fn main() {
    let driver = InMemoryRaftDriver::new_elected(
        NodeId(1),
        vec![
            group(1, &[2, 3], 3),
            group(2, &[1, 3], 9),
            group(3, &[1, 2], 9),
        ],
    )
    .expect("managed in-memory cluster elects node 1");
    let raft = driver.handle();

    let write = block_on(raft.write(("alpha".to_owned(), "one".to_owned())))
        .expect("managed write commits and applies");
    assert_eq!(write.result, None);
    println!(
        "write applied at {:?}/{:?}: {:?}",
        write.index, write.term, write.result
    );

    let read = block_on(raft.read("alpha".to_owned(), ReadConsistency::Linearizable))
        .expect("managed read succeeds");
    assert_eq!(read.result, Some("one".to_owned()));
    assert!(read.proof.is_some());
    println!("linearizable read returned alpha={:?}", read.result);

    let metrics = raft.metrics().expect("metrics watch opens").current();
    assert_eq!(metrics.applied_index, write.index);
    println!(
        "metrics: role={:?} term={:?} applied={:?}",
        metrics.role, metrics.term, metrics.applied_index
    );

    block_on(raft.shutdown()).expect("shutdown succeeds");
}

fn group(id: u64, peers: &[u64], election_timeout_ticks: u64) -> KvGroup {
    let config = NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("static Raft config is valid");
    let raft = DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
        .expect("in-memory durable node opens");
    RaftGroup::new((), NodeId(id), raft, KvStateMachine::default())
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct KvStateMachine {
    applied_index: LogIndex,
    values: BTreeMap<String, String>,
}

impl ReplicatedStateMachine for KvStateMachine {
    type Command = KvCommand;
    type CommandResult = Option<String>;
    type Query = KvQuery;
    type QueryResult = Option<String>;
    type Error = String;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        Ok(format!("{}\n{}", command.0, command.1).into_bytes())
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        let text = std::str::from_utf8(payload).map_err(|error| error.to_string())?;
        let (key, value) = text
            .split_once('\n')
            .ok_or_else(|| "malformed command payload".to_owned())?;
        Ok((key.to_owned(), value.to_owned()))
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        let mut results = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            let (key, value) = entry.command;
            let result = self.values.insert(key, value);
            self.applied_index = entry.index;
            results.push(ApplyResult {
                index: entry.index,
                term: entry.term,
                result,
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
        Ok(self.values.get(&query).cloned())
    }

    fn build_snapshot(&mut self, at: LogIndex) -> Result<ApplicationSnapshot, Self::Error> {
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: Vec::new(),
            raft_snapshot: None,
        })
    }

    fn install_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<(), Self::Error> {
        self.applied_index = snapshot.applied_index;
        Ok(())
    }
}

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
