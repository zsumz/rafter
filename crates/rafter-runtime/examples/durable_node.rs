//! A single-voter durable node: propose, restart, recover.
//!
//! `DurableRaftNode` wraps the kernel and persists hard state and log entries
//! through the `rafter-storage` traits before any output is visible. This
//! example uses the file-backed stores and proves state survives a restart.
//! It is single-voter and local-only: it is not a cluster transport,
//! authentication, or application-state durability template.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter-runtime --example durable_node
//! ```

use rafter::{Input, NodeConfig, NodeId, Output};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{FileRaftHardStateStore, FileRaftLogSegment};

fn main() {
    let dir = std::env::temp_dir().join(format!("rafter-durable-example-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create example dir");
    let hard_state_path = dir.join("hard-state");
    let log_path = dir.join("log");

    let config = || NodeConfig::new(NodeId(1), vec![], 3).expect("single-voter config is valid");

    // First process lifetime: elect self (single voter), commit a payload.
    {
        let hard_state = FileRaftHardStateStore::open(&hard_state_path).expect("open hard state");
        let log = FileRaftLogSegment::open(&log_path).expect("open log");
        let mut node =
            DurableRaftNode::with_storage(config(), hard_state, log).expect("hydrate durable node");

        for _ in 0..3 {
            node.step(Input::Tick).expect("tick persists");
        }
        let outputs = node
            .step(Input::ClientProposal {
                payload: b"durable fact".to_vec(),
            })
            .expect("proposal persists");
        assert!(outputs
            .iter()
            .any(|output| matches!(output, Output::Apply { .. })));
        println!(
            "committed at term {:?}, last log index {:?}",
            node.current_term(),
            node.last_log_index()
        );
    }

    // Second process lifetime: reopen the same files, state is recovered.
    {
        let hard_state = FileRaftHardStateStore::open(&hard_state_path).expect("reopen hard state");
        let log = FileRaftLogSegment::open(&log_path).expect("reopen log");
        let node = DurableRaftNode::with_storage(config(), hard_state, log)
            .expect("rehydrate durable node");
        println!(
            "recovered term {:?}, last log index {:?}",
            node.current_term(),
            node.last_log_index()
        );
        assert_eq!(node.last_log_index(), rafter::LogIndex(1));
    }

    std::fs::remove_dir_all(&dir).ok();
}
