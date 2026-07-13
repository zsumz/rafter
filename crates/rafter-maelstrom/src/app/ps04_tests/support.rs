use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::{Input, LogIndex, NodeId, Output, Role};
use serde_json::{json, Value};

use super::super::{
    apply_committed_command, canonical_key, load_app_state, AfterAppPersist, AppState,
    ClientMutation, Command, CommandApplyOutcome,
};
use crate::{protocol::Envelope, runtime::open_application_node, MaelstromNode};

static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

pub(super) fn fresh_elected_node(root: &Path) -> crate::runtime::OpenedApplicationNode {
    let mut opened = open_application_node(root, NodeId(1), Vec::new())
        .expect("fresh Maelstrom application and Raft node open");
    assert!(opened.recovery_outputs.is_empty());
    for _ in 0..20 {
        if opened.node.role() == Role::Leader {
            break;
        }
        opened
            .node
            .step(Input::Tick)
            .expect("single-node leader election persists");
    }
    assert_eq!(opened.node.role(), Role::Leader);
    opened
}

pub(super) fn initialize_process(root: &Path) -> MaelstromNode {
    let mut process = MaelstromNode::default();
    process
        .initialize_at_root(&init_envelope(), root.to_path_buf())
        .expect("production Maelstrom initialization recovers application state");
    process
}

fn init_envelope() -> Envelope {
    Envelope {
        src: "controller".to_owned(),
        dest: "n1".to_owned(),
        body: json!({
            "type": "init",
            "msg_id": 1,
            "node_id": "n1",
            "node_ids": ["n1"],
        }),
    }
}

pub(super) fn persist_with_interruption(
    root: &Path,
    app: &mut AppState,
    committed: &(LogIndex, Vec<u8>),
) {
    let command: Command = serde_json::from_slice(&committed.1).expect("committed command decodes");
    let outcome = apply_committed_command(root, app, committed.0, &command, |_| {
        AfterAppPersist::Interrupt
    })
    .expect("application state persists before injected interruption");
    assert_eq!(
        outcome,
        CommandApplyOutcome::Interrupted,
        "failure injection must interrupt after persistence and before a reply result escapes"
    );
}

pub(super) fn commit(
    node: &mut crate::raft_node::FileNode,
    command: &Command,
) -> (LogIndex, Vec<u8>) {
    let payload = serde_json::to_vec(command).expect("command encodes");
    let outputs = node
        .step(Input::ClientProposal { payload })
        .expect("single-node proposal commits durably");
    let applies = outputs
        .into_iter()
        .filter_map(|output| match output {
            Output::Apply { index, payload, .. } => Some((index, payload.to_vec())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(applies.len(), 1, "proposal must emit exactly one Apply");
    applies.into_iter().next().expect("one Apply exists")
}

pub(super) fn recovery_commands(outputs: &[Output]) -> Vec<(LogIndex, Command)> {
    outputs
        .iter()
        .filter_map(|output| match output {
            Output::Apply { index, payload, .. } => Some((
                *index,
                serde_json::from_slice(payload).expect("recovery command decodes"),
            )),
            _ => None,
        })
        .collect()
}

pub(super) fn write_command(in_reply_to: u64, value: u64) -> Command {
    Command {
        origin: "n1".to_owned(),
        client: "client".to_owned(),
        in_reply_to,
        request: ClientMutation::Write {
            key: json!("counter"),
            value: json!(value),
        },
    }
}

pub(super) fn cas_command(in_reply_to: u64, from: u64, to: u64) -> Command {
    Command {
        origin: "n1".to_owned(),
        client: "client".to_owned(),
        in_reply_to,
        request: ClientMutation::Cas {
            key: json!("counter"),
            from: json!(from),
            to: json!(to),
        },
    }
}

pub(super) fn expected_kv(value: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([(canonical_key(&json!("counter")), json!(value))])
}

pub(super) fn load_persisted_app(root: &Path) -> AppState {
    load_app_state(root).expect("persisted application state reopens")
}

pub(super) fn test_root(name: &str) -> PathBuf {
    let id = TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "rafter-maelstrom-{name}-{}-{id}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    root
}

pub(super) fn remove_test_root(root: PathBuf) {
    let _ = std::fs::remove_dir_all(root);
}
