use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use rafter::{Input, LogIndex, NodeId, Output, Role};
use serde_json::{json, Value};

use super::{
    apply_committed_command, canonical_key, load_app_state, AfterAppPersist, ClientMutation,
    Command, CommandApplyOutcome,
};
use crate::runtime::open_application_node;

static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn ps04_app_persist_interrupt_reopens_at_durable_floor_and_replays_suffix_once() {
    let root = test_root("ps04-app-persist-recovery");
    let mut opened = open_application_node(&root, NodeId(1), Vec::new())
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

    let commands = [
        write_command(1, 0),
        write_command(2, 1),
        write_command(3, 2),
    ];
    let committed = commands
        .iter()
        .map(|command| commit(&mut opened.node, command))
        .collect::<Vec<_>>();

    let (floor, floor_payload) = &committed[0];
    let floor_command: Command =
        serde_json::from_slice(floor_payload).expect("committed command decodes");
    let outcome = apply_committed_command(&root, &mut opened.app, *floor, &floor_command, |_| {
        AfterAppPersist::Interrupt
    })
    .expect("application state persists before injected interruption");
    assert_eq!(
        outcome,
        CommandApplyOutcome::Interrupted,
        "failure injection must interrupt after persistence and before a reply result escapes"
    );
    drop(opened);

    let persisted_after_crash = load_app_state(&root).expect("persisted application reopens");
    assert_eq!(persisted_after_crash.applied, *floor);
    assert_eq!(persisted_after_crash.kv, expected_kv(0));

    let mut reopened = open_application_node(&root, NodeId(1), Vec::new())
        .expect("production Maelstrom reopen path accepts durable application floor");
    assert_eq!(reopened.app.applied, *floor);
    assert!(reopened.app.applied <= reopened.node.commit_index());
    assert!(reopened.app.applied <= reopened.node.last_log_index());

    let replay = recovery_commands(&reopened.recovery_outputs);
    let replay_indices = replay.iter().map(|(index, _)| *index).collect::<Vec<_>>();
    let expected_indices = committed[1..]
        .iter()
        .map(|(index, _)| *index)
        .collect::<Vec<_>>();
    assert_eq!(
        replay_indices, expected_indices,
        "recovery must suppress Apply through the durable floor and replay the committed suffix in order"
    );
    assert!(replay_indices.iter().all(|index| *index > *floor));

    for (index, command) in &replay {
        let outcome = apply_committed_command(&root, &mut reopened.app, *index, command, |_| {
            AfterAppPersist::Continue
        })
        .expect("recovery command applies");
        assert!(matches!(outcome, CommandApplyOutcome::Applied(_)));
    }
    assert_eq!(reopened.app.kv, expected_kv(2));
    assert_eq!(reopened.app.applied, *expected_indices.last().unwrap());

    let state_after_replay = reopened.app.clone();
    let duplicate_outcome =
        apply_committed_command(&root, &mut reopened.app, replay[0].0, &replay[0].1, |_| {
            panic!("duplicate replay must not reach the post-persist injection point")
        })
        .expect("duplicate replay is ignored");
    assert_eq!(duplicate_outcome, CommandApplyOutcome::AlreadyApplied);
    assert_eq!(reopened.app.applied, state_after_replay.applied);
    assert_eq!(reopened.app.kv, state_after_replay.kv);

    let persisted_after_replay = load_app_state(&root).expect("replayed application reopens");
    assert_eq!(persisted_after_replay.applied, state_after_replay.applied);
    assert_eq!(persisted_after_replay.kv, state_after_replay.kv);
    let _ = std::fs::remove_dir_all(root);
}

fn commit(node: &mut crate::raft_node::FileNode, command: &Command) -> (LogIndex, Vec<u8>) {
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

fn recovery_commands(outputs: &[Output]) -> Vec<(LogIndex, Command)> {
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

fn write_command(in_reply_to: u64, value: u64) -> Command {
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

fn expected_kv(value: u64) -> BTreeMap<String, Value> {
    BTreeMap::from([(canonical_key(&json!("counter")), json!(value))])
}

fn test_root(name: &str) -> PathBuf {
    let id = TEST_DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
    let root = std::env::temp_dir().join(format!(
        "rafter-maelstrom-{name}-{}-{id}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    root
}
