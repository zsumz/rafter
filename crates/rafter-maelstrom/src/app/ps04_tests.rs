use rafter::NodeId;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

use crate::runtime::{dispatch_recovery_outputs, open_application_node};

use self::support::{
    commit, expected_kv, initialize_process, load_persisted_app, persist_with_interruption,
    recovery_commands, remove_test_root, test_root, write_command,
};

mod durability;
mod floor;
mod live_snapshot;
mod snapshot;
mod support;

#[test]
fn ps04_app_persist_interrupt_reopens_at_durable_floor_and_replays_suffix_once() {
    let root = test_root("ps04-app-persist-recovery");
    let mut opened = support::fresh_elected_node(&root);
    let commands = [
        write_command(1, 0),
        write_command(2, 1),
        write_command(3, 2),
    ];
    let committed = commands
        .iter()
        .map(|command| commit(&mut opened.node, command))
        .collect::<Vec<_>>();

    persist_with_interruption(&root, &mut opened.app, &committed[0]);
    let floor = committed[0].0;
    drop(opened);

    let persisted_after_crash = load_persisted_app(&root);
    oracle_assert_eq!(persisted_after_crash.applied, floor);
    oracle_assert_eq!(persisted_after_crash.kv, expected_kv(0));

    let reopened = open_application_node(&root, NodeId(1), Vec::new())
        .expect("production Maelstrom reopen path accepts durable application floor");
    oracle_assert_eq!(reopened.app.applied, floor);
    oracle_assert!(reopened.app.applied <= reopened.node.commit_index());
    oracle_assert!(reopened.app.applied <= reopened.node.last_log_index());

    let replay = recovery_commands(&reopened.recovery_outputs);
    let replay_indices = replay.iter().map(|(index, _)| *index).collect::<Vec<_>>();
    let expected_indices = committed[1..]
        .iter()
        .map(|(index, _)| *index)
        .collect::<Vec<_>>();
    oracle_assert_eq!(
        replay_indices, expected_indices,
        "recovery must suppress Apply through the durable floor and replay the committed suffix in order"
    );
    oracle_assert!(replay_indices.iter().all(|index| *index > floor));
    let duplicate = reopened.recovery_outputs[0].clone();
    drop(reopened);

    let mut process = initialize_process(&root);
    let initialized = process.initialized.as_mut().expect("node initializes");
    oracle_assert_eq!(initialized.app.kv, expected_kv(2));
    oracle_assert_eq!(
        initialized.app.applied,
        *expected_indices.last().expect("committed suffix exists")
    );
    oracle_assert!(initialized
        .completed_replies
        .contains(&("client".to_owned(), 2)));
    oracle_assert!(initialized
        .completed_replies
        .contains(&("client".to_owned(), 3)));

    let state_after_replay = initialized.app.clone();
    dispatch_recovery_outputs(initialized, vec![duplicate]);
    oracle_assert_eq!(initialized.app.applied, state_after_replay.applied);
    oracle_assert_eq!(initialized.app.kv, state_after_replay.kv);

    let persisted_after_replay = load_persisted_app(&root);
    oracle_assert_eq!(persisted_after_replay.applied, state_after_replay.applied);
    oracle_assert_eq!(persisted_after_replay.kv, state_after_replay.kv);
    remove_test_root(root);
}
