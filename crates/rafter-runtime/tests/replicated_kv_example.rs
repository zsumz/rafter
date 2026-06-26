#[allow(dead_code)]
#[path = "../examples/replicated_kv.rs"]
mod replicated_kv;

use std::collections::BTreeMap;

use rafter::{LogIndex, NodeId};

use replicated_kv::{ProcessSpawn, ScenarioOptions, ScenarioReport};

#[test]
fn replicated_kv_example_covers_full_lifecycle() {
    let root =
        std::env::temp_dir().join(format!("rafter-replicated-kv-test-{}", std::process::id()));

    let report: ScenarioReport = replicated_kv::run_in_process_demo(
        root,
        ScenarioOptions {
            keep_dir: false,
            verbose: false,
        },
    );

    assert_eq!(report.alpha_read, Some("1".to_string()));
    assert_eq!(report.final_values.get("beta"), Some(&"2".to_string()));
    assert_eq!(report.final_values.get("gamma"), Some(&"3".to_string()));
    assert_eq!(report.final_values.get("delta"), Some(&"4".to_string()));
    assert!(report.snapshot_index >= report.restarted_applied_floor);
}

#[test]
fn replicated_kv_app_state_record_is_atomic_and_checksummed() {
    let root = std::env::temp_dir().join(format!(
        "rafter-replicated-kv-app-state-test-{}",
        std::process::id()
    ));
    let node_id = NodeId(42);
    let dir = root.join(format!("node-{}", node_id.0));
    let mut kv = BTreeMap::new();
    kv.insert("alpha".to_owned(), "1".to_owned());
    kv.insert("beta".to_owned(), "2".to_owned());

    replicated_kv::persist_app_state(&dir, &kv, LogIndex(7));

    assert!(dir.join("app.state").exists());
    assert!(!dir.join("app.tsv").exists());
    assert!(!dir.join("app.applied").exists());
    let loaded = replicated_kv::load_app_state(&root, node_id);
    assert_eq!(loaded.kv, kv);
    assert_eq!(loaded.applied, LogIndex(7));

    let path = dir.join("app.state");
    let mut record = std::fs::read(&path).expect("read app state record");
    let last = record.last_mut().expect("record has payload");
    *last ^= 0x01;
    std::fs::write(&path, record).expect("write corrupt app state record");
    let rejected = std::panic::catch_unwind(|| replicated_kv::load_app_state(&root, node_id));
    assert!(rejected.is_err(), "corrupt app state record must reject");

    std::fs::remove_dir_all(root).ok();
}

#[test]
#[ignore = "requires loopback TCP and child process spawning"]
fn replicated_kv_process_per_node_tcp_survives_kill_restart() {
    let root = std::env::temp_dir().join(format!(
        "rafter-replicated-kv-process-test-{}",
        std::process::id()
    ));
    let report: ScenarioReport = replicated_kv::run_process_demo_with_spawn(
        root,
        ScenarioOptions {
            keep_dir: false,
            verbose: false,
        },
        ProcessSpawn::test_harness(std::env::current_exe().expect("current test executable")),
    );

    assert_eq!(report.alpha_read, Some("1".to_string()));
    assert_eq!(report.final_values.get("beta"), Some(&"2".to_string()));
    assert_eq!(report.final_values.get("gamma"), Some(&"3".to_string()));
    assert_eq!(report.final_values.get("delta"), Some(&"4".to_string()));
    assert!(report.snapshot_index >= report.restarted_applied_floor);
}

#[test]
#[ignore = "child process entrypoint for replicated_kv_process_per_node_tcp_survives_kill_restart"]
fn replicated_kv_process_node_child() {
    if std::env::var_os("RAFTER_KV_PROCESS_NODE").is_some() {
        replicated_kv::run_process_node_from_env();
    }
}
