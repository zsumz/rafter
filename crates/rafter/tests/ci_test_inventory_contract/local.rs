//! Local orchestration must run the same reviewed durable selection as CI.

use std::path::Path;

pub(super) fn assert_durable_inventory(root: &Path, selection: &str) {
    let local = super::read(&root.join("zcheck.toml"));
    let local_task = local
        .split("[tasks.durable-client]")
        .nth(1)
        .and_then(|task| task.split("\n[").next())
        .expect("local durable-client task");
    let arguments = selection
        .split_whitespace()
        .map(|argument| format!("{argument:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    assert!(
        local_task.contains(&format!("run = [{arguments}]")),
        "local durable inventory must match the reviewed CI selection"
    );
}
