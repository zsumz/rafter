//! Isolated hard-state publication timing and a delimited Linux syscall probe.
//!
//! Usage: bench-hard-state replace|journal WRITES EXISTING_SCRATCH_PARENT.
//! A fresh private directory is created and removed for each invocation.

use rafter::{LogIndex, Term};
use rafter_storage::{
    FileRaftHardStateStore, JournalRaftHardStateStore, RaftHardState, RaftHardStateStore,
};
use std::{path::PathBuf, time::Instant};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    assert_eq!(args.len(), 3, "expected backend, writes, scratch parent");
    let writes: u32 = args[1].parse().expect("integer writes");
    assert!((1..=1_000_000).contains(&writes), "bounded writes");
    assert!(
        matches!(args[0].as_str(), "replace" | "journal"),
        "known backend"
    );
    let directory =
        PathBuf::from(&args[2]).join(format!("hard-state-probe-{}", std::process::id()));
    std::fs::create_dir(&directory).expect("fresh private scratch directory");
    let path = directory.join("hard-state");
    let open = || -> Box<dyn RaftHardStateStore> {
        match args[0].as_str() {
            "replace" => Box::new(FileRaftHardStateStore::open(&path).expect("replace open")),
            "journal" => Box::new(JournalRaftHardStateStore::open(&path).expect("journal open")),
            _ => unreachable!("validated backend"),
        }
    };
    let mut store = open();
    let mut state = RaftHardState {
        current_term: Term(1),
        ..RaftHardState::default()
    };
    store
        .write_hard_state(state)
        .expect("initial publication outside measurement");
    eprintln!("BENCH_WRITES_BEGIN");
    let started = Instant::now();
    for index in 1..=writes {
        state.commit_index = LogIndex(u64::from(index));
        store.write_hard_state(state).expect("durable publication");
    }
    let elapsed = started.elapsed();
    eprintln!("BENCH_WRITES_END");
    assert_eq!(store.current(), state);
    drop(store);
    assert_eq!(
        open().current(),
        state,
        "reopen recovers last acknowledged state"
    );
    let file_bytes = std::fs::metadata(&path).expect("metadata").len();
    println!("{{\"backend\": \"{}\", \"writes\": {writes}, \"elapsed_ms\": {:.3}, \"writes_per_s\": {:.3}, \"file_bytes\": {file_bytes}}}",
        args[0], elapsed.as_secs_f64() * 1000.0, f64::from(writes) / elapsed.as_secs_f64());
    std::fs::remove_dir_all(&directory).expect("remove scratch");
}
