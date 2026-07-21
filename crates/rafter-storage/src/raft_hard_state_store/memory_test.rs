//! Volatile hard-state replacement and acknowledged-state scenarios.

use super::test_support::hard_state;
use super::{InMemoryRaftHardStateStore, RaftHardStateStore};

#[test]
fn in_memory_store_returns_latest_written_hard_state() {
    let mut store = InMemoryRaftHardStateStore::new();

    store
        .write_hard_state(hard_state(1, Some(7)))
        .expect("state writes");
    store
        .write_hard_state(hard_state(2, None))
        .expect("state writes");

    assert_eq!(store.current(), hard_state(2, None));
}
