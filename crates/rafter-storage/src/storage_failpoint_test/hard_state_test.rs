//! Hard-state crash windows resolve from the authoritative final path.

use crate::{
    FileRaftHardStateStore, RaftHardState, RaftHardStateStore, RaftHardStateStoreWriteError,
};

use super::{
    arm,
    support_test::{
        hard_state_temp_path, initial_hard_state, replacement_hard_state, TestWorkspace,
    },
    DurabilityPoint,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ReopenedState {
    Initial,
    Replacement,
}

#[test]
fn hard_state_publication_matrix_reopens_to_the_visible_commit() {
    let cases = [
        (
            DurabilityPoint::HardStateAfterTempSync,
            ReopenedState::Initial,
        ),
        (
            DurabilityPoint::HardStateAfterRename,
            ReopenedState::Replacement,
        ),
        (
            DurabilityPoint::HardStateAfterDirectorySync,
            ReopenedState::Replacement,
        ),
    ];

    for (point, expected) in cases {
        verify_hard_state_window(point, expected);
    }
}

fn verify_hard_state_window(point: DurabilityPoint, expected: ReopenedState) {
    let workspace = TestWorkspace::new(&format!("hard-state-{point:?}"));
    let path = workspace.path("hard-state");
    let initial = initial_hard_state();
    let replacement = replacement_hard_state();
    let mut store = FileRaftHardStateStore::open(&path).expect("hard-state store opens");
    store
        .write_hard_state(initial)
        .expect("initial hard state writes");

    let guard = arm(point);
    let error = store
        .write_hard_state(replacement)
        .expect_err("armed publication point fails");
    guard.assert_triggered();

    assert!(matches!(error, RaftHardStateStoreWriteError::Io { .. }));
    assert!(store.requires_reopen());
    assert_eq!(
        store.current(),
        initial,
        "a failed write never becomes acknowledged through the poisoned handle"
    );
    let temp_exists = hard_state_temp_path(&path).exists();
    if point == DurabilityPoint::HardStateAfterTempSync {
        assert!(temp_exists);
    } else {
        assert!(!temp_exists);
    }

    drop(store);
    let reopened = FileRaftHardStateStore::open(&path).expect("hard-state store reopens");
    assert!(!reopened.requires_reopen());
    assert_eq!(
        reopened.current(),
        expected_state(expected, initial, replacement)
    );
}

const fn expected_state(
    expected: ReopenedState,
    initial: RaftHardState,
    replacement: RaftHardState,
) -> RaftHardState {
    match expected {
        ReopenedState::Initial => initial,
        ReopenedState::Replacement => replacement,
    }
}
