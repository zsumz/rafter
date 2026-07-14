use std::io;

use rafter::LogIndex;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

use super::support::{expected_kv, load_persisted_app, remove_test_root, test_root};
use crate::app::{persist_app_state, persist_app_state_with_observer, AppPersistStage, AppState};

#[test]
pub(super) fn ps04_atomic_app_persist_syncs_file_and_directory_before_success() {
    let root = test_root("ps04-atomic-app-persist");
    let original = AppState {
        applied: LogIndex(1),
        kv: expected_kv(0),
    };
    persist_app_state(&root, &original).expect("baseline application state is durable");

    let replacement = AppState {
        applied: LogIndex(2),
        kv: expected_kv(1),
    };
    let mut temp_interruption = Vec::new();
    let error = persist_app_state_with_observer(&root, &replacement, |stage| {
        temp_interruption.push(stage);
        if stage == AppPersistStage::TempFileSynced {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "injected crash after temp-file sync",
            ))
        } else {
            Ok(())
        }
    });
    oracle_assert!(error.is_err());
    oracle_assert_eq!(temp_interruption, vec![AppPersistStage::TempFileSynced]);
    let reopened_original = load_persisted_app(&root);
    oracle_assert_eq!(reopened_original.applied, original.applied);
    oracle_assert_eq!(reopened_original.kv, original.kv);

    let mut rename_interruption = Vec::new();
    let error = persist_app_state_with_observer(&root, &replacement, |stage| {
        rename_interruption.push(stage);
        if stage == AppPersistStage::Renamed {
            Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "injected crash after atomic rename",
            ))
        } else {
            Ok(())
        }
    });
    oracle_assert!(error.is_err());
    oracle_assert_eq!(
        rename_interruption,
        vec![AppPersistStage::TempFileSynced, AppPersistStage::Renamed]
    );
    let reopened_replacement = load_persisted_app(&root);
    oracle_assert_eq!(reopened_replacement.applied, replacement.applied);
    oracle_assert_eq!(reopened_replacement.kv, replacement.kv);

    let final_state = AppState {
        applied: LogIndex(3),
        kv: expected_kv(2),
    };
    let mut completed = Vec::new();
    persist_app_state_with_observer(&root, &final_state, |stage| {
        completed.push(stage);
        Ok(())
    })
    .expect("successful persist crosses every durability boundary");
    oracle_assert_eq!(
        completed,
        vec![
            AppPersistStage::TempFileSynced,
            AppPersistStage::Renamed,
            AppPersistStage::DirectorySynced,
        ]
    );
    let reopened_final = load_persisted_app(&root);
    oracle_assert_eq!(reopened_final.applied, final_state.applied);
    oracle_assert_eq!(reopened_final.kv, final_state.kv);
    remove_test_root(root);
}
