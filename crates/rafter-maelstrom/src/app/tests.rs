use super::*;

use serde_json::json;
use std::{
    collections::BTreeMap,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static TEST_DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

#[test]
fn lin_kv_write_read_and_cas_apply_in_log_order() {
    let mut kv = BTreeMap::new();
    assert!(matches!(
        apply_mutation(
            &mut kv,
            &ClientMutation::Write {
                key: json!(1),
                value: json!(7),
            },
        ),
        ClientResult::WriteOk
    ));
    assert_eq!(
        read_value(&kv, &json!(1)),
        ClientResult::ReadOk { value: json!(7) }
    );
    assert!(matches!(
        apply_mutation(
            &mut kv,
            &ClientMutation::Cas {
                key: json!(1),
                from: json!(7),
                to: json!(8),
            },
        ),
        ClientResult::CasOk
    ));
    assert_eq!(
        read_value(&kv, &json!(1)),
        ClientResult::ReadOk { value: json!(8) }
    );
}

#[test]
fn lin_kv_cas_reports_maelstrom_error_codes() {
    let mut kv = BTreeMap::new();
    assert!(matches!(
        apply_mutation(
            &mut kv,
            &ClientMutation::Cas {
                key: json!("missing"),
                from: json!(1),
                to: json!(2),
            },
        ),
        ClientResult::Error {
            code: ERROR_KEY_DOES_NOT_EXIST,
            ..
        }
    ));
    apply_mutation(
        &mut kv,
        &ClientMutation::Write {
            key: json!("x"),
            value: json!(1),
        },
    );
    assert!(matches!(
        apply_mutation(
            &mut kv,
            &ClientMutation::Cas {
                key: json!("x"),
                from: json!(2),
                to: json!(3),
            },
        ),
        ClientResult::Error {
            code: ERROR_PRECONDITION_FAILED,
            ..
        }
    ));
}

#[test]
fn snapshot_payload_round_trips_maelstrom_kv_state() {
    let kv = BTreeMap::from([
        (canonical_key(&json!("alpha")), json!(1)),
        (canonical_key(&json!(["nested", 2])), json!({"ok": true})),
    ]);
    let payload = encode_snapshot_payload(&kv).expect("snapshot payload encodes");
    let decoded = decode_snapshot_payload(&payload).expect("snapshot payload decodes");
    assert_eq!(decoded, kv);
}

#[test]
fn persisted_app_state_round_trips_applied_floor() {
    let root = test_root("app-state");
    let app = AppState {
        applied: LogIndex(7),
        kv: BTreeMap::from([(canonical_key(&json!("key")), json!("value"))]),
    };

    persist_app_state(&root, &app).expect("app state persists");
    let loaded = load_app_state(&root).expect("app state reloads");

    assert_eq!(loaded.applied, LogIndex(7));
    assert_eq!(loaded.kv, app.kv);
    let _ = std::fs::remove_dir_all(root);
}

#[test]
fn app_persist_crash_point_fires_once_per_root() {
    let root = test_root("app-persist-crashpoint");
    std::fs::create_dir_all(&root).expect("test root exists");

    assert!(claim_app_persist_crash_point_once(&root));
    assert!(root.join(APP_PERSIST_CRASH_MARKER).exists());
    assert!(!claim_app_persist_crash_point_once(&root));
    let _ = std::fs::remove_dir_all(root);
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
