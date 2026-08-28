//! Reading and writing the application checkpoint, and the snapshot payload it
//! encodes into.
//!
//! The checkpoint is an optimization over replaying the log, never the
//! durability guarantee, so a failure here is handed back to the caller and
//! never allowed to make an applied command look un-applied.

use std::{collections::BTreeMap, error::Error, path::Path};

use rafter::LogIndex;

use super::{decode_snapshot_payload, persist_app_state_with_observer, AppState, PersistedApp};

pub(crate) fn load_app_state(root: &Path) -> Result<AppState, Box<dyn Error>> {
    let path = root.join("app.json");
    if !path.exists() {
        return Ok(AppState {
            applied: LogIndex::ZERO,
            kv: BTreeMap::new(),
        });
    }
    let persisted: PersistedApp = serde_json::from_slice(&std::fs::read(path)?)?;
    Ok(AppState {
        applied: LogIndex(persisted.applied),
        kv: persisted.kv,
    })
}

pub(crate) fn persist_app_state(root: &Path, app: &AppState) -> Result<(), Box<dyn Error>> {
    persist_app_state_with_observer(root, app, |_| Ok(()))
}

pub(crate) fn persist_snapshot_application_state(
    root: &Path,
    app: &mut AppState,
    snapshot_index: LogIndex,
    payload: &[u8],
) -> Result<(), Box<dyn Error>> {
    let kv = decode_snapshot_payload(payload).map_err(std::io::Error::other)?;
    app.kv = kv;
    app.applied = snapshot_index;
    persist_app_state(root, app)
}
