//! The replicated key/value state machine and its durability boundary.
//!
//! The checkpoint written here is an optimization, not the durability
//! guarantee. The Raft log holds every committed entry, and recovery replays
//! from it; the checkpoint only lets recovery skip work it has already done. So
//! a checkpoint that fails to write does not make an applied command
//! un-applied, and does not excuse the node from answering for it — the
//! distinction [`CommandApplyOutcome`] exists to keep.
//!
//! The crash points are the point of this module. They fire between the
//! durable write and the client's reply, which is the window where a node can
//! acknowledge something it will forget, or forget something it acknowledged.

use std::{
    collections::BTreeMap,
    error::Error,
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use rafter::LogIndex;
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod apply;
mod checkpoint;
mod requests;

pub(crate) use apply::{
    apply_committed_command, maybe_crash_after_app_persist_before_reply, AfterAppPersist,
    CommandApplyOutcome,
};
pub(crate) use checkpoint::{
    load_app_state, persist_app_state, persist_snapshot_application_state,
};
pub(crate) use requests::{
    apply_mutation, parse_client_request, read_value, ClientMutation, ClientRequest, ClientResult,
    Command,
};

/// Maelstrom's `timeout`, and the only *indefinite* error this harness sends.
///
/// An indefinite error tells the checker that the operation may or may not
/// have taken effect, so it is free to order the request either way. Every
/// other code below is definite — it asserts the operation did not happen —
/// which is why none of them can answer a request whose fate this node does
/// not know. Sending a definite code for a write that may have committed is
/// not caution; it is a false statement the checker will hold this node to.
pub(crate) const ERROR_TIMEOUT: u64 = 0;
pub(crate) const ERROR_TEMPORARILY_UNAVAILABLE: u64 = 11;
pub(crate) const ERROR_KEY_DOES_NOT_EXIST: u64 = 20;
pub(crate) const ERROR_PRECONDITION_FAILED: u64 = 22;

const CRASH_AFTER_APP_PERSIST_ONCE_ENV: &str = "RAFTER_MAELSTROM_CRASH_AFTER_APP_PERSIST_ONCE";
const APP_PERSIST_CRASH_MARKER: &str = ".app-persist-crashpoint-fired";
const APP_PERSIST_CRASH_EXIT_CODE: i32 = 42;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedApp {
    applied: u64,
    kv: BTreeMap<String, Value>,
}

#[derive(Clone, Debug)]
pub(crate) struct AppState {
    pub(crate) applied: LogIndex,
    pub(crate) kv: BTreeMap<String, Value>,
}

/// The three durable moments of one checkpoint write.
///
/// Writing a temporary file, renaming it over the old one, and syncing the
/// directory are separately durable, and a crash between any two leaves a
/// different state on disk. Naming them lets a test place a fault at each.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AppPersistStage {
    /// The new contents are durable, but nothing points at them yet; recovery
    /// still reads the old checkpoint.
    TempFileSynced,
    /// The directory entry now names the new file, but that rename is not
    /// itself durable — recovery may still read either one.
    Renamed,
    /// The rename is durable; recovery reads the new checkpoint.
    DirectorySynced,
}

pub(super) fn persist_app_state_with_observer(
    root: &Path,
    app: &AppState,
    mut observer: impl FnMut(AppPersistStage) -> io::Result<()>,
) -> Result<(), Box<dyn Error>> {
    create_dir_all_durable(root)?;
    let tmp = root.join("app.json.tmp");
    let path = root.join("app.json");
    let persisted = PersistedApp {
        applied: app.applied.0,
        kv: app.kv.clone(),
    };
    let bytes = serde_json::to_vec(&persisted)?;
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    observer(AppPersistStage::TempFileSynced)?;
    drop(file);

    fs::rename(&tmp, &path)?;
    observer(AppPersistStage::Renamed)?;
    sync_directory(root)?;
    observer(AppPersistStage::DirectorySynced)?;
    Ok(())
}

fn create_dir_all_durable(path: &Path) -> io::Result<()> {
    let mut missing = Vec::<PathBuf>::new();
    let mut candidate = path;
    while !candidate.exists() {
        missing.push(candidate.to_path_buf());
        let Some(parent) = candidate.parent() else {
            break;
        };
        candidate = parent;
    }
    fs::create_dir_all(path)?;
    for directory in missing.iter().rev() {
        let parent = directory
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_directory(parent)?;
    }
    Ok(())
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

fn claim_app_persist_crash_point_once(root: &Path) -> bool {
    let marker = root.join(APP_PERSIST_CRASH_MARKER);
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&marker)
    {
        Ok(mut file) => {
            let _ = writeln!(file, "after app persist before reply");
            true
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => false,
        Err(error) => {
            eprintln!(
                "failed to claim app persist crashpoint marker {}: {error}",
                marker.display()
            );
            false
        }
    }
}

pub(crate) fn encode_snapshot_payload(kv: &BTreeMap<String, Value>) -> Result<Vec<u8>, String> {
    serde_json::to_vec(kv).map_err(|error| error.to_string())
}

pub(crate) fn decode_snapshot_payload(payload: &[u8]) -> Result<BTreeMap<String, Value>, String> {
    serde_json::from_slice(payload).map_err(|error| error.to_string())
}

fn canonical_key(key: &Value) -> String {
    serde_json::to_string(key).expect("JSON value serializes")
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod ps04_tests;
