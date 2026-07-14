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

use crate::protocol::body_type;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AfterAppPersist {
    Continue,
    Interrupt,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum AppPersistStage {
    TempFileSynced,
    Renamed,
    DirectorySynced,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CommandApplyOutcome {
    Applied(ClientResult),
    AlreadyApplied,
    Interrupted,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Command {
    pub(crate) origin: String,
    pub(crate) client: String,
    pub(crate) in_reply_to: u64,
    pub(crate) request: ClientMutation,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum ClientMutation {
    Write { key: Value, value: Value },
    Cas { key: Value, from: Value, to: Value },
}

#[derive(Clone, Debug)]
pub(crate) enum ClientRequest {
    Read { key: Value },
    Write { key: Value, value: Value },
    Cas { key: Value, from: Value, to: Value },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ClientResult {
    ReadOk { value: Value },
    WriteOk,
    CasOk,
    Error { code: u64, text: String },
}

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

pub(crate) fn apply_committed_command(
    root: &Path,
    app: &mut AppState,
    index: LogIndex,
    command: &Command,
    after_persist: impl FnOnce(&Path) -> AfterAppPersist,
) -> Result<CommandApplyOutcome, Box<dyn Error>> {
    if index <= app.applied {
        return Ok(CommandApplyOutcome::AlreadyApplied);
    }

    let result = apply_mutation(&mut app.kv, &command.request);
    app.applied = index;
    persist_app_state(root, app)?;

    if after_persist(root) == AfterAppPersist::Interrupt {
        return Ok(CommandApplyOutcome::Interrupted);
    }
    Ok(CommandApplyOutcome::Applied(result))
}

pub(crate) fn maybe_crash_after_app_persist_before_reply(root: &Path) {
    if std::env::var_os(CRASH_AFTER_APP_PERSIST_ONCE_ENV).is_none() {
        return;
    }
    if claim_app_persist_crash_point_once(root) {
        eprintln!(
            "rafter-maelstrom crashpoint={CRASH_AFTER_APP_PERSIST_ONCE_ENV} fired after app persist before reply"
        );
        std::process::exit(APP_PERSIST_CRASH_EXIT_CODE);
    }
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

pub(crate) fn parse_client_request(body: &Value) -> Result<ClientRequest, ClientResult> {
    match body_type(body) {
        Some("read") => Ok(ClientRequest::Read {
            key: required_value(body, "key")?,
        }),
        Some("write") => Ok(ClientRequest::Write {
            key: required_value(body, "key")?,
            value: required_value(body, "value")?,
        }),
        Some("cas") => Ok(ClientRequest::Cas {
            key: required_value(body, "key")?,
            from: required_value(body, "from")?,
            to: required_value(body, "to")?,
        }),
        Some(other) => Err(ClientResult::Error {
            code: ERROR_TEMPORARILY_UNAVAILABLE,
            text: format!("unsupported request type {other}"),
        }),
        None => Err(ClientResult::Error {
            code: ERROR_TEMPORARILY_UNAVAILABLE,
            text: "request body missing type".to_string(),
        }),
    }
}

fn required_value(body: &Value, field: &str) -> Result<Value, ClientResult> {
    body.get(field).cloned().ok_or_else(|| ClientResult::Error {
        code: ERROR_TEMPORARILY_UNAVAILABLE,
        text: format!("request missing {field}"),
    })
}

pub(crate) fn apply_mutation(
    kv: &mut BTreeMap<String, Value>,
    request: &ClientMutation,
) -> ClientResult {
    match request {
        ClientMutation::Write { key, value } => {
            kv.insert(canonical_key(key), value.clone());
            ClientResult::WriteOk
        }
        ClientMutation::Cas { key, from, to } => {
            let key = canonical_key(key);
            let Some(current) = kv.get_mut(&key) else {
                return ClientResult::Error {
                    code: ERROR_KEY_DOES_NOT_EXIST,
                    text: "key does not exist".to_string(),
                };
            };
            if current != from {
                return ClientResult::Error {
                    code: ERROR_PRECONDITION_FAILED,
                    text: "current value did not match CAS precondition".to_string(),
                };
            }
            *current = to.clone();
            ClientResult::CasOk
        }
    }
}

pub(crate) fn read_value(kv: &BTreeMap<String, Value>, key: &Value) -> ClientResult {
    kv.get(&canonical_key(key)).map_or_else(
        || ClientResult::Error {
            code: ERROR_KEY_DOES_NOT_EXIST,
            text: "key does not exist".to_string(),
        },
        |value| ClientResult::ReadOk {
            value: value.clone(),
        },
    )
}

fn canonical_key(key: &Value) -> String {
    serde_json::to_string(key).expect("JSON value serializes")
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod ps04_tests;
