use std::{collections::BTreeMap, io::Write, path::Path};

use rafter::{LogIndex, NodeId};

use super::{
    codec::{crc32, decode_snapshot, encode_snapshot},
    storage::node_dir,
};

const APP_STATE_FILE: &str = "app.state";
const APP_STATE_TMP_FILE: &str = "app.state.tmp";
const APP_STATE_MAGIC: &[u8; 4] = b"RKVS";
const APP_STATE_VERSION: u8 = 1;
const APP_STATE_HEADER_LEN: usize = 4 + 1 + 8 + 8 + 4;

#[derive(Debug)]
pub struct AppState {
    pub kv: BTreeMap<String, String>,
    pub applied: LogIndex,
}

pub fn load_app_state(root: &Path, node_id: NodeId) -> AppState {
    let dir = node_dir(root, node_id);
    let path = dir.join(APP_STATE_FILE);
    if !path.exists() {
        return AppState {
            kv: BTreeMap::new(),
            applied: LogIndex::ZERO,
        };
    }
    let record = std::fs::read(path).expect("read app state record");
    decode_app_state_record(&record).expect("app state record is valid")
}

pub fn persist_app_state(dir: &Path, kv: &BTreeMap<String, String>, applied: LogIndex) {
    std::fs::create_dir_all(dir).expect("create app state directory");
    let tmp_path = dir.join(APP_STATE_TMP_FILE);
    let path = dir.join(APP_STATE_FILE);
    let record = encode_app_state_record(kv, applied);

    // The app snapshot and applied floor are one crash-consistency unit:
    // recovery passes `applied` into the Raft runtime, so publishing the floor
    // without the matching KV bytes could skip committed commands. Write and
    // sync a complete temp record, atomically rename it into place, then sync
    // the parent directory so the rename itself is durable on Unix filesystems.
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .expect("open app state temp file");
    file.write_all(&record).expect("write app state record");
    file.sync_all().expect("sync app state temp file");
    drop(file);
    std::fs::rename(&tmp_path, &path).expect("install app state record");
    sync_parent_dir(dir);
}

fn encode_app_state_record(kv: &BTreeMap<String, String>, applied: LogIndex) -> Vec<u8> {
    let payload = encode_snapshot(kv);
    let payload_len = u64::try_from(payload.len()).expect("app state payload length fits u64");
    let payload_crc32 = crc32(&payload);
    let mut record = Vec::with_capacity(APP_STATE_HEADER_LEN + payload.len());
    record.extend_from_slice(APP_STATE_MAGIC);
    record.push(APP_STATE_VERSION);
    record.extend_from_slice(&applied.0.to_be_bytes());
    record.extend_from_slice(&payload_len.to_be_bytes());
    record.extend_from_slice(&payload_crc32.to_be_bytes());
    record.extend_from_slice(&payload);
    record
}

fn decode_app_state_record(record: &[u8]) -> Result<AppState, String> {
    if record.len() < APP_STATE_HEADER_LEN {
        return Err(format!(
            "app state record is {} bytes; header needs {APP_STATE_HEADER_LEN}",
            record.len()
        ));
    }
    if &record[..4] != APP_STATE_MAGIC {
        return Err("app state record has invalid magic".to_owned());
    }
    if record[4] != APP_STATE_VERSION {
        return Err(format!(
            "app state record version {} is unsupported",
            record[4]
        ));
    }

    let applied = u64::from_be_bytes(record[5..13].try_into().expect("applied field width"));
    let payload_len = u64::from_be_bytes(
        record[13..21]
            .try_into()
            .expect("payload length field width"),
    );
    let expected_crc32 = u32::from_be_bytes(record[21..25].try_into().expect("crc32 field width"));
    let payload_len =
        usize::try_from(payload_len).map_err(|_| "app state payload is too large".to_owned())?;
    let expected_record_len = APP_STATE_HEADER_LEN + payload_len;
    if record.len() != expected_record_len {
        return Err(format!(
            "app state record is {} bytes; header declares {expected_record_len}",
            record.len()
        ));
    }

    let payload = &record[APP_STATE_HEADER_LEN..];
    let actual_crc32 = crc32(payload);
    if actual_crc32 != expected_crc32 {
        return Err(format!(
            "app state payload checksum mismatch: expected {expected_crc32:#010x}, actual {actual_crc32:#010x}"
        ));
    }
    Ok(AppState {
        kv: decode_snapshot(payload),
        applied: LogIndex(applied),
    })
}

#[cfg(unix)]
fn sync_parent_dir(dir: &Path) {
    let directory = std::fs::File::open(dir).expect("open app state directory for sync");
    directory.sync_all().expect("sync app state directory");
}

#[cfg(not(unix))]
fn sync_parent_dir(_dir: &Path) {}
