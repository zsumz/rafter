//! The peer-control-plane checkpoint, made durable beside this replica's log.
//!
//! Rafter opens no files, so a driver's peer control plane is a value its
//! embedder persists. This is that embedder. The three facts it carries —
//! which `NodeId`s a committed removal has spent, how far identity allocation
//! has got, and which fences the link layer has not accepted — cannot be
//! rebuilt from the Raft log: retirement is the *difference* between two
//! committed configurations, a restarted process sees only the latest one, and
//! compaction erases the rest. A process that dropped them would stop retrying a
//! refused fence and would let a spent identity be allocated again.
//!
//! # This cluster's membership never changes
//!
//! `CONTRACT.md` says so, and it is why this file is short. With a static
//! configuration the checkpoint holds one committed set and one high-water mark
//! and never grows a fence obligation, so what is exercised here is that the
//! *plumbing* is real: the mark survives a restart, the live set survives with
//! it, and a replica that reopens is refused an identity the cluster spent. A
//! consumer whose cluster did reconfigure would write exactly this file and get
//! exactly this behavior — which is the point of wiring it in a consumer that
//! does not need it.
//!
//! # Crash contract
//!
//! One statement, published the way this process publishes its peer address and
//! then made durable the way its store makes an image durable: the whole file is
//! written under a process-unique temporary name, `fsync`ed, renamed over the
//! final name, and the directory entry `fsync`ed. A reader therefore sees the
//! previous checkpoint or this one and never half of either, and a crash after
//! the rename returns cannot lose it.
//!
//! **A checkpoint is allowed to be stale, and the staleness is bounded by when
//! it was last written.** A crash between a change and its persistence loses
//! that change and no more. This process writes whenever the driver's epoch
//! moves, which is on every committed configuration that moves the retirement
//! record and every fence the link layer accepts — so the window is one tick of
//! the process loop. The deployment's monotonic `NodeId` allocator remains the
//! cross-process backstop, exactly as `rafter::NodeId` states.
//!
//! An absent file is a first boot and reads as an empty checkpoint. A malformed
//! one is not: this refuses rather than starting from nothing, because starting
//! from nothing is precisely the state this file exists to prevent, and doing it
//! silently would make a corrupted byte indistinguishable from a fresh replica.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rafter::NodeId;
use rafter_service::PeerControlPlaneCheckpoint;

/// The file name inside a replica's directory.
const CHECKPOINT_FILE: &str = "control-plane";

/// The format tag, so a later shape is a refusal rather than a misreading.
const FORMAT_TAG: &str = "rafter-lock-control-plane 1";

/// Why a checkpoint could not be read or written.
#[derive(Debug)]
pub enum CheckpointError {
    /// The file could not be read, staged, synced, or renamed.
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    /// The file exists and this build cannot interpret it.
    ///
    /// Refused rather than defaulted, for the reason the module header gives.
    Malformed { path: PathBuf, detail: String },
}

impl std::fmt::Display for CheckpointError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} the control-plane checkpoint at {}: {source}",
                path.display()
            ),
            Self::Malformed { path, detail } => write!(
                formatter,
                "the control-plane checkpoint at {} is malformed: {detail}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for CheckpointError {}

/// Returns the checkpoint this replica last made durable.
///
/// An absent file is a first boot and answers an empty checkpoint, which is what
/// a driver over empty storage would have derived anyway.
///
/// # Errors
///
/// Returns an error when the file exists and cannot be read or interpreted.
pub fn load(node_dir: &Path) -> Result<PeerControlPlaneCheckpoint, CheckpointError> {
    let path = node_dir.join(CHECKPOINT_FILE);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PeerControlPlaneCheckpoint::default());
        }
        Err(source) => {
            return Err(CheckpointError::Io {
                operation: "read",
                path,
                source,
            })
        }
    };
    decode(&text).map_err(|detail| CheckpointError::Malformed { path, detail })
}

/// Makes one checkpoint durable, replacing whatever was there.
///
/// # Errors
///
/// Returns an error when the file cannot be staged, synced, or renamed.
pub fn store(
    node_dir: &Path,
    checkpoint: &PeerControlPlaneCheckpoint,
) -> Result<(), CheckpointError> {
    let final_path = node_dir.join(CHECKPOINT_FILE);
    let staged_path = node_dir.join(format!("{CHECKPOINT_FILE}.{}.tmp", std::process::id()));
    let bytes = encode(checkpoint);

    let mut file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&staged_path)
        .map_err(|source| CheckpointError::Io {
            operation: "create",
            path: staged_path.clone(),
            source,
        })?;
    file.write_all(bytes.as_bytes())
        .and_then(|()| file.sync_all())
        .map_err(|source| CheckpointError::Io {
            operation: "write",
            path: staged_path.clone(),
            source,
        })?;
    drop(file);

    fs::rename(&staged_path, &final_path).map_err(|source| CheckpointError::Io {
        operation: "publish",
        path: final_path.clone(),
        source,
    })?;
    // The rename is what makes the new bytes reachable, and the directory entry
    // carrying it has to be durable too — otherwise a crash can leave the old
    // name pointing at the old file with the new one already synced and
    // unreferenced.
    File::open(node_dir)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| CheckpointError::Io {
            operation: "sync the directory of",
            path: final_path,
            source,
        })
}

/// One line per fact, so a reader can see what a replica retired.
fn encode(checkpoint: &PeerControlPlaneCheckpoint) -> String {
    let mut text = String::from(FORMAT_TAG);
    text.push_str("\nhigh_water ");
    match checkpoint.committed_id_high_water {
        Some(node_id) => text.push_str(&node_id.0.to_string()),
        None => text.push('-'),
    }
    text.push_str("\nlive");
    for node_id in &checkpoint.live_committed_members {
        text.push(' ');
        text.push_str(&node_id.0.to_string());
    }
    text.push_str("\nfences");
    for node_id in &checkpoint.pending_fences {
        text.push(' ');
        text.push_str(&node_id.0.to_string());
    }
    text.push('\n');
    text
}

fn decode(text: &str) -> Result<PeerControlPlaneCheckpoint, String> {
    let mut lines = text.lines();
    let tag = lines.next().ok_or_else(|| "the file is empty".to_owned())?;
    if tag != FORMAT_TAG {
        return Err(format!("expected `{FORMAT_TAG}`, found `{tag}`"));
    }

    let mut checkpoint = PeerControlPlaneCheckpoint::default();
    let high_water = field(lines.next(), "high_water")?;
    checkpoint.committed_id_high_water = match high_water {
        "-" => None,
        value => Some(NodeId(parse_id(value)?)),
    };
    for value in field(lines.next(), "live")?.split_whitespace() {
        checkpoint
            .live_committed_members
            .insert(NodeId(parse_id(value)?));
    }
    for value in field(lines.next(), "fences")?.split_whitespace() {
        checkpoint.pending_fences.insert(NodeId(parse_id(value)?));
    }
    Ok(checkpoint)
}

fn field<'a>(line: Option<&'a str>, name: &str) -> Result<&'a str, String> {
    let line = line.ok_or_else(|| format!("the `{name}` line is missing"))?;
    let rest = line
        .strip_prefix(name)
        .ok_or_else(|| format!("expected the `{name}` line, found `{line}`"))?;
    Ok(rest.trim())
}

fn parse_id(value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| format!("`{value}` is not a node id"))
}
