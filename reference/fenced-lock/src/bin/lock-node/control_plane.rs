//! The peer-control-plane checkpoint, made durable beside this replica's log.
//!
//! Rafter opens no files, so a driver's peer control plane is a value its
//! embedder persists. This is that embedder. The facts it carries — which
//! `NodeId`s a committed removal has spent, how far identity allocation has got,
//! and which fences the link layer has not accepted — cannot be rebuilt from the
//! Raft log: retirement is the *difference* between two committed
//! configurations, a restarted process sees only the latest one, and compaction
//! erases the rest. A process that dropped them would stop retrying a refused
//! fence and would let a spent identity be allocated again.
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
//! # Format
//!
//! One line per fact, so a reader can see what a replica retired, sealed the way
//! the [lock store](rafter_reference_fenced_lock::store) seals a slot. The
//! conventions are that module's: the checksum is CRC-32/IEEE over the canonical
//! bytes and is an **accidental-corruption check, not an authentication tag**,
//! and a tag other than the one named here refuses the file rather than being
//! quietly reinterpreted.
//!
//! ```text
//! rafter-lock-control-plane 2
//! group      <u64>
//! high_water <u64> | -
//! live       <u64>*
//! fences     <u64>*
//! crc32      <8 hex digits>
//! ```
//!
//! The checksum covers every byte before the `crc32` line, which is the whole of
//! the canonical encoding, and **nothing may follow it**: a file with trailing
//! bytes is refused rather than truncated to the part that verified. A checksum
//! placed at the end and then ignored past would let an append go unnoticed, and
//! an artifact whose reader stops at the first complete record cannot tell a
//! finished write from a partial overwrite of a longer one.
//!
//! `group` binds the file to the group it describes. Retirement is per
//! `(group_id, NodeId)` pair, so this file means nothing for another group; a
//! process that came to host several replicas and crossed two files would
//! otherwise raise one group's mark past identities it never committed.
//!
//! # What a syntactically valid corruption must not do
//!
//! The three properties this file exists to protect, in the order they cost:
//! **the mark must not fall**, **a live identity must not appear**, and **an
//! active member must not be fenced**. A flipped bit inside a number is
//! syntactically valid and does all three, which is why a version tag and a
//! parser were not enough. The checksum catches the flipped bit; the
//! [invariant checks](decode) catch a record that is internally contradictory;
//! and the driver refuses the same contradictions again at
//! [`PeerControlPlaneCheckpoint`], because a value that reached it by another
//! route deserves the same gate.
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
//! # An absent file is a first boot; a damaged one is not repairable here
//!
//! An absent file reads as an empty checkpoint, which is the honest description
//! of a replica that has retired nothing yet.
//!
//! A file that exists and does not verify is a **refusal to open**, and the
//! process fails closed on it. It is deliberately not offered to the store's
//! `NEEDS_REPAIR` path, and it is deliberately not regenerated: there is nothing
//! to repair *from*. The lock store can refuse one damaged slot and adopt its
//! partner because a second copy of the same image exists; this file has one
//! copy, and the state it holds is by construction the state no other artifact
//! carries. Regenerating an empty one would succeed, start the replica, and
//! **silently forget** every retirement this replica witnessed — the exact
//! outcome the file exists to prevent, reached by a path an operator did not
//! choose. So the honest default is refusal, and recovery is an operator
//! decision made with the deployment's own record of what was retired.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rafter::NodeId;
use rafter_reference_fenced_lock::store::crc32;
use rafter_service::PeerControlPlaneCheckpoint;

use super::replica::{LockGroupId, GROUP_ID};

/// The file name inside a replica's directory.
const CHECKPOINT_FILE: &str = "control-plane";

/// The format tag, so a later shape is a refusal rather than a misreading.
const FORMAT_TAG: &str = "rafter-lock-control-plane 2";

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
    /// This covers a wrong tag, a missing or malformed field, a checksum that
    /// does not match, trailing bytes after the checksum, a record for another
    /// group, and a record whose own facts contradict each other.
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
/// Returns an error when the file exists and cannot be read, verified, or
/// interpreted. Every such refusal is fatal by policy; see the module header for
/// why this artifact is neither repairable nor regenerable.
pub fn load(node_dir: &Path) -> Result<PeerControlPlaneCheckpoint<LockGroupId>, CheckpointError> {
    let path = node_dir.join(CHECKPOINT_FILE);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            return Ok(PeerControlPlaneCheckpoint::empty(GROUP_ID));
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
    checkpoint: &PeerControlPlaneCheckpoint<LockGroupId>,
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

/// One line per fact, sealed with a checksum over everything before it.
fn encode(checkpoint: &PeerControlPlaneCheckpoint<LockGroupId>) -> String {
    let body = encode_body(checkpoint);
    format!("{body}crc32 {:08x}\n", crc32(body.as_bytes()))
}

/// The canonical bytes the checksum covers: everything but the `crc32` line.
fn encode_body(checkpoint: &PeerControlPlaneCheckpoint<LockGroupId>) -> String {
    let mut text = String::from(FORMAT_TAG);
    text.push_str("\ngroup ");
    text.push_str(&checkpoint.group.0.to_string());
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

/// Verifies and interprets one file, in that order.
///
/// The checksum is checked before any field is believed, because a field read
/// out of unverified bytes is a field that can lower the mark. The invariant
/// checks after it are the same ones the driver applies at restore, made here so
/// the refusal names the *file* — an operator looking at a replica that will not
/// start needs to know which artifact disagrees, and a driver error one layer up
/// names only the value.
fn decode(text: &str) -> Result<PeerControlPlaneCheckpoint<LockGroupId>, String> {
    const CHECKSUM_LINE: &str = "\ncrc32 ";
    let separator = text
        .rfind(CHECKSUM_LINE)
        .ok_or_else(|| "the `crc32` line is missing".to_owned())?;
    let body = &text[..=separator];
    let trailer = &text[separator + 1..];
    let Some(rest) = trailer.strip_prefix("crc32 ") else {
        unreachable!("the separator was found by this prefix")
    };
    let (digits, after) = rest
        .split_once('\n')
        .ok_or_else(|| "the `crc32` line is not terminated".to_owned())?;
    if !after.is_empty() {
        return Err(format!(
            "{} trailing bytes follow the checksum",
            after.len()
        ));
    }
    let recorded = u32::from_str_radix(digits.trim(), 16)
        .map_err(|_| format!("`{digits}` is not a checksum"))?;
    let computed = crc32(body.as_bytes());
    if recorded != computed {
        return Err(format!(
            "checksum mismatch: the file records {recorded:08x} and its bytes compute {computed:08x}"
        ));
    }

    let mut lines = body.lines();
    let tag = lines.next().ok_or_else(|| "the file is empty".to_owned())?;
    if tag != FORMAT_TAG {
        return Err(format!("expected `{FORMAT_TAG}`, found `{tag}`"));
    }

    let group = LockGroupId(parse_id(field(lines.next(), "group")?)?);
    if group != GROUP_ID {
        return Err(format!(
            "the file describes group {} and this replica serves {}",
            group.0, GROUP_ID.0
        ));
    }

    let mut checkpoint = PeerControlPlaneCheckpoint::empty(group);
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
    if lines.next().is_some() {
        return Err("unexpected lines before the checksum".to_owned());
    }
    check_invariants(&checkpoint)?;
    Ok(checkpoint)
}

/// Refuses a record whose own facts contradict each other.
///
/// Each clause holds by construction for a record this process wrote, so each
/// failure means the bytes were damaged in a way the checksum happened not to
/// catch, or that a hand-edited file is being offered. Each one moves a
/// retirement record in the unsafe direction.
fn check_invariants(checkpoint: &PeerControlPlaneCheckpoint<LockGroupId>) -> Result<(), String> {
    for node_id in &checkpoint.live_committed_members {
        let Some(mark) = checkpoint.committed_id_high_water else {
            return Err(format!(
                "live member {node_id} is recorded with no high-water mark"
            ));
        };
        if *node_id > mark {
            return Err(format!(
                "live member {node_id} is above the high-water mark {mark}"
            ));
        }
    }
    if let Some(node_id) = checkpoint
        .pending_fences
        .intersection(&checkpoint.live_committed_members)
        .next()
    {
        return Err(format!("{node_id} is fenced and also a live member"));
    }
    Ok(())
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

#[cfg(test)]
mod tests;
