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
//! rafter-lock-control-plane 4
//! group      <u64>
//! high_water <u64> | -
//! live       <u64>*
//! fences     <u64>*
//! crossings  <u64> | -
//! endpoint   <u64> | -
//! crc32      <8 hex digits>
//! ```
//!
//! # Version 4, and why an old file is refused rather than migrated
//!
//! `crossings` and `endpoint` are the driver's two consumer offsets into the
//! committed configuration stream. They arrived with the fields of the same
//! meaning on [`PeerControlPlaneCheckpoint`], replacing the single `through` of
//! version 3, and they are the reason the tag moved from 3 to 4.
//!
//! **They are two fields because one number could not say what it was evidence
//! of.** A crossing is a configuration entry the commit index crossed, carrying
//! that entry's own index, so consuming it really covers that index. An endpoint
//! is the committed membership a runtime holds, read at its commit index, for a
//! move with no entry behind it — and it covers nothing beneath itself. A
//! replica that recovers from a snapshot at commit 10 honestly records an
//! endpoint there and knows nothing about what committed and was superseded
//! below the boundary. Under one offset that record suppressed another
//! process's real crossings at 6 and 7, so an identity a committed removal spent
//! was never spent and its fence was never owed.
//!
//! **A version-3 file is refused, and refusing is the honest option rather than
//! the lazy one.** There is no derivable value for the split: a version-3
//! `through` cannot say which meaning it carried, because the format that wrote
//! it did not distinguish them. Both migrations are lies and both are dangerous.
//! Reading it as a crossing offset claims history coverage the record may never
//! have had, and skips the crossings a recovery replays beneath it — the exact
//! failure the split closes. Reading it as an endpoint offset leaves the
//! crossing offset absent, which the invariant below refuses outright for a
//! record that retired something.
//!
//! Supplying `-` for both is the same lie version 2 offered, in the other
//! direction: with no offsets, the next recovery replays every configuration
//! entry above the applied floor against a live set that already reflects them,
//! and each one reads as a removal of everything the entries above it added. It
//! would fence live members on the first restart after the upgrade.
//!
//! Rafter is pre-release and this composition is an example rather than a
//! deployment, so the cost of refusing is that an operator deletes a file from a
//! scratch cluster, and the cost of guessing is a fenced replica in the one
//! artifact whose whole purpose is to not lose retirement facts. A refusal an
//! operator reads beats a silent wrong answer.
//!
//! **The version tag refuses the shape and [`check_invariants`] refuses the
//! semantics**, and both are needed because they catch it arriving by different
//! routes. The tag turns away a version-3 *file*; nothing about a tag stops a
//! well-formed version-4 file whose `endpoint` line has been flipped to `-`, and
//! that record is a migration this module declined to perform, written by hand.
//! So the coupling is checked as a record invariant here, and again at
//! [`PeerControlPlaneCheckpoint`] for a value that reached the driver by another
//! route. Until the driver carried the same clause, it accepted the exact
//! semantic shape this file's version gate exists to refuse.
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
//! # An absent file is a first boot only when the replica really is one
//!
//! An absent file reads as an empty checkpoint **only for a replica whose
//! durable Raft state proves it has committed nothing**. That qualification is
//! the whole of the rule, and without it the absence of this file was
//! indistinguishable from a first boot on a replica that had been running for
//! months — so deleting one file downgraded a replica to "nothing was ever
//! retired here" and the process started cheerfully.
//!
//! The evidence is the recovered runtime's commit index, taken at
//! [`load`]'s call site, and it is the right question rather than a convenient
//! one: retirement is derived from *committed configurations*, so a replica
//! whose commit index is zero has committed no configuration, retired no
//! identity, and owes no fence. An empty checkpoint is not a guess for it, it is
//! the same value the driver would derive anyway.
//!
//! Probing the filesystem instead — "does `raft/` exist yet?" — was considered
//! and is wrong for this composition's layout. [`super::replica::Replica::open`]
//! creates `raft/` and `app/` and opens both stores *before* it writes this
//! file, so a genuine first boot that crashed in its own opening sequence leaves
//! the directories behind with no checkpoint. A directory probe would refuse to
//! start that replica forever, for a retirement record that never existed.
//!
//! This is the store's [`MissingSlot`](rafter_reference_fenced_lock::store::LockStoreError)
//! posture, in this artifact's own terms: something that should exist and does
//! not is unreadable rather than absent, and the process refuses with a reason an
//! operator can act on. A missing checkpoint forgot facts no other artifact
//! holds, so it is at least as serious as a missing slot — which has a partner
//! copy and this does not.
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

use rafter::{LogIndex, NodeId};
use rafter_reference_fenced_lock::store::crc32;
use rafter_service::PeerControlPlaneCheckpoint;

use super::replica::{LockGroupId, GROUP_ID};

/// The file name inside a replica's directory.
const CHECKPOINT_FILE: &str = "control-plane";

/// The format tag, so a later shape is a refusal rather than a misreading.
///
/// Version 4 split `through` into `crossings` and `endpoint`. A version-3 file
/// is refused rather than migrated; the module header says why neither reading
/// of the old field is honest.
const FORMAT_TAG: &str = "rafter-lock-control-plane 4";

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
    /// The file is absent from a replica whose durable Raft state says it has
    /// run before.
    ///
    /// Distinct from [`CheckpointError::Malformed`] because the operator's
    /// question is different: a damaged file is a corruption, and this is a
    /// *deletion*. It is fatal on the same terms — there is no second copy and
    /// no way to re-derive what it held — and it is separated so the refusal can
    /// say which of the two happened.
    Missing {
        path: PathBuf,
        commit_index: LogIndex,
    },
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
            Self::Missing { path, commit_index } => write!(
                formatter,
                "the control-plane checkpoint at {} is missing, and this replica has \
                 committed through index {} — so it retired identities whose record \
                 no other artifact holds; restore the file or reseed this replica \
                 deliberately",
                path.display(),
                commit_index.0
            ),
        }
    }
}

impl std::error::Error for CheckpointError {}

/// Returns the checkpoint this replica last made durable.
///
/// `commit_index` is the recovered runtime's durable commit floor, and it is the
/// evidence that decides what an *absent* file means. Zero is a replica that has
/// committed no configuration, so it has retired no identity and owes no fence:
/// an empty checkpoint is not a guess for it but the same value the driver
/// derives on its own. Anything above zero is a replica that has run, and an
/// absent file there is a deleted artifact rather than a first boot — refused,
/// because there is no second copy and nothing else on disk records what it
/// held. The module header covers why a directory probe cannot answer this and
/// the commit floor can.
///
/// # Errors
///
/// Returns [`CheckpointError::Missing`] when the file is absent from a replica
/// that has committed something, and an error when the file exists and cannot be
/// read, verified, or interpreted. Every such refusal is fatal by policy; see the
/// module header for why this artifact is neither repairable nor regenerable.
pub fn load(
    node_dir: &Path,
    commit_index: LogIndex,
) -> Result<PeerControlPlaneCheckpoint<LockGroupId>, CheckpointError> {
    let path = node_dir.join(CHECKPOINT_FILE);
    let text = match fs::read_to_string(&path) {
        Ok(text) => text,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            if commit_index > LogIndex::ZERO {
                return Err(CheckpointError::Missing { path, commit_index });
            }
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
    text.push_str("\ncrossings ");
    match checkpoint.committed_crossings_through {
        Some(index) => text.push_str(&index.0.to_string()),
        None => text.push('-'),
    }
    text.push_str("\nendpoint ");
    match checkpoint.committed_endpoint_through {
        Some(index) => text.push_str(&index.0.to_string()),
        None => text.push('-'),
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
    checkpoint.committed_crossings_through = position(field(lines.next(), "crossings")?)?;
    checkpoint.committed_endpoint_through = position(field(lines.next(), "endpoint")?)?;
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
    // **The offsets and the retirement record are one record, and the coupling
    // is not symmetric.** A file carrying one without the other is the same
    // semantic damage the version gate above refuses a version-3 file for,
    // arriving inside a well-formed version-4 file — a flipped `endpoint` line
    // is syntactically valid and does exactly this. Checked first, because it is
    // the clause that decides whether the fields below describe a replica that
    // has read any history at all.
    //
    // The asymmetry is the driver's, mirrored: every fold raises the mark, and
    // every driver that folded anything was constructed or adopted — and both
    // publish an endpoint observation unconditionally. So retirement state
    // implies the *endpoint* offset specifically, while the crossing offset is
    // legitimately absent on a replica that recovered from a snapshot or has
    // only ever seen its own opening configuration. Writing this as "at least
    // one offset" would accept a crossing offset standing alone with a
    // retirement record, which no replica produces and whose absorption leaves
    // the endpoint fold ungated on the next open.
    let retired_something = checkpoint.committed_id_high_water.is_some()
        || !checkpoint.live_committed_members.is_empty()
        || !checkpoint.pending_fences.is_empty();
    match (checkpoint.committed_endpoint_through, retired_something) {
        (None, true) => {
            return Err(String::from(
                "the record says what it retired and not where it last observed the \
                 committed configuration, so a recovery would re-fold the runtime's \
                 endpoint against it and fence the replicas a rebuilt runtime has \
                 not caught up to",
            ))
        }
        (Some(index), false) => {
            return Err(format!(
                "the record says it observed the committed configuration at index {} \
                 and names nothing it retired, so a recovery would skip that \
                 observation with no record of what it spent",
                index.0
            ))
        }
        (None, false) | (Some(_), true) => {}
    }
    if let Some(index) = checkpoint.committed_crossings_through {
        if !retired_something {
            return Err(format!(
                "the record says it read the crossing history through index {} and \
                 names nothing it retired, so a recovery would skip that history with \
                 no record of what it spent",
                index.0
            ));
        }
    }
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
    for node_id in &checkpoint.pending_fences {
        if checkpoint.live_committed_members.contains(node_id) {
            return Err(format!("{node_id} is fenced and also a live member"));
        }
        // Every fence has to sit at or below the mark, because a fence is the
        // residue of a committed removal and a removal can only have spent an
        // identity some committed configuration named. An identity *above* the
        // mark was in no configuration this record saw, so this record cannot
        // have watched it leave one — and a driver that absorbed the obligation
        // anyway would raise its mark past a replica another record calls live,
        // publish that replica, and then fence it forever.
        match checkpoint.committed_id_high_water {
            Some(mark) if *node_id <= mark => {}
            Some(mark) => {
                return Err(format!(
                    "{node_id} is fenced and sits above the high-water mark {mark}, \
                     so no committed removal here spent it"
                ))
            }
            None => {
                return Err(format!(
                    "{node_id} is fenced with no high-water mark, so no committed \
                     configuration here ever named it"
                ))
            }
        }
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

/// One consumer offset, where `-` is "nothing of this kind consumed".
///
/// `-` rather than `0`, because `LogIndex(0)` is a real position — the index
/// before any entry — and an absent offset has to be distinguishable from one
/// that has read through the bottom of the log.
fn position(value: &str) -> Result<Option<LogIndex>, String> {
    match value {
        "-" => Ok(None),
        value => Ok(Some(LogIndex(parse_id(value)?))),
    }
}

#[cfg(test)]
mod tests;
