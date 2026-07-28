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
//! rafter-lock-control-plane 5
//! group      <u64>
//! high_water <u64> | -
//! through    <u64> | -
//! live       <u64>*
//! fences     <u64>*
//! crc32      <8 hex digits>
//! ```
//!
//! # Version 5, and why a version-4 file is refused rather than migrated
//!
//! `through` and `live` are one fact written on two lines: the committed
//! membership this replica believes is current, and the log position it was
//! observed at. They replace version 4's `live` beside its two consumer offsets,
//! and they are the reason the tag moved from 4 to 5.
//!
//! **The offsets are gone rather than merged.** They existed so a replayed
//! configuration could be skipped as already-folded, and the driver no longer
//! needs that: a committed configuration reaches it as a *transition*, so its
//! removal set is the same wherever it is folded and re-folding one changes
//! nothing. What is left is the question the offsets were never answering —
//! which of two observations of the current membership is the later — and that
//! needs the position to travel *with* the membership rather than beside it.
//!
//! **A version-4 file is refused, and the reason is the same shape as version
//! 3's: the old field's provenance is unknowable.** It is worth stating
//! carefully, because the mapping looks obvious and is not. Version 4's `live`
//! was assigned by a fold of *either* kind while only that kind's own offset
//! advanced, so neither offset dates it:
//!
//! * `through = endpoint` understates it. A driver publishes an endpoint at
//!   construction and then follows the cluster through crossings, which move
//!   `live` and leave `endpoint` where it was — the ordinary case, not an exotic
//!   one. A record whose `live` came from index 7 and whose `endpoint` reads 5
//!   would be migrated to position 5, and the next runtime observation at 6
//!   would then outrank it and read everything index 7 had added as removed.
//! * `through = max(crossings, endpoint)` overstates it. An endpoint at 10
//!   beside a later-folded crossing at 7 leaves `live` from index 7 under a
//!   maximum of 10, and a genuine observation at 9 is then treated as older —
//!   so everything committed between 7 and 9 reads as removed instead.
//!
//! Both readings fence live replicas, in opposite directions, and there is no
//! third: the record simply does not say where its membership was observed. That
//! is the defect the versioned register exists to fix, so a file written before
//! the fix cannot describe the state that fixes it.
//!
//! Supplying `-` is not available either. A record with no current state and a
//! mark beside it is the shape the invariant below refuses outright, because a
//! mark read against no membership spends every identity at or below it.
//!
//! Rafter is pre-release and this composition is an example rather than a
//! deployment, so the cost of refusing is that an operator deletes a file from a
//! scratch cluster, and the cost of guessing is a fenced replica in the one
//! artifact whose whole purpose is to not lose retirement facts. A refusal an
//! operator reads beats a silent wrong answer.
//!
//! **The version tag refuses the shape and [`check_invariants`] refuses the
//! semantics**, and both are needed because they catch it arriving by different
//! routes. The tag turns away a version-4 *file*; nothing about a tag stops a
//! well-formed version-5 file whose `through` line has been flipped to `-`, and
//! that record is a migration this module declined to perform, written by hand.
//! So the coupling is checked as a record invariant here, and again at
//! [`PeerControlPlaneCheckpoint`] for a value that reached the driver by another
//! route.
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
    collections::BTreeSet,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rafter::{LogIndex, NodeId};
use rafter_reference_fenced_lock::store::crc32;
use rafter_service::{CurrentCommittedState, PeerControlPlaneCheckpoint};

use super::replica::{LockGroupId, GROUP_ID};

/// The file name inside a replica's directory.
const CHECKPOINT_FILE: &str = "control-plane";

/// The format tag, so a later shape is a refusal rather than a misreading.
///
/// Version 5 replaced version 4's `live` and its two consumer offsets with one
/// positioned current state. A version-4 file is refused rather than migrated;
/// the module header says why no reading of the old fields dates the membership
/// they carry.
const FORMAT_TAG: &str = "rafter-lock-control-plane 5";

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
    // The position and the membership are one fact on two lines, and the
    // position is written first so a reader meets it before the set it dates.
    text.push_str("\nthrough ");
    match checkpoint.current_committed.as_ref() {
        Some(current) => text.push_str(&current.through.0.to_string()),
        None => text.push('-'),
    }
    text.push_str("\nlive");
    if let Some(current) = checkpoint.current_committed.as_ref() {
        for node_id in &current.membership {
            text.push(' ');
            text.push_str(&node_id.0.to_string());
        }
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
    let through = position(field(lines.next(), "through")?)?;
    let mut membership = BTreeSet::new();
    for value in field(lines.next(), "live")?.split_whitespace() {
        membership.insert(NodeId(parse_id(value)?));
    }
    // A membership with no position is not half a fact, it is a fact this format
    // cannot represent: the two lines are one value, and a reader that accepted
    // the set alone would have to invent a position to date it by.
    match through {
        Some(through) => {
            checkpoint.current_committed = Some(CurrentCommittedState::new(through, membership));
        }
        None if membership.is_empty() => {}
        None => {
            return Err(String::from(
                "the record names live members and no position to date them, so \
                 nothing says which of two observations of the committed \
                 membership this one is",
            ))
        }
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
    // **The current state and the retirement record are one record, and the
    // coupling is a biconditional.** A file carrying one without the other is
    // the same semantic damage the version gate above refuses a version-4 file
    // for, arriving inside a well-formed version-5 file — a flipped `through`
    // line is syntactically valid and does exactly this. Checked first, because
    // it is the clause that decides whether the fields below describe a replica
    // that has observed anything at all.
    //
    // The driver's derivation, mirrored: every observation of a committed
    // configuration raises the mark to at least its greatest identity, and
    // assigns the current state in the same call because a first observation is
    // always the latest one a record has. Neither can stand alone.
    let retired_something =
        checkpoint.committed_id_high_water.is_some() || !checkpoint.pending_fences.is_empty();
    let live = checkpoint
        .current_committed
        .as_ref()
        .map(|current| &current.membership);
    match (checkpoint.current_committed.as_ref(), retired_something) {
        (None, true) => {
            return Err(String::from(
                "the record says what it retired and names no committed membership to \
                 read it against, so every identity at or below its mark would be \
                 spent and the replica would refuse the whole cluster",
            ))
        }
        (Some(current), false) => {
            return Err(format!(
                "the record says it observed the committed configuration at index {} \
                 and names no high-water mark, so the observation that produced it \
                 has been lost along with what it spent",
                current.through.0
            ))
        }
        (None, false) | (Some(_), true) => {}
    }
    for node_id in live.into_iter().flatten() {
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
        if live.is_some_and(|live| live.contains(node_id)) {
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
