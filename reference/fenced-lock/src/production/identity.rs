//! Durable, monotonic replica identity allocation for the production fixture.
//!
//! A `(group, node)` identity is single-use. The allocator therefore publishes
//! the per-group high-water mark before it publishes the new replica record. A
//! crash in between spends one number and creates no replica, which is safe; the
//! opposite order could expose a replica whose allocation disappears after a
//! crash and permit the same ID to be issued again.
//!
//! Calls are deliberately serialized by a create-new lock file. The fixture has
//! one deployment controller and never contends legitimately. A lock left by a
//! crashed controller is a typed refusal requiring operator inspection, not a
//! lease guessed from wall-clock time.

use std::{
    error::Error,
    fmt, fs,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rafter::NodeId;

use crate::store::crc32;

use super::replay::initialize_transport_state;

const ALLOCATION_FILE: &str = "identity-allocation";
const ALLOCATION_LOCK: &str = "identity-allocation.lock";
const IDENTITY_FILE: &str = "replica-identity";
const ALLOCATION_TAG: &str = "rafter-lock-identity-allocation 1";
const IDENTITY_TAG: &str = "rafter-lock-replica-identity 1";

/// A deterministic publication seam used only by crash-window acceptance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AllocationCrashPoint {
    /// Publish both the allocation and replica identity.
    None,
    /// Stop before the new high-water mark is renamed into place.
    BeforeHighWaterPublication,
    /// Stop after the high-water mark is durable but before the identity exists.
    AfterHighWaterPublication,
}

/// One durable replica identity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicaIdentity {
    /// The Raft group this identity belongs to.
    pub group_id: u64,
    /// The single-use node identity.
    pub node_id: NodeId,
    /// Whether committed removal has permanently retired the identity.
    pub retired: bool,
}

impl ReplicaIdentity {
    /// Path to this replica's caller-owned identity record.
    #[must_use]
    pub fn path(allocation_dir: &Path, node_id: NodeId) -> PathBuf {
        allocation_dir
            .join(format!("node-{}", node_id.0))
            .join(IDENTITY_FILE)
    }
}

/// Why identity metadata could not be trusted or published.
#[derive(Debug)]
pub enum IdentityError {
    /// A filesystem operation failed.
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    /// A durable record is absent where the caller required one.
    Missing { path: PathBuf },
    /// A record is truncated, corrupt, foreign, or internally inconsistent.
    Malformed { path: PathBuf, detail: String },
    /// Another allocator owns the single-writer publication lock, or a crashed
    /// allocator left it for an operator to inspect.
    AllocationLocked { path: PathBuf },
    /// A crash-window seam stopped publication at the named safe boundary.
    InjectedCrash(AllocationCrashPoint),
    /// The identity was permanently retired after committed removal.
    Retired { node_id: NodeId },
    /// Allocation exhausted the `NodeId` namespace.
    Exhausted,
}

impl fmt::Display for IdentityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} identity metadata at {}: {source}",
                path.display()
            ),
            Self::Missing { path } => {
                write!(
                    formatter,
                    "required identity metadata is missing at {}",
                    path.display()
                )
            }
            Self::Malformed { path, detail } => write!(
                formatter,
                "identity metadata at {} is refused: {detail}",
                path.display()
            ),
            Self::AllocationLocked { path } => write!(
                formatter,
                "identity allocation is locked at {}; inspect the prior allocator before retrying",
                path.display()
            ),
            Self::InjectedCrash(point) => {
                write!(
                    formatter,
                    "injected identity publication crash at {point:?}"
                )
            }
            Self::Retired { node_id } => write!(
                formatter,
                "replica identity {} was permanently retired",
                node_id.0
            ),
            Self::Exhausted => formatter.write_str("replica identity allocation is exhausted"),
        }
    }
}

impl Error for IdentityError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Allocates and publishes one fresh identity.
///
/// The caller serializes deployment changes by calling this function. The
/// create-new lock makes accidental concurrency and a crashed prior allocator
/// fail closed.
///
/// # Errors
///
/// Returns [`IdentityError`] for corrupt metadata, concurrent allocation,
/// exhaustion, injected crash boundaries, or filesystem failure.
pub fn allocate_replica(
    allocation_dir: &Path,
    group_id: u64,
    crash: AllocationCrashPoint,
) -> Result<ReplicaIdentity, IdentityError> {
    fs::create_dir_all(allocation_dir)
        .map_err(|source| io("create the allocation directory", allocation_dir, source))?;
    let lock_path = allocation_dir.join(ALLOCATION_LOCK);
    let lock = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| {
            if source.kind() == std::io::ErrorKind::AlreadyExists {
                IdentityError::AllocationLocked {
                    path: lock_path.clone(),
                }
            } else {
                io("create the allocation lock", &lock_path, source)
            }
        })?;
    let guard = AllocationGuard {
        path: lock_path,
        _file: lock,
    };

    let allocation_path = allocation_dir.join(ALLOCATION_FILE);
    let current = read_allocation(&allocation_path, group_id)?;
    let next = current
        .map_or(Some(NodeId(1)), |node_id| {
            node_id.0.checked_add(1).map(NodeId)
        })
        .ok_or(IdentityError::Exhausted)?;
    let allocation = encode_allocation(group_id, next);
    publish(
        &allocation_path,
        allocation.as_bytes(),
        crash == AllocationCrashPoint::BeforeHighWaterPublication,
    )?;
    if crash == AllocationCrashPoint::BeforeHighWaterPublication {
        return Err(IdentityError::InjectedCrash(crash));
    }
    if crash == AllocationCrashPoint::AfterHighWaterPublication {
        return Err(IdentityError::InjectedCrash(crash));
    }

    let identity = ReplicaIdentity {
        group_id,
        node_id: next,
        retired: false,
    };
    let parent = allocation_dir.join(format!("node-{}", next.0));
    let identity_path = parent.join(IDENTITY_FILE);
    fs::create_dir_all(&parent)
        .map_err(|source| io("create the replica identity directory", &parent, source))?;
    initialize_transport_state(&parent, group_id).map_err(|error| IdentityError::Malformed {
        path: parent.join("transport-replay"),
        detail: error.to_string(),
    })?;
    publish(&identity_path, encode_identity(identity).as_bytes(), false)?;
    drop(guard);
    Ok(identity)
}

/// Loads the allocation high-water mark for one group.
///
/// # Errors
///
/// Returns [`IdentityError`] when the record is absent, corrupt, or foreign.
pub fn load_allocation_high_water(
    allocation_dir: &Path,
    group_id: u64,
) -> Result<NodeId, IdentityError> {
    let path = allocation_dir.join(ALLOCATION_FILE);
    read_allocation(&path, group_id)?.ok_or(IdentityError::Missing { path })
}

/// Loads an active replica identity and cross-checks it against the allocator.
///
/// # Errors
///
/// Returns [`IdentityError`] when either record is missing or malformed, the
/// identity belongs to another group, its ID is above the durable allocation
/// high-water mark, or committed removal retired it.
pub fn load_active_replica(
    allocation_dir: &Path,
    identity_path: &Path,
    expected_group: u64,
) -> Result<ReplicaIdentity, IdentityError> {
    let identity = read_identity(identity_path, expected_group)?;
    if identity.retired {
        return Err(IdentityError::Retired {
            node_id: identity.node_id,
        });
    }
    let high_water = load_allocation_high_water(allocation_dir, expected_group)?;
    if identity.node_id > high_water {
        return Err(IdentityError::Malformed {
            path: identity_path.to_path_buf(),
            detail: format!(
                "node {} is above allocation high-water {}",
                identity.node_id.0, high_water.0
            ),
        });
    }
    Ok(identity)
}

/// Permanently retires one identity after its removal is known committed.
///
/// # Errors
///
/// Returns [`IdentityError`] when the record cannot be read or published.
pub fn retire_replica(
    identity_path: &Path,
    expected_group: u64,
) -> Result<ReplicaIdentity, IdentityError> {
    let mut identity = read_identity(identity_path, expected_group)?;
    identity.retired = true;
    publish(identity_path, encode_identity(identity).as_bytes(), false)?;
    Ok(identity)
}

fn read_allocation(path: &Path, expected_group: u64) -> Result<Option<NodeId>, IdentityError> {
    let bytes = match fs::read(path) {
        Ok(bytes) => bytes,
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(source) => return Err(io("read the allocation record", path, source)),
    };
    let fields = decode_fields(path, &bytes, ALLOCATION_TAG, &["group", "high_water"])?;
    let group = parse_u64(path, "group", &fields[0])?;
    if group != expected_group {
        return Err(foreign_group(path, expected_group, group));
    }
    Ok(Some(NodeId(parse_u64(path, "high_water", &fields[1])?)))
}

fn read_identity(path: &Path, expected_group: u64) -> Result<ReplicaIdentity, IdentityError> {
    let bytes = fs::read(path).map_err(|source| {
        if source.kind() == std::io::ErrorKind::NotFound {
            IdentityError::Missing {
                path: path.to_path_buf(),
            }
        } else {
            io("read the replica identity", path, source)
        }
    })?;
    let fields = decode_fields(path, &bytes, IDENTITY_TAG, &["group", "node", "state"])?;
    let group_id = parse_u64(path, "group", &fields[0])?;
    if group_id != expected_group {
        return Err(foreign_group(path, expected_group, group_id));
    }
    let node_id = NodeId(parse_u64(path, "node", &fields[1])?);
    let retired = match fields[2].as_str() {
        "active" => false,
        "retired" => true,
        state => {
            return Err(IdentityError::Malformed {
                path: path.to_path_buf(),
                detail: format!("state is {state:?}, not active or retired"),
            });
        }
    };
    Ok(ReplicaIdentity {
        group_id,
        node_id,
        retired,
    })
}

fn encode_allocation(group_id: u64, high_water: NodeId) -> String {
    encode(
        ALLOCATION_TAG,
        &[
            ("group", group_id.to_string()),
            ("high_water", high_water.0.to_string()),
        ],
    )
}

fn encode_identity(identity: ReplicaIdentity) -> String {
    encode(
        IDENTITY_TAG,
        &[
            ("group", identity.group_id.to_string()),
            ("node", identity.node_id.0.to_string()),
            (
                "state",
                if identity.retired {
                    "retired".to_owned()
                } else {
                    "active".to_owned()
                },
            ),
        ],
    )
}

fn encode(tag: &str, fields: &[(&str, String)]) -> String {
    let mut body = format!("{tag}\n");
    for (name, value) in fields {
        body.push_str(name);
        body.push(' ');
        body.push_str(value);
        body.push('\n');
    }
    let checksum = crc32(body.as_bytes());
    format!("{body}crc32 {checksum:08x}\n")
}

fn decode_fields(
    path: &Path,
    bytes: &[u8],
    expected_tag: &str,
    expected_names: &[&str],
) -> Result<Vec<String>, IdentityError> {
    let text = std::str::from_utf8(bytes).map_err(|error| malformed(path, error.to_string()))?;
    let mut lines = text.lines();
    if lines.next() != Some(expected_tag) {
        return Err(malformed(
            path,
            format!("expected format tag {expected_tag:?}"),
        ));
    }
    let mut values = Vec::with_capacity(expected_names.len());
    for expected in expected_names {
        let line = lines
            .next()
            .ok_or_else(|| malformed(path, format!("missing {expected} field")))?;
        let (name, value) = line
            .split_once(' ')
            .ok_or_else(|| malformed(path, format!("malformed {expected} field")))?;
        if name != *expected || value.is_empty() {
            return Err(malformed(path, format!("expected {expected} field")));
        }
        values.push(value.to_owned());
    }
    let checksum_line = lines
        .next()
        .ok_or_else(|| malformed(path, "missing crc32 field".to_owned()))?;
    if lines.next().is_some() || !text.ends_with('\n') {
        return Err(malformed(path, "trailing or unterminated bytes".to_owned()));
    }
    let encoded_checksum = checksum_line
        .strip_prefix("crc32 ")
        .ok_or_else(|| malformed(path, "malformed crc32 field".to_owned()))?;
    if encoded_checksum.len() != 8 {
        return Err(malformed(
            path,
            "crc32 must contain eight hex digits".to_owned(),
        ));
    }
    let expected_checksum = u32::from_str_radix(encoded_checksum, 16)
        .map_err(|_| malformed(path, "crc32 is not hexadecimal".to_owned()))?;
    let checksum_offset = text
        .rfind("crc32 ")
        .expect("the checksum line was parsed above");
    let actual_checksum = crc32(&bytes[..checksum_offset]);
    if actual_checksum != expected_checksum {
        return Err(malformed(
            path,
            format!(
                "crc32 mismatch: recorded {expected_checksum:08x}, computed {actual_checksum:08x}"
            ),
        ));
    }
    Ok(values)
}

fn parse_u64(path: &Path, name: &str, value: &str) -> Result<u64, IdentityError> {
    value
        .parse()
        .map_err(|_| malformed(path, format!("{name} is not a u64")))
}

fn foreign_group(path: &Path, expected: u64, observed: u64) -> IdentityError {
    malformed(
        path,
        format!("record belongs to group {observed}, expected group {expected}"),
    )
}

fn malformed(path: &Path, detail: String) -> IdentityError {
    IdentityError::Malformed {
        path: path.to_path_buf(),
        detail,
    }
}

fn io(operation: &'static str, path: &Path, source: std::io::Error) -> IdentityError {
    IdentityError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn publish(path: &Path, bytes: &[u8], stop_before_rename: bool) -> Result<(), IdentityError> {
    let parent = path.parent().ok_or_else(|| {
        malformed(
            path,
            "identity metadata path has no parent directory".to_owned(),
        )
    })?;
    let staged = parent.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("identity"),
        std::process::id()
    ));
    let mut file =
        File::create(&staged).map_err(|source| io("create the staged record", &staged, source))?;
    file.write_all(bytes)
        .map_err(|source| io("write the staged record", &staged, source))?;
    file.sync_all()
        .map_err(|source| io("sync the staged record", &staged, source))?;
    if stop_before_rename {
        return Ok(());
    }
    fs::rename(&staged, path).map_err(|source| io("publish the record", path, source))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io("sync the record directory", parent, source))
}

#[derive(Debug)]
struct AllocationGuard {
    path: PathBuf,
    _file: File,
}

impl Drop for AllocationGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rafter_reference_harness::process::ScratchSpace;

    use super::*;

    #[test]
    fn allocation_is_monotonic_and_retirement_is_permanent() {
        let scratch =
            ScratchSpace::create("production-identity", "monotonic").expect("scratch opens");
        let first = allocate_replica(scratch.path(), 7, AllocationCrashPoint::None)
            .expect("first identity allocates");
        let second = allocate_replica(scratch.path(), 7, AllocationCrashPoint::None)
            .expect("second identity allocates");
        assert_eq!((first.node_id, second.node_id), (NodeId(1), NodeId(2)));

        let path = ReplicaIdentity::path(scratch.path(), first.node_id);
        retire_replica(&path, 7).expect("identity retires");
        assert!(matches!(
            load_active_replica(scratch.path(), &path, 7),
            Err(IdentityError::Retired { node_id: NodeId(1) })
        ));
        let replacement = allocate_replica(scratch.path(), 7, AllocationCrashPoint::None)
            .expect("replacement uses a fresh identity");
        assert_eq!(replacement.node_id, NodeId(3));
    }

    #[test]
    fn crash_before_high_water_publication_reuses_nothing_that_was_published() {
        let scratch =
            ScratchSpace::create("production-identity", "before-publish").expect("scratch opens");
        assert!(matches!(
            allocate_replica(
                scratch.path(),
                1,
                AllocationCrashPoint::BeforeHighWaterPublication
            ),
            Err(IdentityError::InjectedCrash(
                AllocationCrashPoint::BeforeHighWaterPublication
            ))
        ));
        let first = allocate_replica(scratch.path(), 1, AllocationCrashPoint::None)
            .expect("no published allocation was spent");
        assert_eq!(first.node_id, NodeId(1));
    }

    #[test]
    fn crash_after_high_water_publication_spends_the_unpublished_identity() {
        let scratch =
            ScratchSpace::create("production-identity", "after-publish").expect("scratch opens");
        assert!(matches!(
            allocate_replica(
                scratch.path(),
                1,
                AllocationCrashPoint::AfterHighWaterPublication
            ),
            Err(IdentityError::InjectedCrash(
                AllocationCrashPoint::AfterHighWaterPublication
            ))
        ));
        let next = allocate_replica(scratch.path(), 1, AllocationCrashPoint::None)
            .expect("allocation resumes above the durable high-water mark");
        assert_eq!(next.node_id, NodeId(2));
        assert!(!ReplicaIdentity::path(scratch.path(), NodeId(1)).exists());
    }

    #[test]
    fn corrupt_truncated_and_foreign_identity_records_fail_closed() {
        let scratch =
            ScratchSpace::create("production-identity", "refusals").expect("scratch opens");
        let identity = allocate_replica(scratch.path(), 9, AllocationCrashPoint::None)
            .expect("identity allocates");
        let path = ReplicaIdentity::path(scratch.path(), identity.node_id);

        let original = fs::read(&path).expect("identity reads");
        fs::write(&path, &original[..original.len() / 2]).expect("identity truncates");
        assert!(matches!(
            load_active_replica(scratch.path(), &path, 9),
            Err(IdentityError::Malformed { .. })
        ));

        fs::write(&path, encode_identity(identity)).expect("identity restores");
        let mut corrupt = fs::read(&path).expect("identity reads");
        corrupt[0] ^= 1;
        fs::write(&path, corrupt).expect("identity corrupts");
        assert!(matches!(
            load_active_replica(scratch.path(), &path, 9),
            Err(IdentityError::Malformed { .. })
        ));

        fs::write(&path, encode_identity(identity)).expect("identity restores");
        assert!(matches!(
            load_active_replica(scratch.path(), &path, 10),
            Err(IdentityError::Malformed { detail, .. })
                if detail.contains("group 9") && detail.contains("group 10")
        ));
    }
}
