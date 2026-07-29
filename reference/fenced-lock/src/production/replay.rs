//! Durable replay-window state for the authenticated production link.
//!
//! TLS authenticates and encrypts a connection; it does not make an accepted
//! application frame single-use across reconnects. This store assigns every
//! outbound connection a durable, monotonic session and remembers a 64-frame
//! receive window per authenticated peer. Accepted receive state is published
//! before the frame reaches the Raft driver. A crash may therefore lose a frame
//! the peer must retransmit, but it cannot make an accepted frame fresh again.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt::{self, Write as _},
    fs,
    fs::File,
    io::Write as _,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use rafter::NodeId;

use crate::store::crc32;

const REPLAY_FILE: &str = "transport-replay";
const FORMAT_TAG: &str = "rafter-lock-transport-replay 1";

/// Number of sequence positions retained per authenticated peer session.
pub const REPLAY_WINDOW: u64 = 64;

/// What a durable replay-window check decided.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayDecision {
    /// The frame is fresh and its acceptance is durable.
    Accepted,
    /// This exact sequence was already accepted in the current session.
    Duplicate,
    /// A connection from an older durable session attempted to speak.
    StaleSession,
    /// The sequence fell behind the retained 64-frame window.
    OutsideWindow,
    /// Sequence zero is reserved and never accepted.
    InvalidSequence,
}

/// Why replay metadata could not be trusted or published.
#[derive(Debug)]
pub enum TransportReplayError {
    /// The durable file is absent.
    Missing { path: PathBuf },
    /// Filesystem publication failed.
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    /// The file is truncated, corrupt, foreign, or internally inconsistent.
    Malformed { path: PathBuf, detail: String },
    /// No fresh transport session remains.
    SessionExhausted,
}

impl fmt::Display for TransportReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { path } => write!(
                formatter,
                "required transport replay metadata is missing at {}",
                path.display()
            ),
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} transport replay metadata at {}: {source}",
                path.display()
            ),
            Self::Malformed { path, detail } => write!(
                formatter,
                "transport replay metadata at {} is refused: {detail}",
                path.display()
            ),
            Self::SessionExhausted => {
                formatter.write_str("durable transport session allocation is exhausted")
            }
        }
    }
}

impl Error for TransportReplayError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct PeerWindow {
    session: u64,
    maximum: u64,
    bitmap: u64,
}

#[derive(Debug)]
struct ReplayState {
    session_high_water: u64,
    inbound: BTreeMap<NodeId, PeerWindow>,
    failure: Option<String>,
}

/// Thread-safe durable session allocator and inbound replay window.
#[derive(Clone, Debug)]
pub struct TransportReplayStore {
    path: PathBuf,
    group_id: u64,
    state: Arc<Mutex<ReplayState>>,
}

impl TransportReplayStore {
    /// Opens and verifies replay metadata initialized with the replica identity.
    ///
    /// # Errors
    ///
    /// Returns [`TransportReplayError`] when the file is absent, corrupt, or
    /// belongs to another group.
    pub fn open(node_dir: &Path, expected_group: u64) -> Result<Self, TransportReplayError> {
        let path = node_dir.join(REPLAY_FILE);
        let bytes = fs::read(&path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                TransportReplayError::Missing { path: path.clone() }
            } else {
                io("read", &path, source)
            }
        })?;
        let state = decode(&path, expected_group, &bytes)?;
        Ok(Self {
            path,
            group_id: expected_group,
            state: Arc::new(Mutex::new(state)),
        })
    }

    /// Allocates and durably publishes a fresh outbound connection session.
    ///
    /// # Errors
    ///
    /// Returns [`TransportReplayError`] on exhaustion or publication failure.
    pub fn allocate_session(&self) -> Result<u64, TransportReplayError> {
        let mut state = lock(&self.state);
        if let Some(failure) = &state.failure {
            return Err(malformed(&self.path, failure.clone()));
        }
        let session = state
            .session_high_water
            .checked_add(1)
            .ok_or(TransportReplayError::SessionExhausted)?;
        state.session_high_water = session;
        self.publish_or_latch(&mut state)?;
        Ok(session)
    }

    /// Checks one authenticated frame and publishes acceptance before returning.
    ///
    /// `known_peer` is supplied by the certificate map. Refusing unknown peers
    /// before this call keeps the persisted map bounded by configured
    /// principals rather than attacker input.
    ///
    /// # Errors
    ///
    /// Returns [`TransportReplayError`] when an accepted update cannot be made
    /// durable. The first failure is latched for the process supervisor.
    pub fn admit(
        &self,
        known_peer: NodeId,
        session: u64,
        sequence: u64,
    ) -> Result<ReplayDecision, TransportReplayError> {
        if sequence == 0 {
            return Ok(ReplayDecision::InvalidSequence);
        }
        let mut state = lock(&self.state);
        if let Some(failure) = &state.failure {
            return Err(malformed(&self.path, failure.clone()));
        }
        let held = state.inbound.entry(known_peer).or_default();
        let decision = if session < held.session {
            ReplayDecision::StaleSession
        } else if session > held.session {
            *held = PeerWindow {
                session,
                maximum: sequence,
                bitmap: 1,
            };
            ReplayDecision::Accepted
        } else if sequence > held.maximum {
            let distance = sequence - held.maximum;
            held.bitmap = if distance >= REPLAY_WINDOW {
                1
            } else {
                (held.bitmap << distance) | 1
            };
            held.maximum = sequence;
            ReplayDecision::Accepted
        } else {
            let distance = held.maximum - sequence;
            if distance >= REPLAY_WINDOW {
                ReplayDecision::OutsideWindow
            } else {
                let bit = 1_u64 << distance;
                if held.bitmap & bit != 0 {
                    ReplayDecision::Duplicate
                } else {
                    held.bitmap |= bit;
                    ReplayDecision::Accepted
                }
            }
        };
        if decision == ReplayDecision::Accepted {
            self.publish_or_latch(&mut state)?;
        }
        Ok(decision)
    }

    /// Returns the first durable replay failure observed by a link thread.
    #[must_use]
    pub fn terminal_failure(&self) -> Option<String> {
        lock(&self.state).failure.clone()
    }

    /// Returns the number of authenticated peers occupying replay memory.
    #[must_use]
    pub fn peer_windows(&self) -> usize {
        lock(&self.state).inbound.len()
    }

    fn publish_or_latch(&self, state: &mut ReplayState) -> Result<(), TransportReplayError> {
        let encoded = encode(self.group_id, state);
        if let Err(error) = publish(&self.path, encoded.as_bytes()) {
            state.failure.get_or_insert_with(|| error.to_string());
            return Err(error);
        }
        Ok(())
    }
}

pub(super) fn initialize_transport_state(
    node_dir: &Path,
    group_id: u64,
) -> Result<(), TransportReplayError> {
    let path = node_dir.join(REPLAY_FILE);
    if path.exists() {
        return Err(malformed(
            &path,
            "new replica already has transport replay metadata".to_owned(),
        ));
    }
    let state = ReplayState {
        session_high_water: 0,
        inbound: BTreeMap::new(),
        failure: None,
    };
    publish(&path, encode(group_id, &state).as_bytes())
}

fn encode(group_id: u64, state: &ReplayState) -> String {
    let mut body = format!(
        "{FORMAT_TAG}\ngroup {group_id}\nsession_high_water {}\n",
        state.session_high_water
    );
    for (peer, window) in &state.inbound {
        writeln!(
            body,
            "peer {} {} {} {:016x}",
            peer.0, window.session, window.maximum, window.bitmap
        )
        .expect("writing to a String is infallible");
    }
    let checksum = crc32(body.as_bytes());
    format!("{body}crc32 {checksum:08x}\n")
}

fn decode(
    path: &Path,
    expected_group: u64,
    bytes: &[u8],
) -> Result<ReplayState, TransportReplayError> {
    let text = std::str::from_utf8(bytes).map_err(|error| malformed(path, error.to_string()))?;
    if !text.ends_with('\n') {
        return Err(malformed(path, "unterminated record".to_owned()));
    }
    let mut lines = text.lines();
    if lines.next() != Some(FORMAT_TAG) {
        return Err(malformed(path, "wrong format tag".to_owned()));
    }
    let group = parse_named_u64(path, lines.next(), "group")?;
    if group != expected_group {
        return Err(malformed(
            path,
            format!("record belongs to group {group}, expected {expected_group}"),
        ));
    }
    let session_high_water = parse_named_u64(path, lines.next(), "session_high_water")?;
    let mut inbound = BTreeMap::new();
    let mut checksum = None;
    for line in lines {
        if let Some(value) = line.strip_prefix("crc32 ") {
            if checksum.replace(value).is_some() {
                return Err(malformed(path, "duplicate crc32 field".to_owned()));
            }
            continue;
        }
        if checksum.is_some() {
            return Err(malformed(path, "bytes follow the crc32 field".to_owned()));
        }
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() != 5 || fields[0] != "peer" {
            return Err(malformed(path, format!("malformed peer line {line:?}")));
        }
        let peer = NodeId(parse_u64(path, "peer", fields[1])?);
        let window = PeerWindow {
            session: parse_u64(path, "session", fields[2])?,
            maximum: parse_u64(path, "maximum", fields[3])?,
            bitmap: u64::from_str_radix(fields[4], 16)
                .map_err(|_| malformed(path, "bitmap is not hexadecimal".to_owned()))?,
        };
        if window.session == 0 || window.maximum == 0 || window.bitmap == 0 {
            return Err(malformed(
                path,
                "persisted peer windows must be nonzero".to_owned(),
            ));
        }
        if inbound.insert(peer, window).is_some() {
            return Err(malformed(path, format!("duplicate peer {}", peer.0)));
        }
    }
    let checksum = checksum.ok_or_else(|| malformed(path, "missing crc32 field".to_owned()))?;
    if checksum.len() != 8 {
        return Err(malformed(
            path,
            "crc32 must contain eight hex digits".to_owned(),
        ));
    }
    let expected = u32::from_str_radix(checksum, 16)
        .map_err(|_| malformed(path, "crc32 is not hexadecimal".to_owned()))?;
    let checksum_offset = text
        .rfind("crc32 ")
        .expect("the checksum field was parsed above");
    let actual = crc32(&bytes[..checksum_offset]);
    if expected != actual {
        return Err(malformed(
            path,
            format!("crc32 mismatch: recorded {expected:08x}, computed {actual:08x}"),
        ));
    }
    Ok(ReplayState {
        session_high_water,
        inbound,
        failure: None,
    })
}

fn parse_named_u64(
    path: &Path,
    line: Option<&str>,
    expected: &str,
) -> Result<u64, TransportReplayError> {
    let line = line.ok_or_else(|| malformed(path, format!("missing {expected} field")))?;
    let (name, value) = line
        .split_once(' ')
        .ok_or_else(|| malformed(path, format!("malformed {expected} field")))?;
    if name != expected {
        return Err(malformed(path, format!("expected {expected} field")));
    }
    parse_u64(path, expected, value)
}

fn parse_u64(path: &Path, name: &str, value: &str) -> Result<u64, TransportReplayError> {
    value
        .parse()
        .map_err(|_| malformed(path, format!("{name} is not a u64")))
}

fn publish(path: &Path, bytes: &[u8]) -> Result<(), TransportReplayError> {
    let parent = path
        .parent()
        .ok_or_else(|| malformed(path, "transport replay path has no parent".to_owned()))?;
    let staged = parent.join(format!(".{REPLAY_FILE}.{}.tmp", std::process::id()));
    let mut file = File::create(&staged).map_err(|source| io("create staged", &staged, source))?;
    file.write_all(bytes)
        .map_err(|source| io("write staged", &staged, source))?;
    file.sync_all()
        .map_err(|source| io("sync staged", &staged, source))?;
    fs::rename(&staged, path).map_err(|source| io("publish", path, source))?;
    File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io("sync directory", parent, source))
}

fn malformed(path: &Path, detail: String) -> TransportReplayError {
    TransportReplayError::Malformed {
        path: path.to_path_buf(),
        detail,
    }
}

fn io(operation: &'static str, path: &Path, source: std::io::Error) -> TransportReplayError {
    TransportReplayError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}

fn lock<T>(state: &Mutex<T>) -> MutexGuard<'_, T> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use rafter_reference_harness::process::ScratchSpace;

    use super::*;

    fn store(label: &str) -> (ScratchSpace, TransportReplayStore) {
        let scratch =
            ScratchSpace::create("production-replay", label).expect("scratch directory opens");
        initialize_transport_state(scratch.path(), 1).expect("replay state initializes");
        let store = TransportReplayStore::open(scratch.path(), 1).expect("replay state opens");
        (scratch, store)
    }

    #[test]
    fn duplicate_and_out_of_window_frames_are_refused() {
        let (_scratch, store) = store("window");
        assert_eq!(
            store.admit(NodeId(2), 1, 1).expect("frame admits"),
            ReplayDecision::Accepted
        );
        assert_eq!(
            store.admit(NodeId(2), 1, 1).expect("duplicate classifies"),
            ReplayDecision::Duplicate
        );
        assert_eq!(
            store.admit(NodeId(2), 1, 65).expect("new frame admits"),
            ReplayDecision::Accepted
        );
        assert_eq!(
            store.admit(NodeId(2), 1, 1).expect("old frame classifies"),
            ReplayDecision::OutsideWindow
        );
    }

    #[test]
    fn old_sessions_stay_stale_after_restart() {
        let (scratch, store) = store("restart");
        assert_eq!(
            store.admit(NodeId(2), 4, 1).expect("frame admits"),
            ReplayDecision::Accepted
        );
        drop(store);
        let reopened = TransportReplayStore::open(scratch.path(), 1).expect("replay state reopens");
        assert_eq!(
            reopened
                .admit(NodeId(2), 3, 9)
                .expect("stale session classifies"),
            ReplayDecision::StaleSession
        );
        assert_eq!(
            reopened.admit(NodeId(2), 5, 1).expect("new session admits"),
            ReplayDecision::Accepted
        );
    }

    #[test]
    fn session_allocation_is_durable_and_monotonic() {
        let (scratch, store) = store("session");
        assert_eq!(store.allocate_session().expect("session allocates"), 1);
        assert_eq!(store.allocate_session().expect("session allocates"), 2);
        drop(store);
        let reopened = TransportReplayStore::open(scratch.path(), 1).expect("replay state reopens");
        assert_eq!(reopened.allocate_session().expect("session allocates"), 3);
    }
}
