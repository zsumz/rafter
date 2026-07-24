use std::{
    cmp::Ordering,
    fmt,
    num::NonZeroU64,
    str::{self, Utf8Error},
};

/// Largest admissible resource name in bytes.
pub const MAX_RESOURCE_NAME_LEN: usize = 64;

/// Bounded inline name of one lockable resource.
///
/// The bytes live in the value itself so a name can never allocate and can
/// never exceed [`MAX_RESOURCE_NAME_LEN`]. Names are compared byte-exactly:
/// replicas must reach the same naming decision from the same bytes without
/// consulting case or normalization tables that can differ between builds.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct ResourceName {
    bytes: [u8; MAX_RESOURCE_NAME_LEN],
    len: usize,
}

impl ResourceName {
    /// Creates a resource name from admissible ASCII bytes.
    ///
    /// # Errors
    ///
    /// Returns an error when the name is empty, longer than
    /// [`MAX_RESOURCE_NAME_LEN`], or contains a byte outside the admissible
    /// set.
    pub const fn new(name: &str) -> Result<Self, ResourceNameError> {
        let source = name.as_bytes();
        if source.is_empty() {
            return Err(ResourceNameError::Empty);
        }
        if source.len() > MAX_RESOURCE_NAME_LEN {
            return Err(ResourceNameError::TooLong);
        }

        let mut bytes = [0_u8; MAX_RESOURCE_NAME_LEN];
        let mut index = 0;
        while index < source.len() {
            let byte = source[index];
            if !is_admissible_name_byte(byte) {
                return Err(ResourceNameError::InvalidByte);
            }
            bytes[index] = byte;
            index += 1;
        }

        Ok(Self {
            bytes,
            len: source.len(),
        })
    }

    /// Returns the admissible bytes of the name.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    /// Returns the name as text.
    #[must_use]
    pub fn as_str(&self) -> &str {
        str::from_utf8(self.as_bytes()).unwrap_or_else(|error: Utf8Error| {
            unreachable!("admissible name bytes are ASCII: {error}")
        })
    }

    /// Returns the length of the name in bytes.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns whether the name is empty, which construction forbids.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl fmt::Debug for ResourceName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_tuple("ResourceName")
            .field(&self.as_str())
            .finish()
    }
}

impl Ord for ResourceName {
    /// Orders names lexicographically by content rather than by the padded
    /// backing array, which would otherwise order by trailing zero bytes.
    fn cmp(&self, other: &Self) -> Ordering {
        self.as_bytes().cmp(other.as_bytes())
    }
}

impl PartialOrd for ResourceName {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

const fn is_admissible_name_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/')
}

/// Inadmissible resource name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResourceNameError {
    /// The name had no bytes.
    Empty,
    /// The name exceeded [`MAX_RESOURCE_NAME_LEN`].
    TooLong,
    /// The name contained a byte outside the admissible set.
    InvalidByte,
}

/// Index of one bounded client-session slot.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ClientId(u32);

impl ClientId {
    /// Creates a client slot identifier.
    #[must_use]
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the numeric slot.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.0
    }
}

/// Nonzero session generation for one client slot.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SessionEpoch(NonZeroU64);

impl SessionEpoch {
    /// Creates a session epoch, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric epoch.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Nonzero monotonically increasing request sequence within one session.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Sequence(NonZeroU64);

impl Sequence {
    /// Creates a request sequence, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric sequence.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns the only sequence a fresh session may execute.
    #[must_use]
    pub const fn first() -> Self {
        match Self::new(1) {
            Some(sequence) => sequence,
            None => unreachable!(),
        }
    }

    /// Returns the successor sequence, or `None` at the numeric maximum.
    #[must_use]
    pub const fn successor(self) -> Option<Self> {
        match self.0.get().checked_add(1) {
            Some(value) => Self::new(value),
            None => None,
        }
    }
}

/// Nonzero fencing token scoped to one resource name.
///
/// Tokens issued for different resource names are unrelated and must never be
/// compared.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct FencingToken(NonZeroU64);

impl FencingToken {
    /// Creates a fencing token, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric token.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Returns the token issued by a resource's first acquisition.
    #[must_use]
    pub const fn first() -> Self {
        match Self::new(1) {
            Some(token) => token,
            None => unreachable!(),
        }
    }

    /// Returns the next token for the same resource, or `None` when the
    /// resource's token space is exhausted.
    ///
    /// Exhaustion fails closed. Wrapping would reissue a token that a guarded
    /// resource has already accepted.
    #[must_use]
    pub const fn successor(self) -> Option<Self> {
        match self.0.get().checked_add(1) {
            Some(value) => Self::new(value),
            None => None,
        }
    }
}

/// Replicated logical time.
///
/// This is a counter, not a clock. It advances only through
/// [`Operation::ExpireThrough`] and carries no real-world duration meaning.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LogicalTime(u64);

impl LogicalTime {
    /// Logical time before any expiration horizon has been applied.
    pub const ZERO: Self = Self(0);

    /// Creates a logical time.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric time.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }

    /// Returns the expiry produced by adding a lease, or `None` on overflow.
    #[must_use]
    pub const fn checked_add_lease(self, lease: LeaseDuration) -> Option<Self> {
        match self.0.checked_add(lease.get()) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

/// Nonzero lease length measured in replicated logical time.
///
/// A nonzero lease is what guarantees that an acquired lock is never born
/// expired.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LeaseDuration(NonZeroU64);

impl LeaseDuration {
    /// Creates a lease duration, rejecting zero.
    #[must_use]
    pub const fn new(value: u64) -> Option<Self> {
        match NonZeroU64::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// Returns the numeric duration.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

/// Fixed resource limits for one lock service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LockConfig {
    max_clients: u32,
    max_resources: u32,
}

impl LockConfig {
    /// Creates a bounded lock service configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when either bound is zero.
    pub const fn new(max_clients: u32, max_resources: u32) -> Result<Self, LockConfigError> {
        if max_clients == 0 {
            return Err(LockConfigError::ZeroClients);
        }
        if max_resources == 0 {
            return Err(LockConfigError::ZeroResources);
        }
        Ok(Self {
            max_clients,
            max_resources,
        })
    }

    /// Maximum number of addressable client slots.
    #[must_use]
    pub const fn max_clients(self) -> u32 {
        self.max_clients
    }

    /// Maximum number of resources that may ever be tracked.
    ///
    /// A tracked resource keeps its fencing high-water mark forever, so this
    /// bound applies to names ever acquired, not to locks currently held.
    #[must_use]
    pub const fn max_resources(self) -> u32 {
        self.max_resources
    }

    pub(crate) const fn admits_client(self, client_id: ClientId) -> bool {
        client_id.get() < self.max_clients
    }
}

/// Invalid lock service configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockConfigError {
    /// No client slot could ever open.
    ZeroClients,
    /// No resource could ever be acquired.
    ZeroResources,
}

/// Deterministic digest of the operation an identity claims to carry.
///
/// The digest binds a request identity to its operation so an adapter can route
/// a retry after an unknown outcome. It is never the admission key: retry and
/// conflict decisions compare the exact bounded operation, so a collision can
/// never admit a conflicting retry as an exact one.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RequestFingerprint(u64);

impl RequestFingerprint {
    /// Computes the fingerprint of an operation's canonical encoding.
    #[must_use]
    pub fn of(operation: &Operation) -> Self {
        let mut digest = Digest::new();
        match operation {
            Operation::Acquire { resource, lease } => {
                digest.tag(1);
                digest.resource(resource);
                digest.word(lease.get());
            }
            Operation::Renew {
                resource,
                token,
                lease,
            } => {
                digest.tag(2);
                digest.resource(resource);
                digest.word(token.get());
                digest.word(lease.get());
            }
            Operation::Release { resource, token } => {
                digest.tag(3);
                digest.resource(resource);
                digest.word(token.get());
            }
            Operation::ExpireThrough { horizon } => {
                digest.tag(4);
                digest.word(horizon.get());
            }
        }
        Self(digest.finish())
    }

    /// Returns the numeric digest.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

struct Digest(u64);

impl Digest {
    fn new() -> Self {
        Self(FNV_OFFSET_BASIS)
    }

    fn tag(&mut self, value: u8) {
        self.byte(value);
    }

    fn byte(&mut self, value: u8) {
        self.0 ^= u64::from(value);
        self.0 = self.0.wrapping_mul(FNV_PRIME);
    }

    fn word(&mut self, value: u64) {
        for byte in value.to_le_bytes() {
            self.byte(byte);
        }
    }

    /// Length-prefixes the name so that no name plus following field can share
    /// an encoding with a longer name.
    fn resource(&mut self, resource: &ResourceName) {
        let length = u8::try_from(resource.len())
            .expect("resource names are bounded well below the byte maximum");
        self.byte(length);
        for byte in resource.as_bytes() {
            self.byte(*byte);
        }
    }

    fn finish(self) -> u64 {
        self.0
    }
}

/// Identity of one operation within a client session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RequestIdentity {
    /// Bounded client slot.
    pub client_id: ClientId,
    /// Exact active session generation.
    pub session_epoch: SessionEpoch,
    /// Monotone sequence within the session.
    pub sequence: Sequence,
    /// Digest the client claims describes its operation.
    pub fingerprint: RequestFingerprint,
}

/// Deterministic operation applied by the lock service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Operation {
    /// Takes ownership of a free resource and issues its next fencing token.
    Acquire {
        resource: ResourceName,
        lease: LeaseDuration,
    },
    /// Extends the caller's existing tenure without issuing a new token.
    Renew {
        resource: ResourceName,
        token: FencingToken,
        lease: LeaseDuration,
    },
    /// Ends the caller's tenure, retaining the resource's high-water mark.
    Release {
        resource: ResourceName,
        token: FencingToken,
    },
    /// Advances replicated logical time and releases every lease it passes.
    ExpireThrough { horizon: LogicalTime },
}

/// Replicated lock service command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Command {
    /// Opens or advances one bounded client session.
    OpenSession {
        client_id: ClientId,
        session_epoch: SessionEpoch,
    },
    /// Submits an operation under a client request identity.
    Submit {
        request: RequestIdentity,
        operation: Operation,
    },
}

/// Deterministic result of an admitted operation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OperationResult {
    /// A tenure began and received a fencing token.
    Acquired {
        token: FencingToken,
        expiry: LogicalTime,
    },
    /// An existing tenure was extended under its unchanged token.
    Renewed {
        token: FencingToken,
        expiry: LogicalTime,
    },
    /// A tenure ended by owner request.
    Released,
    /// Logical time advanced, releasing the reported number of leases.
    Expired {
        released_locks: u32,
        logical_time: LogicalTime,
    },
    /// The operation was admitted under the request identity but rejected by
    /// lock service rules.
    Rejected(LockRejection),
}

/// Lock-level deterministic rejection that consumes and caches its sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockRejection {
    /// The resource is held, possibly by the requesting client.
    LockHeld {
        owner: ClientId,
        token: FencingToken,
        expiry: LogicalTime,
    },
    /// The resource has no current tenure, or has never been acquired.
    LockNotHeld,
    /// Another client owns the current tenure.
    NotLockHolder { owner: ClientId },
    /// The presented token does not name the current tenure.
    FencingTokenMismatch { current: FencingToken },
    /// The lease would push the expiry past the numeric maximum.
    LeaseOverflow,
    /// The resource's token space is exhausted and it can never be reacquired.
    TokenExhausted,
    /// The configured tracked-resource bound was reached.
    ResourceCapacityExceeded,
    /// The expiration horizon did not strictly exceed replicated logical time.
    LogicalTimeNotAdvanced { current: LogicalTime },
}

/// Request/session rejection that does not consume a sequence.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RequestRejection {
    /// The client ID is outside the configured slot range.
    ClientOutOfRange,
    /// No session has been opened for the client slot.
    SessionNotOpen,
    /// The command names an older session generation.
    StaleSession { current: SessionEpoch },
    /// The command names a newer generation that must be opened first.
    FutureSession { current: SessionEpoch },
    /// The sequence is older than the cached completion.
    StaleSequence { highest: Sequence },
    /// The sequence skipped the required next value.
    SequenceGap { expected: Sequence },
    /// The highest completed identity was reused with another operation.
    ConflictingRetry,
    /// The supplied fingerprint does not describe the supplied operation.
    FingerprintMismatch { expected: RequestFingerprint },
}

/// Stable client-visible response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockResponse {
    /// The requested session epoch is open.
    SessionOpened { session_epoch: SessionEpoch },
    /// A newly executed or exactly replayed operation result.
    Operation(OperationResult),
    /// The request was rejected before operation admission.
    Rejected(RequestRejection),
}

/// Internal classification useful to tests and adapters without changing the
/// stable client response.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyDisposition {
    /// A previously unused client slot opened.
    SessionOpened,
    /// A greater session epoch replaced the old one.
    SessionReplaced,
    /// The requested epoch was already open.
    SessionAlreadyOpen,
    /// A new request sequence executed and was cached.
    Applied,
    /// The highest completed request was replayed exactly.
    Replayed,
    /// The command failed request/session admission.
    Rejected,
}

/// Result of applying one replicated command.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ApplyOutcome {
    /// Stable response returned to the client.
    pub response: LockResponse,
    /// Whether and how the command affected session/application state.
    pub disposition: ApplyDisposition,
}

/// Public view of one current tenure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LockHolderView {
    /// Client slot that owns the tenure.
    pub owner: ClientId,
    /// Token issued when the tenure began.
    pub token: FencingToken,
    /// First logical time at which the lease no longer holds.
    pub expiry: LogicalTime,
}

/// Query result for one resource name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceStatus {
    /// Name that was queried.
    pub resource: ResourceName,
    /// Current tenure, when the resource is held.
    pub holder: Option<LockHolderView>,
    /// Highest token ever issued for the name, when it is tracked.
    pub token_floor: Option<FencingToken>,
    /// Replicated logical time observed with the query.
    pub logical_time: LogicalTime,
}

/// Canonical deterministic view of one tracked resource.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ResourceView {
    /// Tracked resource name.
    pub resource: ResourceName,
    /// Highest token ever issued for the name.
    pub token_floor: FencingToken,
    /// Current tenure, when the resource is held.
    pub holder: Option<LockHolderView>,
}

/// Public deterministic inspection of one active client session.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SessionView {
    /// Client slot that owns the session.
    pub client_id: ClientId,
    /// Current session generation.
    pub session_epoch: SessionEpoch,
    /// Highest completed request and its exact cached data, when present.
    pub cached: Option<(Sequence, Operation, OperationResult)>,
}

/// Canonical deterministic state view shared only for differential assertions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceView {
    /// Tracked resources sorted by name.
    pub resources: Vec<ResourceView>,
    /// Active client sessions sorted by slot.
    pub sessions: Vec<SessionView>,
    /// Replicated logical time.
    pub logical_time: LogicalTime,
}

/// Query result summarizing one lock service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ServiceSummary {
    /// Number of resources currently held.
    pub held_locks: u32,
    /// Number of resources with a retained high-water mark.
    pub tracked_resources: u32,
    /// Replicated logical time.
    pub logical_time: LogicalTime,
}
