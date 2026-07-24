use std::{cmp::Ordering, collections::BTreeMap};

use crate::{
    ApplyDisposition, ApplyOutcome, ClientId, Command, FencingToken, LeaseDuration, LockConfig,
    LockHolderView, LockRejection, LockResponse, LogicalTime, Operation, OperationResult,
    RequestFingerprint, RequestIdentity, RequestRejection, ResourceName, ResourceStatus,
    ResourceView, Sequence, ServiceSummary, ServiceView, SessionEpoch, SessionView,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CachedCompletion {
    sequence: Sequence,
    fingerprint: RequestFingerprint,
    operation: Operation,
    result: OperationResult,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct SessionRecord {
    session_epoch: SessionEpoch,
    cached: Option<CachedCompletion>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HeldLock {
    owner: ClientId,
    token: FencingToken,
    expiry: LogicalTime,
}

/// One tracked resource: its retained fencing high-water mark and, when held,
/// the tenure embedded beside it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ResourceRecord {
    token_floor: FencingToken,
    holder: Option<HeldLock>,
}

/// Opaque transport-neutral snapshot of the pure lock service model.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockServiceSnapshot {
    logical_time: LogicalTime,
    resources: Vec<(ResourceName, ResourceRecord)>,
    sessions: Vec<(ClientId, SessionRecord)>,
}

/// Invalid lock service snapshot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SnapshotError {
    /// The snapshot exceeds the configured tracked-resource bound.
    TooManyResources,
    /// The snapshot exceeds the configured client-slot bound.
    TooManySessions,
    /// A session belongs to a client outside the configured slot range.
    ClientOutOfRange,
    /// A lock is owned by a client outside the configured slot range.
    HolderOutOfRange,
    /// The snapshot contains the same resource more than once.
    DuplicateResource,
    /// The snapshot contains the same client slot more than once.
    DuplicateSession,
    /// A held lock's expiry does not exceed replicated logical time.
    HeldLockExpired,
    /// A held lock's token is not its resource's high-water mark.
    TokenFloorMismatch,
    /// A cached fingerprint does not describe its cached operation.
    CachedFingerprintMismatch,
}

/// Deterministic fenced lock service that will later sit behind
/// `rafter-service`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockService {
    config: LockConfig,
    logical_time: LogicalTime,
    resources: BTreeMap<ResourceName, ResourceRecord>,
    sessions: BTreeMap<ClientId, SessionRecord>,
}

impl LockService {
    /// Creates an empty bounded lock service at logical time zero.
    #[must_use]
    pub fn new(config: LockConfig) -> Self {
        Self {
            config,
            logical_time: LogicalTime::ZERO,
            resources: BTreeMap::new(),
            sessions: BTreeMap::new(),
        }
    }

    /// Restores and validates a snapshot under `config`.
    ///
    /// # Errors
    ///
    /// Returns an error when the snapshot violates a resource bound, the
    /// expiry invariant, or the held-token/high-water-mark equality.
    pub fn from_snapshot(
        config: LockConfig,
        snapshot: LockServiceSnapshot,
    ) -> Result<Self, SnapshotError> {
        if u32::try_from(snapshot.resources.len())
            .map_or(true, |tracked| tracked > config.max_resources())
        {
            return Err(SnapshotError::TooManyResources);
        }
        if u32::try_from(snapshot.sessions.len())
            .map_or(true, |session_count| session_count > config.max_clients())
        {
            return Err(SnapshotError::TooManySessions);
        }

        let mut resources = BTreeMap::new();
        for (resource, record) in snapshot.resources {
            if let Some(holder) = record.holder {
                if !config.admits_client(holder.owner) {
                    return Err(SnapshotError::HolderOutOfRange);
                }
                if holder.expiry <= snapshot.logical_time {
                    return Err(SnapshotError::HeldLockExpired);
                }
                if holder.token != record.token_floor {
                    return Err(SnapshotError::TokenFloorMismatch);
                }
            }
            if resources.insert(resource, record).is_some() {
                return Err(SnapshotError::DuplicateResource);
            }
        }

        let mut sessions = BTreeMap::new();
        for (client_id, session) in snapshot.sessions {
            if !config.admits_client(client_id) {
                return Err(SnapshotError::ClientOutOfRange);
            }
            if let Some(cached) = session.cached {
                if cached.fingerprint != RequestFingerprint::of(&cached.operation) {
                    return Err(SnapshotError::CachedFingerprintMismatch);
                }
            }
            if sessions.insert(client_id, session).is_some() {
                return Err(SnapshotError::DuplicateSession);
            }
        }

        Ok(Self {
            config,
            logical_time: snapshot.logical_time,
            resources,
            sessions,
        })
    }

    /// Applies one replicated command.
    pub fn apply(&mut self, command: Command) -> ApplyOutcome {
        match command {
            Command::OpenSession {
                client_id,
                session_epoch,
            } => self.open_session(client_id, session_epoch),
            Command::Submit { request, operation } => self.submit(request, operation),
        }
    }

    /// Returns replicated logical time.
    #[must_use]
    pub const fn logical_time(&self) -> LogicalTime {
        self.logical_time
    }

    /// Answers the `GetLock` query for one resource name.
    ///
    /// Querying an unknown name does not track it.
    #[must_use]
    pub fn status(&self, resource: ResourceName) -> ResourceStatus {
        let record = self.resources.get(&resource);
        ResourceStatus {
            resource,
            holder: record.and_then(|record| record.holder).map(holder_view),
            token_floor: record.map(|record| record.token_floor),
            logical_time: self.logical_time,
        }
    }

    /// Returns aggregate counts for the whole service.
    #[must_use]
    pub fn summary(&self) -> ServiceSummary {
        let held_locks = self
            .resources
            .values()
            .filter(|record| record.holder.is_some())
            .count();
        ServiceSummary {
            held_locks: bounded_count(held_locks),
            tracked_resources: bounded_count(self.resources.len()),
            logical_time: self.logical_time,
        }
    }

    /// Returns a canonical view for independent differential assertions.
    #[must_use]
    pub fn view(&self) -> ServiceView {
        ServiceView {
            resources: self
                .resources
                .iter()
                .map(|(resource, record)| ResourceView {
                    resource: *resource,
                    token_floor: record.token_floor,
                    holder: record.holder.map(holder_view),
                })
                .collect(),
            sessions: self
                .sessions
                .iter()
                .map(|(client_id, session)| SessionView {
                    client_id: *client_id,
                    session_epoch: session.session_epoch,
                    cached: session
                        .cached
                        .map(|cached| (cached.sequence, cached.operation, cached.result)),
                })
                .collect(),
            logical_time: self.logical_time,
        }
    }

    /// Captures all safety-relevant state, including high-water marks and the
    /// deduplication cache.
    #[must_use]
    pub fn snapshot(&self) -> LockServiceSnapshot {
        LockServiceSnapshot {
            logical_time: self.logical_time,
            resources: self
                .resources
                .iter()
                .map(|(resource, record)| (*resource, *record))
                .collect(),
            sessions: self
                .sessions
                .iter()
                .map(|(client_id, session)| (*client_id, *session))
                .collect(),
        }
    }

    fn open_session(&mut self, client_id: ClientId, requested_epoch: SessionEpoch) -> ApplyOutcome {
        if !self.config.admits_client(client_id) {
            return rejected(RequestRejection::ClientOutOfRange);
        }

        let disposition = match self.sessions.get(&client_id) {
            None => ApplyDisposition::SessionOpened,
            Some(current) => match requested_epoch.cmp(&current.session_epoch) {
                Ordering::Less => {
                    return rejected(RequestRejection::StaleSession {
                        current: current.session_epoch,
                    });
                }
                Ordering::Equal => ApplyDisposition::SessionAlreadyOpen,
                Ordering::Greater => ApplyDisposition::SessionReplaced,
            },
        };

        // A replaced epoch clears deduplication state only. Locks outlive
        // sessions; releasing them here would be an expiration path that
        // replicated logical time does not govern.
        if disposition != ApplyDisposition::SessionAlreadyOpen {
            self.sessions.insert(
                client_id,
                SessionRecord {
                    session_epoch: requested_epoch,
                    cached: None,
                },
            );
        }
        ApplyOutcome {
            response: LockResponse::SessionOpened {
                session_epoch: requested_epoch,
            },
            disposition,
        }
    }

    fn submit(&mut self, request: RequestIdentity, operation: Operation) -> ApplyOutcome {
        let Some(session) = self.sessions.get(&request.client_id) else {
            return rejected(if self.config.admits_client(request.client_id) {
                RequestRejection::SessionNotOpen
            } else {
                RequestRejection::ClientOutOfRange
            });
        };

        match request.session_epoch.cmp(&session.session_epoch) {
            Ordering::Less => {
                return rejected(RequestRejection::StaleSession {
                    current: session.session_epoch,
                });
            }
            Ordering::Greater => {
                return rejected(RequestRejection::FutureSession {
                    current: session.session_epoch,
                });
            }
            Ordering::Equal => {}
        }

        // Envelope self-consistency is decided before sequence admission: a
        // request whose fingerprint does not describe its own operation is
        // malformed wherever its sequence falls.
        let expected_fingerprint = RequestFingerprint::of(&operation);
        if request.fingerprint != expected_fingerprint {
            return rejected(RequestRejection::FingerprintMismatch {
                expected: expected_fingerprint,
            });
        }

        if let Some(cached) = session.cached {
            match request.sequence.cmp(&cached.sequence) {
                Ordering::Less => {
                    return rejected(RequestRejection::StaleSequence {
                        highest: cached.sequence,
                    });
                }
                // Exact comparison of the bounded operation decides a retry.
                // The fingerprint never substitutes for it.
                Ordering::Equal if operation == cached.operation => {
                    return ApplyOutcome {
                        response: LockResponse::Operation(cached.result),
                        disposition: ApplyDisposition::Replayed,
                    };
                }
                Ordering::Equal => return rejected(RequestRejection::ConflictingRetry),
                Ordering::Greater => {
                    let expected = cached
                        .sequence
                        .successor()
                        .expect("a larger u64 sequence implies a representable successor");
                    if request.sequence != expected {
                        return rejected(RequestRejection::SequenceGap { expected });
                    }
                }
            }
        } else {
            let first = Sequence::first();
            if request.sequence != first {
                return rejected(RequestRejection::SequenceGap { expected: first });
            }
        }

        let result = self.run_operation(request.client_id, operation);
        self.sessions
            .get_mut(&request.client_id)
            .expect("validated session remains present")
            .cached = Some(CachedCompletion {
            sequence: request.sequence,
            fingerprint: expected_fingerprint,
            operation,
            result,
        });
        ApplyOutcome {
            response: LockResponse::Operation(result),
            disposition: ApplyDisposition::Applied,
        }
    }

    fn run_operation(&mut self, client_id: ClientId, operation: Operation) -> OperationResult {
        match operation {
            Operation::Acquire { resource, lease } => self.acquire(client_id, resource, lease),
            Operation::Renew {
                resource,
                token,
                lease,
            } => self.renew(client_id, resource, token, lease),
            Operation::Release { resource, token } => self.release(client_id, resource, token),
            Operation::ExpireThrough { horizon } => self.expire_through(horizon),
        }
    }

    fn acquire(
        &mut self,
        client_id: ClientId,
        resource: ResourceName,
        lease: LeaseDuration,
    ) -> OperationResult {
        let Some(expiry) = self.logical_time.checked_add_lease(lease) else {
            return OperationResult::Rejected(LockRejection::LeaseOverflow);
        };

        let token = if let Some(record) = self.resources.get(&resource) {
            if let Some(holder) = record.holder {
                return OperationResult::Rejected(LockRejection::LockHeld {
                    owner: holder.owner,
                    token: holder.token,
                    expiry: holder.expiry,
                });
            }
            let Some(next) = record.token_floor.successor() else {
                return OperationResult::Rejected(LockRejection::TokenExhausted);
            };
            next
        } else {
            if bounded_count(self.resources.len()) >= self.config.max_resources() {
                return OperationResult::Rejected(LockRejection::ResourceCapacityExceeded);
            }
            FencingToken::first()
        };

        self.resources.insert(
            resource,
            ResourceRecord {
                token_floor: token,
                holder: Some(HeldLock {
                    owner: client_id,
                    token,
                    expiry,
                }),
            },
        );
        OperationResult::Acquired { token, expiry }
    }

    fn renew(
        &mut self,
        client_id: ClientId,
        resource: ResourceName,
        token: FencingToken,
        lease: LeaseDuration,
    ) -> OperationResult {
        let Some(record) = self.resources.get_mut(&resource) else {
            return OperationResult::Rejected(LockRejection::LockNotHeld);
        };
        let Some(holder) = record.holder.as_mut() else {
            return OperationResult::Rejected(LockRejection::LockNotHeld);
        };
        if holder.owner != client_id {
            return OperationResult::Rejected(LockRejection::NotLockHolder {
                owner: holder.owner,
            });
        }
        if holder.token != token {
            return OperationResult::Rejected(LockRejection::FencingTokenMismatch {
                current: holder.token,
            });
        }
        let Some(candidate) = self.logical_time.checked_add_lease(lease) else {
            return OperationResult::Rejected(LockRejection::LeaseOverflow);
        };

        // Expiry is monotone for the life of a tenure. An owner that could pull
        // its own expiry backwards could let a successor acquire earlier than
        // the owner believes.
        if candidate > holder.expiry {
            holder.expiry = candidate;
        }
        OperationResult::Renewed {
            token: holder.token,
            expiry: holder.expiry,
        }
    }

    fn release(
        &mut self,
        client_id: ClientId,
        resource: ResourceName,
        token: FencingToken,
    ) -> OperationResult {
        let Some(record) = self.resources.get_mut(&resource) else {
            return OperationResult::Rejected(LockRejection::LockNotHeld);
        };
        let Some(holder) = record.holder else {
            return OperationResult::Rejected(LockRejection::LockNotHeld);
        };
        if holder.owner != client_id {
            return OperationResult::Rejected(LockRejection::NotLockHolder {
                owner: holder.owner,
            });
        }
        if holder.token != token {
            return OperationResult::Rejected(LockRejection::FencingTokenMismatch {
                current: holder.token,
            });
        }

        // The tenure ends; the resource stays tracked so its high-water mark
        // outlives every owner.
        record.holder = None;
        OperationResult::Released
    }

    fn expire_through(&mut self, horizon: LogicalTime) -> OperationResult {
        if horizon <= self.logical_time {
            return OperationResult::Rejected(LockRejection::LogicalTimeNotAdvanced {
                current: self.logical_time,
            });
        }

        let mut released_locks = 0_u32;
        for record in self.resources.values_mut() {
            if record.holder.is_some_and(|holder| holder.expiry <= horizon) {
                record.holder = None;
                released_locks += 1;
            }
        }
        self.logical_time = horizon;
        OperationResult::Expired {
            released_locks,
            logical_time: horizon,
        }
    }
}

const fn holder_view(holder: HeldLock) -> LockHolderView {
    LockHolderView {
        owner: holder.owner,
        token: holder.token,
        expiry: holder.expiry,
    }
}

/// Resource counts are held below `max_resources`, which is a `u32`, by every
/// path that inserts.
fn bounded_count(count: usize) -> u32 {
    u32::try_from(count).expect("tracked resources stay within the configured u32 bound")
}

fn rejected(reason: RequestRejection) -> ApplyOutcome {
    ApplyOutcome {
        response: LockResponse::Rejected(reason),
        disposition: ApplyDisposition::Rejected,
    }
}
