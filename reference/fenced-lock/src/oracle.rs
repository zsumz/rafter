use crate::{
    ApplyDisposition, ApplyOutcome, ClientId, Command, FencingToken, LeaseDuration, LockConfig,
    LockHolderView, LockRejection, LockResponse, LogicalTime, Operation, OperationResult,
    RequestFingerprint, RequestIdentity, RequestRejection, ResourceName, ResourceStatus,
    ResourceView, Sequence, ServiceSummary, ServiceView, SessionEpoch, SessionView,
};

/// How a tenure stopped being current.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TenureEnd {
    Released,
    Expired { at: LogicalTime },
}

/// One ownership tenure, recorded when it opens and closed out when it ends.
///
/// The journal is append-only and unbounded. That is affordable only because
/// this type is an oracle: it never runs in a replicated service.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Tenure {
    resource: ResourceName,
    owner: ClientId,
    token: FencingToken,
    expiry: LogicalTime,
    end: Option<TenureEnd>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct OracleSession {
    client_id: ClientId,
    session_epoch: SessionEpoch,
    completed: Option<(Sequence, Operation, OperationResult)>,
}

/// Structurally independent executable specification for the lock service.
///
/// This oracle stores no lock table and no fencing high-water mark. It keeps
/// every tenure ever opened and derives the current holder, the per-resource
/// mark, and the tracked-resource count by folding that journal. It shares
/// vocabulary with [`crate::LockService`], never implementation helpers.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceLockService {
    config: LockConfig,
    logical_time: LogicalTime,
    tenures: Vec<Tenure>,
    sessions: Vec<OracleSession>,
}

impl ReferenceLockService {
    /// Creates an empty reference lock service at logical time zero.
    #[must_use]
    pub fn new(config: LockConfig) -> Self {
        Self {
            config,
            logical_time: LogicalTime::ZERO,
            tenures: Vec::new(),
            sessions: Vec::new(),
        }
    }

    /// Applies one command through the independent transition system.
    pub fn apply(&mut self, command: Command) -> ApplyOutcome {
        match command {
            Command::OpenSession {
                client_id,
                session_epoch,
            } => self.apply_open_session(client_id, session_epoch),
            Command::Submit { request, operation } => self.apply_submission(request, operation),
        }
    }

    /// Returns replicated logical time.
    #[must_use]
    pub const fn logical_time(&self) -> LogicalTime {
        self.logical_time
    }

    /// Answers the `GetLock` query by folding the tenure journal.
    #[must_use]
    pub fn status(&self, resource: ResourceName) -> ResourceStatus {
        ResourceStatus {
            resource,
            holder: self
                .current_tenure(resource)
                .map(|tenure| tenure_holder(&self.tenures[tenure])),
            token_floor: self.derived_token_ceiling(resource),
            logical_time: self.logical_time,
        }
    }

    /// Returns aggregate counts derived from the journal.
    #[must_use]
    pub fn summary(&self) -> ServiceSummary {
        let held_locks = self
            .tenures
            .iter()
            .filter(|tenure| tenure.end.is_none())
            .count();
        ServiceSummary {
            held_locks: oracle_count(held_locks),
            tracked_resources: oracle_count(self.derived_resources().len()),
            logical_time: self.logical_time,
        }
    }

    /// Returns a canonical state view for differential assertions.
    #[must_use]
    pub fn view(&self) -> ServiceView {
        let resources = self
            .derived_resources()
            .into_iter()
            .map(|(resource, token_floor)| ResourceView {
                resource,
                token_floor,
                holder: self
                    .current_tenure(resource)
                    .map(|tenure| tenure_holder(&self.tenures[tenure])),
            })
            .collect();

        let mut sessions = self
            .sessions
            .iter()
            .map(|session| SessionView {
                client_id: session.client_id,
                session_epoch: session.session_epoch,
                cached: session.completed,
            })
            .collect::<Vec<_>>();
        sessions.sort_by_key(|session| session.client_id);

        ServiceView {
            resources,
            sessions,
            logical_time: self.logical_time,
        }
    }

    /// Returns the index of the resource's open tenure, when one exists.
    fn current_tenure(&self, resource: ResourceName) -> Option<usize> {
        self.tenures
            .iter()
            .position(|tenure| tenure.resource == resource && tenure.end.is_none())
    }

    /// Derives the resource's fencing high-water mark from every tenure ever
    /// recorded for it, rather than storing one.
    fn derived_token_ceiling(&self, resource: ResourceName) -> Option<FencingToken> {
        self.tenures
            .iter()
            .filter(|tenure| tenure.resource == resource)
            .map(|tenure| tenure.token)
            .max()
    }

    /// Folds the journal into the sorted set of tracked names paired with the
    /// highest token each has ever issued. Nothing here is stored: the marks
    /// exist only as a consequence of the recorded tenures.
    fn derived_resources(&self) -> Vec<(ResourceName, FencingToken)> {
        let mut derived: Vec<(ResourceName, FencingToken)> = Vec::new();
        for tenure in &self.tenures {
            if let Some(entry) = derived
                .iter_mut()
                .find(|(name, _)| *name == tenure.resource)
            {
                if tenure.token > entry.1 {
                    entry.1 = tenure.token;
                }
            } else {
                derived.push((tenure.resource, tenure.token));
            }
        }
        derived.sort_unstable_by_key(|(name, _)| *name);
        derived
    }

    fn apply_open_session(
        &mut self,
        client_id: ClientId,
        requested_epoch: SessionEpoch,
    ) -> ApplyOutcome {
        if client_id.get() >= self.config.max_clients() {
            return oracle_rejection(RequestRejection::ClientOutOfRange);
        }

        let existing = self
            .sessions
            .iter()
            .position(|session| session.client_id == client_id);
        let disposition = if let Some(position) = existing {
            let current_epoch = self.sessions[position].session_epoch;
            if requested_epoch.get() < current_epoch.get() {
                return oracle_rejection(RequestRejection::StaleSession {
                    current: current_epoch,
                });
            }
            if requested_epoch.get() > current_epoch.get() {
                // A replaced epoch clears only this slot's completion record.
                // Open tenures stay open: no session event ends a tenure.
                self.sessions[position] = OracleSession {
                    client_id,
                    session_epoch: requested_epoch,
                    completed: None,
                };
                ApplyDisposition::SessionReplaced
            } else {
                ApplyDisposition::SessionAlreadyOpen
            }
        } else {
            self.sessions.push(OracleSession {
                client_id,
                session_epoch: requested_epoch,
                completed: None,
            });
            ApplyDisposition::SessionOpened
        };

        ApplyOutcome {
            response: LockResponse::SessionOpened {
                session_epoch: requested_epoch,
            },
            disposition,
        }
    }

    fn apply_submission(&mut self, request: RequestIdentity, operation: Operation) -> ApplyOutcome {
        if request.client_id.get() >= self.config.max_clients() {
            return oracle_rejection(RequestRejection::ClientOutOfRange);
        }
        let Some(session_position) = self
            .sessions
            .iter()
            .position(|session| session.client_id == request.client_id)
        else {
            return oracle_rejection(RequestRejection::SessionNotOpen);
        };

        let session_epoch = self.sessions[session_position].session_epoch;
        if request.session_epoch.get() < session_epoch.get() {
            return oracle_rejection(RequestRejection::StaleSession {
                current: session_epoch,
            });
        }
        if request.session_epoch.get() > session_epoch.get() {
            return oracle_rejection(RequestRejection::FutureSession {
                current: session_epoch,
            });
        }

        let recomputed = RequestFingerprint::of(&operation);
        if request.fingerprint != recomputed {
            return oracle_rejection(RequestRejection::FingerprintMismatch {
                expected: recomputed,
            });
        }

        if let Some((completed_sequence, completed_operation, completed_result)) =
            self.sessions[session_position].completed
        {
            if request.sequence.get() < completed_sequence.get() {
                return oracle_rejection(RequestRejection::StaleSequence {
                    highest: completed_sequence,
                });
            }
            if request.sequence.get() == completed_sequence.get() {
                return if operation == completed_operation {
                    ApplyOutcome {
                        response: LockResponse::Operation(completed_result),
                        disposition: ApplyDisposition::Replayed,
                    }
                } else {
                    oracle_rejection(RequestRejection::ConflictingRetry)
                };
            }
            let expected = Sequence::new(
                completed_sequence
                    .get()
                    .checked_add(1)
                    .expect("a greater sequence implies a successor"),
            )
            .expect("the successor is nonzero");
            if request.sequence.get() != expected.get() {
                return oracle_rejection(RequestRejection::SequenceGap { expected });
            }
        } else {
            let expected = Sequence::new(1).expect("one is nonzero");
            if request.sequence.get() != expected.get() {
                return oracle_rejection(RequestRejection::SequenceGap { expected });
            }
        }

        let result = self.run(request.client_id, operation);
        self.sessions[session_position].completed = Some((request.sequence, operation, result));
        ApplyOutcome {
            response: LockResponse::Operation(result),
            disposition: ApplyDisposition::Applied,
        }
    }

    fn run(&mut self, client_id: ClientId, operation: Operation) -> OperationResult {
        match operation {
            Operation::Acquire { resource, lease } => self.open_tenure(client_id, resource, lease),
            Operation::Renew {
                resource,
                token,
                lease,
            } => self.extend_tenure(client_id, resource, token, lease),
            Operation::Release { resource, token } => self.close_tenure(client_id, resource, token),
            Operation::ExpireThrough { horizon } => self.advance_logical_time(horizon),
        }
    }

    fn open_tenure(
        &mut self,
        client_id: ClientId,
        resource: ResourceName,
        lease: LeaseDuration,
    ) -> OperationResult {
        let Some(expiry) = self.logical_time.checked_add_lease(lease) else {
            return OperationResult::Rejected(LockRejection::LeaseOverflow);
        };
        if let Some(open) = self.current_tenure(resource) {
            let current = self.tenures[open];
            return OperationResult::Rejected(LockRejection::LockHeld {
                owner: current.owner,
                token: current.token,
                expiry: current.expiry,
            });
        }

        let token = if let Some(ceiling) = self.derived_token_ceiling(resource) {
            let Some(next) = ceiling.get().checked_add(1).and_then(FencingToken::new) else {
                return OperationResult::Rejected(LockRejection::TokenExhausted);
            };
            next
        } else {
            if oracle_count(self.derived_resources().len()) >= self.config.max_resources() {
                return OperationResult::Rejected(LockRejection::ResourceCapacityExceeded);
            }
            FencingToken::first()
        };

        self.tenures.push(Tenure {
            resource,
            owner: client_id,
            token,
            expiry,
            end: None,
        });
        OperationResult::Acquired { token, expiry }
    }

    fn extend_tenure(
        &mut self,
        client_id: ClientId,
        resource: ResourceName,
        token: FencingToken,
        lease: LeaseDuration,
    ) -> OperationResult {
        let Some(open) = self.current_tenure(resource) else {
            return OperationResult::Rejected(LockRejection::LockNotHeld);
        };
        if self.tenures[open].owner != client_id {
            return OperationResult::Rejected(LockRejection::NotLockHolder {
                owner: self.tenures[open].owner,
            });
        }
        if self.tenures[open].token != token {
            return OperationResult::Rejected(LockRejection::FencingTokenMismatch {
                current: self.tenures[open].token,
            });
        }
        let Some(candidate) = self.logical_time.checked_add_lease(lease) else {
            return OperationResult::Rejected(LockRejection::LeaseOverflow);
        };

        if candidate.get() > self.tenures[open].expiry.get() {
            self.tenures[open].expiry = candidate;
        }
        OperationResult::Renewed {
            token: self.tenures[open].token,
            expiry: self.tenures[open].expiry,
        }
    }

    fn close_tenure(
        &mut self,
        client_id: ClientId,
        resource: ResourceName,
        token: FencingToken,
    ) -> OperationResult {
        let Some(open) = self.current_tenure(resource) else {
            return OperationResult::Rejected(LockRejection::LockNotHeld);
        };
        if self.tenures[open].owner != client_id {
            return OperationResult::Rejected(LockRejection::NotLockHolder {
                owner: self.tenures[open].owner,
            });
        }
        if self.tenures[open].token != token {
            return OperationResult::Rejected(LockRejection::FencingTokenMismatch {
                current: self.tenures[open].token,
            });
        }

        // The tenure is closed out, never removed: it is the only record of the
        // token this resource has already issued.
        self.tenures[open].end = Some(TenureEnd::Released);
        OperationResult::Released
    }

    fn advance_logical_time(&mut self, horizon: LogicalTime) -> OperationResult {
        if horizon.get() <= self.logical_time.get() {
            return OperationResult::Rejected(LockRejection::LogicalTimeNotAdvanced {
                current: self.logical_time,
            });
        }

        let mut released_locks = 0_u32;
        for tenure in &mut self.tenures {
            if tenure.end.is_none() && tenure.expiry.get() <= horizon.get() {
                tenure.end = Some(TenureEnd::Expired { at: horizon });
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

const fn tenure_holder(tenure: &Tenure) -> LockHolderView {
    LockHolderView {
        owner: tenure.owner,
        token: tenure.token,
        expiry: tenure.expiry,
    }
}

fn oracle_count(count: usize) -> u32 {
    u32::try_from(count).expect("oracle counts stay within the configured u32 bound")
}

fn oracle_rejection(reason: RequestRejection) -> ApplyOutcome {
    ApplyOutcome {
        response: LockResponse::Rejected(reason),
        disposition: ApplyDisposition::Rejected,
    }
}
