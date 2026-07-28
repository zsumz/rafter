//! The replica's line-oriented client protocol.
//!
//! One request is one line; one response is one line. Tokens are separated by
//! single spaces and every token is an ASCII word or a decimal integer, so a
//! reader can check a transcript by eye and a client can be written without a
//! parser generator. The protocol is deliberately not versioned or framed: it
//! is the integration composition's client surface, and a production
//! composition would replace it wholesale rather than negotiate with it.
//!
//! # Requests
//!
//! ```text
//! STATUS
//! OPEN_SESSION <client_id> <epoch>
//! SUBMIT <client_id> <epoch> <sequence> ACQUIRE <resource> <lease>
//! SUBMIT <client_id> <epoch> <sequence> RENEW <resource> <token> <lease>
//! SUBMIT <client_id> <epoch> <sequence> RELEASE <resource> <token>
//! SUBMIT <client_id> <epoch> <sequence> EXPIRE_THROUGH <horizon>
//! QUERY LOCK <resource>
//! LOCAL LOCK <resource>
//! SHUTDOWN
//! ```
//!
//! `QUERY` is linearizable: it runs behind a read barrier and it is the only
//! consistency this application offers a client, because the contract makes no
//! lease-read claim.
//!
//! `LOCAL` is not a weaker read on the same path — there is no such path.
//! `rafter-service`'s transport driver refuses every consistency but
//! linearizable, so `LOCAL` borrows this replica's group and reads its applied
//! state directly. It answers with no barrier, no freshness claim, and no read
//! proof, which is why it is a separate verb rather than a parameter: it exists
//! so an operator, or a test, can ask a rejoining replica what *it* holds, and
//! nothing routing on correctness may use it.
//!
//! # What this protocol does not carry
//!
//! **The request fingerprint.** A [`RequestIdentity`] carries a fingerprint of
//! the operation it claims, and the state machine rejects an envelope whose
//! fingerprint does not describe its operation. Over this protocol the
//! operation travels in the clear on the same line, so the replica derives the
//! fingerprint from it and `RequestRejection::FingerprintMismatch` is
//! unreachable here. That is a real gap in coverage and it is named rather than
//! papered over: the mismatch is exercised by the in-process suites, where a
//! caller can build an envelope that disagrees with its own operation.
//! `ConflictingRetry` is unaffected — reusing a sequence with a *different*
//! operation is still detected, because the operation itself is compared.
//!
//! **Any notion of who is asking.** A client id here is a bounded slot number
//! in the replicated state machine, which is deduplication vocabulary and not a
//! principal. Nothing authenticates a connection, so nothing stops one
//! connection from submitting under another client's identity — including
//! `EXPIRE_THROUGH`, which the contract says only the service's authorized
//! expiration driver should submit. That authorization lives outside the
//! replicated state machine, and at this composition level it lives nowhere at
//! all. `CONTRACT.md` records it as an open residual and
//! `tests/process_cluster.rs` demonstrates it rather than only asserting it.
//!
//! # Responses
//!
//! ```text
//! STATUS <ready|recovering|abandoned> <role> <term> <applied> <committed> <leader|->
//! OK <disposition> SESSION <epoch>
//! OK <disposition> OP ACQUIRED <token> <expiry>
//! OK <disposition> OP RENEWED <token> <expiry>
//! OK <disposition> OP RELEASED
//! OK <disposition> OP EXPIRED <released_locks> <logical_time>
//! OK <disposition> OP LOCK_REJECTED <reason> [<detail>...]
//! OK <disposition> REQUEST_REJECTED <reason> [<detail>...]
//! OK LOCK <resource> <owner|-> <held_token|-> <expiry|-> <token_floor|-> <logical_time>
//! NOTREADY <applied> <committed>
//! NOTCOMMITTED <kind> <leader|->
//! UNKNOWN <detail...>
//! ABANDONED <detail...>
//! BYE
//! ERR <detail...>
//! ```
//!
//! The three terminal write outcomes are exactly the contract's three, and the
//! distinction survives the process boundary intact:
//!
//! - `OK` carries the replicated response.
//! - `NOTCOMMITTED` is emitted **only** when the write error's own
//!   [`WriteFate`](rafter_service::WriteFate) is `NotAppended`, which
//!   `rafter-service` documents as the driver having observed the refusal
//!   itself. It is not inferred from an error category here or anywhere else.
//! - `UNKNOWN` is everything else, including a reply this replica never got to
//!   send. A client that loses its connection observes the same thing by
//!   observing nothing, which is why a killed leader needs no protocol support.
//!
//! `NOTCOMMITTED` names its reason with the error's stable low-cardinality
//! kind rather than a debug rendering, so a client that must distinguish "this
//! node is not the leader" from "this payload is too large" does not have to
//! parse a struct literal, and the leader hint after it stays findable.

use std::fmt;

use rafter_reference_fenced_lock::{
    ApplyDisposition, ClientId, Command, FencingToken, LeaseDuration, LockRejection, LockResponse,
    LogicalTime, Operation, OperationResult, RequestFingerprint, RequestIdentity, RequestRejection,
    ResourceName, ResourceStatus, Sequence, SessionEpoch,
};
use rafter_service::{WriteError, WriteErrorKind};

/// One parsed client request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Request {
    /// Report readiness and role without being gated by readiness.
    Status,
    /// Replicate one command.
    Submit(Command),
    /// Run one linearizable `GetLock` behind a read barrier.
    Query(ResourceName),
    /// Read this replica's own applied state, which may be stale.
    Local(ResourceName),
    /// Stop serving and exit cleanly.
    Shutdown,
}

/// Why a request line could not be turned into a [`Request`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RequestError(String);

impl fmt::Display for RequestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn bad(detail: impl Into<String>) -> RequestError {
    RequestError(detail.into())
}

/// Parses one request line.
///
/// # Errors
///
/// Returns a [`RequestError`] naming the first thing wrong with the line.
pub fn parse_request(line: &str) -> Result<Request, RequestError> {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    match tokens.first().copied() {
        Some("STATUS") => Ok(Request::Status),
        Some("SHUTDOWN") => Ok(Request::Shutdown),
        Some("OPEN_SESSION") => {
            let client_id = ClientId::new(parse_u32(tokens.get(1), "client id")?);
            let session_epoch = SessionEpoch::new(parse_u64(tokens.get(2), "session epoch")?)
                .ok_or_else(|| bad("session epoch must be nonzero"))?;
            Ok(Request::Submit(Command::OpenSession {
                client_id,
                session_epoch,
            }))
        }
        Some("SUBMIT") => {
            let client_id = ClientId::new(parse_u32(tokens.get(1), "client id")?);
            let session_epoch = SessionEpoch::new(parse_u64(tokens.get(2), "session epoch")?)
                .ok_or_else(|| bad("session epoch must be nonzero"))?;
            let sequence = Sequence::new(parse_u64(tokens.get(3), "sequence")?)
                .ok_or_else(|| bad("sequence must be nonzero"))?;
            let operation = parse_operation(&tokens[4..])?;
            Ok(Request::Submit(Command::Submit {
                request: RequestIdentity {
                    client_id,
                    session_epoch,
                    sequence,
                    // Derived rather than carried; the module docs say what
                    // that costs.
                    fingerprint: RequestFingerprint::of(&operation),
                },
                operation,
            }))
        }
        Some("QUERY") => Ok(Request::Query(parse_lock_query(&tokens[1..])?)),
        Some("LOCAL") => Ok(Request::Local(parse_lock_query(&tokens[1..])?)),
        Some(other) => Err(bad(format!("unknown verb {other}"))),
        None => Err(bad("empty request")),
    }
}

fn parse_lock_query(tokens: &[&str]) -> Result<ResourceName, RequestError> {
    match tokens.first().copied() {
        Some("LOCK") => parse_resource(tokens.get(1)),
        Some(other) => Err(bad(format!("unknown query {other}"))),
        None => Err(bad("a query names what to read")),
    }
}

fn parse_operation(tokens: &[&str]) -> Result<Operation, RequestError> {
    match tokens.first().copied() {
        Some("ACQUIRE") => Ok(Operation::Acquire {
            resource: parse_resource(tokens.get(1))?,
            lease: parse_lease(tokens.get(2))?,
        }),
        Some("RENEW") => Ok(Operation::Renew {
            resource: parse_resource(tokens.get(1))?,
            token: parse_token(tokens.get(2))?,
            lease: parse_lease(tokens.get(3))?,
        }),
        Some("RELEASE") => Ok(Operation::Release {
            resource: parse_resource(tokens.get(1))?,
            token: parse_token(tokens.get(2))?,
        }),
        Some("EXPIRE_THROUGH") => Ok(Operation::ExpireThrough {
            horizon: LogicalTime::new(parse_u64(tokens.get(1), "horizon")?),
        }),
        Some(other) => Err(bad(format!("unknown operation {other}"))),
        None => Err(bad("a submission names an operation")),
    }
}

fn parse_resource(token: Option<&&str>) -> Result<ResourceName, RequestError> {
    let name = token.ok_or_else(|| bad("a resource name is required"))?;
    ResourceName::new(name).map_err(|reason| bad(format!("bad resource name: {reason:?}")))
}

fn parse_token(token: Option<&&str>) -> Result<FencingToken, RequestError> {
    FencingToken::new(parse_u64(token, "fencing token")?)
        .ok_or_else(|| bad("a fencing token must be nonzero"))
}

fn parse_lease(token: Option<&&str>) -> Result<LeaseDuration, RequestError> {
    LeaseDuration::new(parse_u64(token, "lease")?).ok_or_else(|| bad("a lease must be nonzero"))
}

fn parse_u64(token: Option<&&str>, what: &str) -> Result<u64, RequestError> {
    token
        .ok_or_else(|| bad(format!("{what} is required")))?
        .parse()
        .map_err(|_| bad(format!("{what} must be an integer")))
}

fn parse_u32(token: Option<&&str>, what: &str) -> Result<u32, RequestError> {
    token
        .ok_or_else(|| bad(format!("{what} is required")))?
        .parse()
        .map_err(|_| bad(format!("{what} must be an integer")))
}

/// What a replica's `STATUS` says about whether it will serve.
///
/// **Three answers rather than two, because the third one is not a stage of the
/// first two.** `Ready` and `Recovering` are points on one path: a recovering
/// replica is expected to become ready, and a client that sees `recovering`
/// should retry here. A replica that cannot make its peer control plane durable
/// is on no path at all — it is going to exit, and nothing it does next will
/// make it ready — so reporting `recovering` for it invites exactly the retry
/// that will never be answered.
///
/// `Abandoned` borrows the protocol's own terminal word rather than inventing
/// one: `ABANDONED` is already what this replica answers every service request
/// in that state, and `STATUS` is the request that exists to say which state
/// that is.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Readiness {
    /// Applied everything it knows to be committed, and serving.
    Ready,
    /// Not caught up yet, and expected to be.
    Recovering,
    /// Will not serve again, whatever a client does.
    Abandoned,
}

impl Readiness {
    /// The wire word for this answer.
    const fn word(self) -> &'static str {
        match self {
            Self::Ready => "ready",
            Self::Recovering => "recovering",
            Self::Abandoned => "abandoned",
        }
    }

    /// The answer a serving replica's own readiness gate gives.
    #[must_use]
    pub const fn of_serving(ready: bool) -> Self {
        if ready {
            Self::Ready
        } else {
            Self::Recovering
        }
    }
}

/// Renders the `STATUS` response.
pub fn render_status(
    readiness: Readiness,
    role: rafter::Role,
    term: u64,
    applied: u64,
    committed: u64,
    leader: Option<u64>,
) -> String {
    let role = match role {
        rafter::Role::Leader => "leader",
        rafter::Role::Candidate => "candidate",
        rafter::Role::PreCandidate => "precandidate",
        rafter::Role::Follower => "follower",
    };
    format!(
        "STATUS {} {role} {term} {applied} {committed} {}",
        readiness.word(),
        leader.map_or_else(|| String::from("-"), |leader| leader.to_string())
    )
}

/// Renders a replicated response and the disposition that produced it.
pub fn render_applied(disposition: ApplyDisposition, response: LockResponse) -> String {
    let disposition = match disposition {
        ApplyDisposition::SessionOpened => "SESSION_OPENED",
        ApplyDisposition::SessionReplaced => "SESSION_REPLACED",
        ApplyDisposition::SessionAlreadyOpen => "SESSION_ALREADY_OPEN",
        ApplyDisposition::Applied => "APPLIED",
        ApplyDisposition::Replayed => "REPLAYED",
        ApplyDisposition::Rejected => "NOT_ADMITTED",
    };
    match response {
        LockResponse::SessionOpened { session_epoch } => {
            format!("OK {disposition} SESSION {}", session_epoch.get())
        }
        LockResponse::Operation(result) => {
            format!("OK {disposition} OP {}", render_operation_result(result))
        }
        LockResponse::Rejected(rejection) => format!(
            "OK {disposition} REQUEST_REJECTED {}",
            render_request_rejection(rejection)
        ),
    }
}

fn render_operation_result(result: OperationResult) -> String {
    match result {
        OperationResult::Acquired { token, expiry } => {
            format!("ACQUIRED {} {}", token.get(), expiry.get())
        }
        OperationResult::Renewed { token, expiry } => {
            format!("RENEWED {} {}", token.get(), expiry.get())
        }
        OperationResult::Released => String::from("RELEASED"),
        OperationResult::Expired {
            released_locks,
            logical_time,
        } => format!("EXPIRED {released_locks} {}", logical_time.get()),
        OperationResult::Rejected(rejection) => {
            format!("LOCK_REJECTED {}", render_lock_rejection(rejection))
        }
    }
}

fn render_lock_rejection(rejection: LockRejection) -> String {
    match rejection {
        LockRejection::LockHeld {
            owner,
            token,
            expiry,
        } => format!("LOCK_HELD {} {} {}", owner.get(), token.get(), expiry.get()),
        LockRejection::LockNotHeld => String::from("LOCK_NOT_HELD"),
        LockRejection::NotLockHolder { owner } => format!("NOT_LOCK_HOLDER {}", owner.get()),
        LockRejection::FencingTokenMismatch { current } => {
            format!("FENCING_TOKEN_MISMATCH {}", current.get())
        }
        LockRejection::LeaseOverflow => String::from("LEASE_OVERFLOW"),
        LockRejection::TokenExhausted => String::from("TOKEN_EXHAUSTED"),
        LockRejection::ResourceCapacityExceeded => String::from("RESOURCE_CAPACITY_EXCEEDED"),
        LockRejection::LogicalTimeNotAdvanced { current } => {
            format!("LOGICAL_TIME_NOT_ADVANCED {}", current.get())
        }
    }
}

fn render_request_rejection(rejection: RequestRejection) -> String {
    match rejection {
        RequestRejection::ClientOutOfRange => String::from("CLIENT_OUT_OF_RANGE"),
        RequestRejection::SessionNotOpen => String::from("SESSION_NOT_OPEN"),
        RequestRejection::StaleSession { current } => {
            format!("STALE_SESSION {}", current.get())
        }
        RequestRejection::FutureSession { current } => {
            format!("FUTURE_SESSION {}", current.get())
        }
        RequestRejection::StaleSequence { highest } => {
            format!("STALE_SEQUENCE {}", highest.get())
        }
        RequestRejection::SequenceGap { expected } => {
            format!("SEQUENCE_GAP {}", expected.get())
        }
        RequestRejection::ConflictingRetry => String::from("CONFLICTING_RETRY"),
        RequestRejection::FingerprintMismatch { expected } => {
            format!("FINGERPRINT_MISMATCH {}", expected.get())
        }
    }
}

/// Renders a write this replica's driver refused before it reached any log.
pub fn render_not_committed(error: &WriteError) -> String {
    let kind = match error.kind() {
        WriteErrorKind::NotLeader => "NOT_LEADER",
        WriteErrorKind::Rejected => "REJECTED",
        WriteErrorKind::PayloadTooLarge => "PAYLOAD_TOO_LARGE",
        WriteErrorKind::UnknownOutcome => "UNKNOWN_OUTCOME",
        WriteErrorKind::WrongGroup => "WRONG_GROUP",
        WriteErrorKind::StateMachine => "STATE_MACHINE",
        WriteErrorKind::Storage => "STORAGE",
        WriteErrorKind::Transport => "TRANSPORT",
        WriteErrorKind::ShuttingDown => "SHUTTING_DOWN",
        WriteErrorKind::Poisoned => "POISONED",
        WriteErrorKind::LocalProposalIdExhausted => "LOCAL_PROPOSAL_ID_EXHAUSTED",
        WriteErrorKind::ManagedInvariantViolation => "MANAGED_INVARIANT_VIOLATION",
        // The kind is `#[non_exhaustive]`, and an unrecognized one is reported
        // as such rather than folded into a neighbour.
        _ => "OTHER",
    };
    let leader = match error {
        WriteError::NotLeader { leader_hint, .. } => {
            leader_hint.map_or_else(|| String::from("-"), |leader| leader.0.to_string())
        }
        _ => String::from("-"),
    };
    format!("NOTCOMMITTED {kind} {leader}")
}

/// Renders an answered `GetLock`.
pub fn render_lock(status: ResourceStatus) -> String {
    let (owner, token, expiry) = status.holder.map_or_else(
        || (String::from("-"), String::from("-"), String::from("-")),
        |holder| {
            (
                holder.owner.get().to_string(),
                holder.token.get().to_string(),
                holder.expiry.get().to_string(),
            )
        },
    );
    format!(
        "OK LOCK {} {owner} {token} {expiry} {} {}",
        status.resource.as_str(),
        status
            .token_floor
            .map_or_else(|| String::from("-"), |floor| floor.get().to_string()),
        status.logical_time.get()
    )
}
