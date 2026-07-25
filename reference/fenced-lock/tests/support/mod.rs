// This module holds only the command builders every suite needs. The heavier
// support modules beside it — `cluster`, `durable`, `scratch`, and `transport`
// — are pulled in by `#[path]` only by the suites that drive them, so the
// model-only tests neither compile nor link a driver or a filesystem.

use rafter_reference_fenced_lock::{
    ClientId, Command, FencingToken, LeaseDuration, LockConfig, LogicalTime, Operation,
    RequestFingerprint, RequestIdentity, ResourceName, Sequence, SessionEpoch,
};

pub fn config(max_clients: u32, max_resources: u32) -> LockConfig {
    LockConfig::new(max_clients, max_resources).expect("test bounds are nonzero")
}

pub fn client(value: u32) -> ClientId {
    ClientId::new(value)
}

pub fn epoch(value: u64) -> SessionEpoch {
    SessionEpoch::new(value).expect("test epoch is nonzero")
}

pub fn sequence(value: u64) -> Sequence {
    Sequence::new(value).expect("test sequence is nonzero")
}

pub fn token(value: u64) -> FencingToken {
    FencingToken::new(value).expect("test token is nonzero")
}

pub fn lease(value: u64) -> LeaseDuration {
    LeaseDuration::new(value).expect("test lease is nonzero")
}

pub fn time(value: u64) -> LogicalTime {
    LogicalTime::new(value)
}

pub fn resource(name: &str) -> ResourceName {
    ResourceName::new(name).expect("test resource name is admissible")
}

pub fn acquire(name: &str, lease_length: u64) -> Operation {
    Operation::Acquire {
        resource: resource(name),
        lease: lease(lease_length),
    }
}

pub fn renew(name: &str, held_token: u64, lease_length: u64) -> Operation {
    Operation::Renew {
        resource: resource(name),
        token: token(held_token),
        lease: lease(lease_length),
    }
}

pub fn release(name: &str, held_token: u64) -> Operation {
    Operation::Release {
        resource: resource(name),
        token: token(held_token),
    }
}

pub fn expire_through(horizon: u64) -> Operation {
    Operation::ExpireThrough {
        horizon: time(horizon),
    }
}

pub fn open_session(client_id: u32, session_epoch: u64) -> Command {
    Command::OpenSession {
        client_id: client(client_id),
        session_epoch: epoch(session_epoch),
    }
}

/// Builds a well-formed submission whose fingerprint describes its operation.
pub fn submit(
    client_id: u32,
    session_epoch: u64,
    request_sequence: u64,
    operation: Operation,
) -> Command {
    Command::Submit {
        request: RequestIdentity {
            client_id: client(client_id),
            session_epoch: epoch(session_epoch),
            sequence: sequence(request_sequence),
            fingerprint: RequestFingerprint::of(&operation),
        },
        operation,
    }
}

/// Builds a submission whose fingerprint is supplied rather than derived, so a
/// test can present an envelope that does not describe its own operation.
pub fn submit_with_fingerprint(
    client_id: u32,
    session_epoch: u64,
    request_sequence: u64,
    fingerprint: RequestFingerprint,
    operation: Operation,
) -> Command {
    Command::Submit {
        request: RequestIdentity {
            client_id: client(client_id),
            session_epoch: epoch(session_epoch),
            sequence: sequence(request_sequence),
            fingerprint,
        },
        operation,
    }
}
