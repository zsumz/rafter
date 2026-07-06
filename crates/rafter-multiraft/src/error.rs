//! Error types for many-group hosts.

/// Errors returned by a many-group host.
///
/// This enum is exhaustive for host-level validation and driver failures
/// currently surfaced by `rafter-multiraft`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MultiRaftError<G> {
    GroupAlreadyOpen { group_id: G },
    UnknownGroup { group_id: G },
    WrongGroup { expected: G, actual: G },
    Driver { group_id: G, message: String },
}
