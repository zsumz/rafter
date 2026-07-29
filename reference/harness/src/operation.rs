/// Stable identifier for one observed operation.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct OperationId(u64);

impl OperationId {
    /// Creates an operation identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// One already-parsed operation interval and its caller-owned action.
///
/// Positions refer to one event sequence. The invocation position precedes
/// the terminal position because the caller rejects malformed intervals before
/// constructing this value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Operation<A> {
    id: OperationId,
    action: A,
    invoked_at: usize,
    returned_at: usize,
}

impl<A> Operation<A> {
    /// Creates one complete, already-validated operation interval.
    #[must_use]
    pub const fn new(id: OperationId, action: A, invoked_at: usize, returned_at: usize) -> Self {
        Self {
            id,
            action,
            invoked_at,
            returned_at,
        }
    }

    pub(crate) const fn id(&self) -> OperationId {
        self.id
    }

    pub(crate) const fn action(&self) -> &A {
        &self.action
    }

    pub(crate) const fn invoked_at(&self) -> usize {
        self.invoked_at
    }

    pub(crate) const fn returned_at(&self) -> usize {
        self.returned_at
    }
}
