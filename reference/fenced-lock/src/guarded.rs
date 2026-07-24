use crate::{FencingToken, ResourceName};

/// A write offered to a guarded resource under a fencing token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuardedWrite {
    /// Resource whose lock the writer claims to hold.
    pub resource: ResourceName,
    /// Token the writer received when its tenure began.
    pub token: FencingToken,
    /// Value the writer wants to store.
    pub value: u64,
}

/// Why a guarded resource refused a write.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuardedRejection {
    /// The write named a resource this guard does not protect. Tokens are
    /// scoped per resource name and are meaningless across names.
    WrongResource,
    /// The write carried a token older than one already accepted, so its
    /// sender is a former owner.
    StaleFencingToken {
        /// Highest token this guard has accepted.
        highest_accepted: FencingToken,
    },
}

/// Downstream resource protected by fencing tokens.
///
/// This type is not part of the replicated state machine and knows nothing
/// about the lock table, sessions, leases, or logical time. It shares only the
/// token and resource-name vocabulary, which is what makes it usable as
/// independent evidence that a stale former owner is excluded.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuardedResource {
    resource: ResourceName,
    highest_accepted: Option<FencingToken>,
    value: u64,
    accepted_writes: u64,
    refused_writes: u64,
}

impl GuardedResource {
    /// Creates a guard for one resource name with no accepted token.
    #[must_use]
    pub const fn new(resource: ResourceName) -> Self {
        Self {
            resource,
            highest_accepted: None,
            value: 0,
            accepted_writes: 0,
            refused_writes: 0,
        }
    }

    /// Offers a write to the guarded resource.
    ///
    /// A token equal to the highest accepted token is accepted again, because
    /// one uninterrupted tenure performs many writes under one token. A
    /// strictly older token is refused.
    ///
    /// # Errors
    ///
    /// Returns an error when the write names another resource or carries a
    /// token older than one already accepted.
    pub fn apply(&mut self, write: GuardedWrite) -> Result<u64, GuardedRejection> {
        if write.resource != self.resource {
            self.refused_writes = self.refused_writes.saturating_add(1);
            return Err(GuardedRejection::WrongResource);
        }
        if let Some(highest_accepted) = self.highest_accepted {
            if write.token < highest_accepted {
                self.refused_writes = self.refused_writes.saturating_add(1);
                return Err(GuardedRejection::StaleFencingToken { highest_accepted });
            }
        }

        self.highest_accepted = Some(write.token);
        self.value = write.value;
        self.accepted_writes = self.accepted_writes.saturating_add(1);
        Ok(self.value)
    }

    /// Returns the protected resource name.
    #[must_use]
    pub const fn resource(&self) -> ResourceName {
        self.resource
    }

    /// Returns the highest token this guard has accepted.
    #[must_use]
    pub const fn highest_accepted(&self) -> Option<FencingToken> {
        self.highest_accepted
    }

    /// Returns the stored value.
    #[must_use]
    pub const fn value(&self) -> u64 {
        self.value
    }

    /// Returns how many writes were accepted.
    #[must_use]
    pub const fn accepted_writes(&self) -> u64 {
        self.accepted_writes
    }

    /// Returns how many writes were refused.
    #[must_use]
    pub const fn refused_writes(&self) -> u64 {
        self.refused_writes
    }
}
