//! Replicated log-entry vocabulary and replication-size accounting.
//!
//! Entry constructors expose the three logical Raft entry kinds. Size
//! accounting is a conservative transport budget, not a wire-format contract.

use crate::{ConfigurationEntry, LogEntryKind, MembershipConfig, SharedPayload, Term};

const APPLICATION_LOG_ENTRY_REPLICATION_OVERHEAD_BYTES: usize = 64;
const NOOP_LOG_ENTRY_REPLICATION_BYTES: usize = 16;

// Conservative upper bound on the wire encoding of a configuration entry:
// a fixed header plus a per-member cost across every voter and learner in
// both joint halves. Pinned as an upper bound of the real encoding by
// rafter-codec's configuration_entry_size_accounting_is_upper_bound test.
const CONFIGURATION_LOG_ENTRY_BASE_BYTES: usize = 64;
const CONFIGURATION_LOG_ENTRY_PER_MEMBER_BYTES: usize = 12;

/// One Raft log entry: term plus logical entry kind.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LogEntry {
    pub term: Term,
    pub kind: LogEntryKind,
}

impl LogEntry {
    /// Builds an application log entry.
    #[must_use]
    pub fn application<P>(term: Term, payload: P) -> Self
    where
        P: Into<SharedPayload>,
    {
        Self {
            term,
            kind: LogEntryKind::application(payload),
        }
    }

    /// Builds a configuration log entry.
    #[must_use]
    pub fn configuration(term: Term, configuration: ConfigurationEntry) -> Self {
        Self {
            term,
            kind: LogEntryKind::configuration(configuration),
        }
    }

    /// Builds a leadership no-op log entry.
    #[must_use]
    pub const fn noop(term: Term) -> Self {
        Self {
            term,
            kind: LogEntryKind::noop(),
        }
    }

    /// Returns the application payload when this is an application entry.
    #[must_use]
    pub fn application_payload(&self) -> Option<&[u8]> {
        self.kind.application_payload()
    }

    /// Returns the configuration payload when this is a configuration entry.
    #[must_use]
    pub fn configuration_entry(&self) -> Option<&ConfigurationEntry> {
        self.kind.configuration_entry()
    }

    #[must_use]
    pub(crate) fn application_replication_bytes(payload_len: usize) -> usize {
        payload_len.saturating_add(APPLICATION_LOG_ENTRY_REPLICATION_OVERHEAD_BYTES)
    }

    #[must_use]
    pub(crate) fn max_application_payload_len(max_replication_bytes: usize) -> usize {
        max_replication_bytes.saturating_sub(APPLICATION_LOG_ENTRY_REPLICATION_OVERHEAD_BYTES)
    }

    /// Size this entry contributes to the append-entries batching target: an
    /// upper bound of its wire encoding.
    ///
    /// This is batch accounting, not a maximum encoded-frame size. A permitted
    /// individual entry may exceed the target. Transport receive limits must
    /// independently accommodate the largest permitted application entry plus
    /// append-frame overhead, and snapshot metadata plus a full chunk. See
    /// `rafter-codec/WIRE_FORMAT_V1.md` for the peer-frame sizing contract.
    #[must_use]
    pub fn replication_bytes(&self) -> usize {
        match &self.kind {
            LogEntryKind::Application(payload) => {
                Self::application_replication_bytes(payload.len())
            }
            LogEntryKind::Configuration(entry) => {
                let members = match entry.membership_config() {
                    MembershipConfig::Stable(membership) => {
                        membership.voters().len() + membership.learners().len()
                    }
                    MembershipConfig::Joint(joint) => {
                        joint.old().voters().len()
                            + joint.old().learners().len()
                            + joint.new_membership().voters().len()
                            + joint.new_membership().learners().len()
                    }
                };
                CONFIGURATION_LOG_ENTRY_BASE_BYTES.saturating_add(
                    CONFIGURATION_LOG_ENTRY_PER_MEMBER_BYTES.saturating_mul(members),
                )
            }
            LogEntryKind::Noop => NOOP_LOG_ENTRY_REPLICATION_BYTES,
        }
    }
}
