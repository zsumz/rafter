//! Replicated membership-configuration and log-entry vocabulary.

use std::fmt;

use super::{JointMembership, LogIndex, MembershipConfig, MembershipSet, NodeId, SharedPayload};

/// Monotonic identity for committed membership configurations.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ConfigurationId(pub u64);

/// Whether a configuration entry is stable or in joint consensus.
///
/// This enum is exhaustive because Raft membership has only stable and joint
/// phases.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConfigurationPhase {
    /// One voter set determines quorum.
    Stable,
    /// Both old and new voter sets must reach quorum.
    Joint,
}

/// Raft log entry carrying a membership configuration change.
///
/// This enum is exhaustive because configuration entries are either stable or
/// joint.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ConfigurationEntry {
    /// Enter or replace a single stable membership.
    Stable {
        /// Monotonic identity of this configuration.
        config_id: ConfigurationId,
        /// Stable voter and learner set.
        membership: MembershipSet,
    },
    /// Enter joint consensus between old and new memberships.
    Joint {
        /// Monotonic identity of this configuration.
        config_id: ConfigurationId,
        /// Old and new memberships participating in joint quorum.
        membership: JointMembership,
    },
}

/// Committed configuration identity and the log index that committed it.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CommittedConfiguration {
    /// Log position of the committed configuration entry.
    pub index: LogIndex,
    /// Monotonic identity carried by that entry.
    pub config_id: ConfigurationId,
}

/// Replication match-index barrier required before promoting one learner.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct PromotionBarrier {
    /// Learner that may be promoted after catching up.
    pub learner_id: NodeId,
    /// Minimum replicated match index required for promotion.
    pub required_match_index: LogIndex,
}

/// Logical kind stored in a Raft log entry.
///
/// This enum is exhaustive because Raft entries are application data,
/// configuration changes, or leadership no-ops.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum LogEntryKind {
    /// Opaque replicated application command bytes.
    Application(super::SharedPayload),
    /// Replicated membership transition.
    Configuration(ConfigurationEntry),
    /// Leadership no-op used to establish the current term.
    Noop,
}

impl ConfigurationId {
    /// Returns the next configuration id.
    #[must_use]
    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }
}

impl PromotionBarrier {
    /// Builds a learner-promotion barrier at a required match index.
    #[must_use]
    pub const fn new(learner_id: NodeId, required_match_index: LogIndex) -> Self {
        Self {
            learner_id,
            required_match_index,
        }
    }
}

impl fmt::Display for ConfigurationId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "config-{}", self.0)
    }
}

impl fmt::Display for ConfigurationPhase {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stable => write!(formatter, "stable"),
            Self::Joint => write!(formatter, "joint"),
        }
    }
}

impl ConfigurationEntry {
    /// Builds a stable configuration entry.
    #[must_use]
    pub fn stable(config_id: ConfigurationId, membership: MembershipSet) -> Self {
        Self::Stable {
            config_id,
            membership,
        }
    }

    /// Builds a joint configuration entry.
    #[must_use]
    pub fn joint(config_id: ConfigurationId, membership: JointMembership) -> Self {
        Self::Joint {
            config_id,
            membership,
        }
    }

    /// Returns the configuration id carried by this entry.
    #[must_use]
    pub const fn config_id(&self) -> ConfigurationId {
        match self {
            Self::Stable { config_id, .. } | Self::Joint { config_id, .. } => *config_id,
        }
    }

    /// Returns whether this entry is stable or joint.
    #[must_use]
    pub const fn phase(&self) -> ConfigurationPhase {
        match self {
            Self::Stable { .. } => ConfigurationPhase::Stable,
            Self::Joint { .. } => ConfigurationPhase::Joint,
        }
    }

    /// Returns this entry as a quorum-checkable membership configuration.
    #[must_use]
    pub fn membership_config(&self) -> MembershipConfig {
        match self {
            Self::Stable { membership, .. } => MembershipConfig::stable(membership.clone()),
            Self::Joint { membership, .. } => MembershipConfig::Joint(membership.clone()),
        }
    }
}

impl LogEntryKind {
    /// Builds an application entry from opaque payload bytes.
    #[must_use]
    pub fn application<P>(payload: P) -> Self
    where
        P: Into<SharedPayload>,
    {
        Self::Application(payload.into())
    }

    /// Builds a configuration entry.
    #[must_use]
    pub fn configuration(entry: ConfigurationEntry) -> Self {
        Self::Configuration(entry)
    }

    /// Builds a no-op entry.
    #[must_use]
    pub const fn noop() -> Self {
        Self::Noop
    }

    /// Returns whether this is an application entry.
    #[must_use]
    pub const fn is_application(&self) -> bool {
        matches!(self, Self::Application(_))
    }

    /// Returns whether this is a configuration entry.
    #[must_use]
    pub const fn is_configuration(&self) -> bool {
        matches!(self, Self::Configuration(_))
    }

    /// Returns the application payload when this is an application entry.
    #[must_use]
    pub fn application_payload(&self) -> Option<&[u8]> {
        match self {
            Self::Application(payload) => Some(payload),
            Self::Configuration(_) | Self::Noop => None,
        }
    }

    /// Returns the configuration payload when this is a configuration entry.
    #[must_use]
    pub const fn configuration_entry(&self) -> Option<&ConfigurationEntry> {
        match self {
            Self::Configuration(entry) => Some(entry),
            Self::Application(_) | Self::Noop => None,
        }
    }
}
