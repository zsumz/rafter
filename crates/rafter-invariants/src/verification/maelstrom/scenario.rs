//! Reviewed Maelstrom scenario identity and execution requirements.

use crate::verification::AggregateError;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum Scenario {
    Base,
    Membership,
    Restart,
    ApplicationCrash,
    Snapshot,
    LeaseIsolation,
}

impl Scenario {
    pub(super) fn from_check_id(check_id: &str) -> Result<Self, AggregateError> {
        let name = check_id
            .strip_prefix("maelstrom/")
            .ok_or_else(|| error(format!("invalid Maelstrom check ID {check_id}")))?;
        Self::from_name(name).ok_or_else(|| error(format!("unknown Maelstrom scenario {name}")))
    }

    pub(super) fn from_evidence_path(path: &str) -> Option<Self> {
        match path {
            "scripts/maelstrom-lin-kv" => Some(Self::Base),
            "scripts/maelstrom-lin-kv-membership-change" => Some(Self::Membership),
            "scripts/maelstrom-lin-kv-repeated-restart" => Some(Self::Restart),
            "scripts/maelstrom-lin-kv-app-persist-crash" => Some(Self::ApplicationCrash),
            "scripts/maelstrom-lin-kv-forced-snapshot" => Some(Self::Snapshot),
            "scripts/maelstrom-lin-kv-lease-isolation" => Some(Self::LeaseIsolation),
            _ => None,
        }
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Membership => "membership",
            Self::Restart => "restart",
            Self::ApplicationCrash => "app-crash",
            Self::Snapshot => "snapshot",
            Self::LeaseIsolation => "lease-isolation",
        }
    }

    pub(super) fn script(self) -> &'static str {
        match self {
            Self::Base => "scripts/maelstrom-lin-kv",
            Self::Membership => "scripts/maelstrom-lin-kv-membership-change",
            Self::Restart => "scripts/maelstrom-lin-kv-repeated-restart",
            Self::ApplicationCrash => "scripts/maelstrom-lin-kv-app-persist-crash",
            Self::Snapshot => "scripts/maelstrom-lin-kv-forced-snapshot",
            Self::LeaseIsolation => "scripts/maelstrom-lin-kv-lease-isolation",
        }
    }

    pub(super) fn requires_proxy(self) -> bool {
        matches!(
            self,
            Self::Restart | Self::ApplicationCrash | Self::Snapshot | Self::LeaseIsolation
        )
    }

    pub(super) fn requires_durable_state(self) -> bool {
        matches!(
            self,
            Self::Restart | Self::ApplicationCrash | Self::Snapshot
        )
    }

    pub(super) fn concurrency(self) -> &'static str {
        if self == Self::Membership {
            "8"
        } else {
            "6"
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        match name {
            "base" => Some(Self::Base),
            "membership" => Some(Self::Membership),
            "restart" => Some(Self::Restart),
            "app-crash" => Some(Self::ApplicationCrash),
            "snapshot" => Some(Self::Snapshot),
            "lease-isolation" => Some(Self::LeaseIsolation),
            _ => None,
        }
    }
}

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}
