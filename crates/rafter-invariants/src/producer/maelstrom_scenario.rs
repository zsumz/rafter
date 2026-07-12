#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum Scenario {
    Base,
    Membership,
    Restart,
    AppCrash,
    Snapshot,
}

pub(super) fn scenario_for(descriptor: &EvidenceDescriptor) -> Option<Scenario> {
    match descriptor.path.as_str() {
        "scripts/maelstrom-lin-kv" => Some(Scenario::Base),
        "scripts/maelstrom-lin-kv-membership-change" => Some(Scenario::Membership),
        "scripts/maelstrom-lin-kv-repeated-restart" => Some(Scenario::Restart),
        "scripts/maelstrom-lin-kv-app-persist-crash" => Some(Scenario::AppCrash),
        "scripts/maelstrom-lin-kv-forced-snapshot" => Some(Scenario::Snapshot),
        _ => None,
    }
}

pub(super) fn required_configuration<'a>(
    configuration: &'a BTreeMap<String, String>,
    key: &str,
) -> Result<&'a str, String> {
    configuration
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| format!("Maelstrom configuration omitted {key}"))
}

impl Scenario {
    pub(super) const ALL: [Self; 5] = [
        Self::Base,
        Self::Membership,
        Self::Restart,
        Self::AppCrash,
        Self::Snapshot,
    ];

    pub(super) const fn name(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Membership => "membership",
            Self::Restart => "restart",
            Self::AppCrash => "app-crash",
            Self::Snapshot => "snapshot",
        }
    }

    pub(super) const fn script(self) -> &'static str {
        match self {
            Self::Base => "scripts/maelstrom-lin-kv",
            Self::Membership => "scripts/maelstrom-lin-kv-membership-change",
            Self::Restart => "scripts/maelstrom-lin-kv-repeated-restart",
            Self::AppCrash => "scripts/maelstrom-lin-kv-app-persist-crash",
            Self::Snapshot => "scripts/maelstrom-lin-kv-forced-snapshot",
        }
    }

    pub(super) const fn concurrency(self) -> &'static str {
        if matches!(self, Self::Membership) {
            "8"
        } else {
            "6"
        }
    }
}
use std::collections::BTreeMap;

use crate::EvidenceDescriptor;
