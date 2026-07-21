//! Strict parsing and observation counting for lease-isolation log markers.

use std::collections::{BTreeMap, BTreeSet};

use super::super::model::ScenarioMarkers;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::producer) struct LeaseMarker {
    pub(in crate::producer) source_node: String,
    pub(in crate::producer) seq: u64,
    pub(in crate::producer) node: String,
    pub(in crate::producer) term: u64,
    pub(in crate::producer) phase: String,
    pub(in crate::producer) client: String,
    pub(in crate::producer) msg_id: u64,
    pub(in crate::producer) code: Option<u64>,
    pub(in crate::producer) reason: Option<String>,
}

impl LeaseMarker {
    pub(in crate::producer) fn parse(line: &str, source_node: &str) -> Result<Self, ()> {
        let fields = line
            .strip_prefix("rafter-maelstrom lease-isolation ")
            .ok_or(())?
            .split_ascii_whitespace()
            .try_fold(BTreeMap::new(), |mut fields, part| {
                let (name, value) = part.split_once('=').ok_or(())?;
                if fields.insert(name, value).is_some() {
                    return Err(());
                }
                Ok(fields)
            })?;
        let allowed = BTreeSet::from([
            "seq", "node", "term", "phase", "client", "msg_id", "code", "reason",
        ]);
        if fields.keys().any(|field| !allowed.contains(field)) {
            return Err(());
        }
        let phase = required(&fields, "phase")?.to_owned();
        let code = fields
            .get("code")
            .map(|value| value.parse())
            .transpose()
            .map_err(|_| ())?;
        let reason = fields.get("reason").map(|value| (*value).to_owned());
        let known_phase = matches!(
            phase.as_str(),
            "fast-path-read-ok"
                | "read-buffered"
                | "lease-expired"
                | "post-expiry-released"
                | "post-expiry-handler"
                | "post-expiry-unavailable"
                | "post-expiry-read-served-violation"
                | "post-expiry-renewed-violation"
                | "post-expiry-unexpected-error"
                | "post-expiry-duplicate-terminal"
                | "coverage-lost"
        );
        if !known_phase
            || (phase == "post-expiry-unexpected-error") != code.is_some()
            || (phase == "coverage-lost") != reason.is_some()
        {
            return Err(());
        }
        Ok(Self {
            source_node: source_node.to_owned(),
            seq: required(&fields, "seq")?.parse().map_err(|_| ())?,
            node: required(&fields, "node")?.to_owned(),
            term: required(&fields, "term")?.parse().map_err(|_| ())?,
            phase,
            client: required(&fields, "client")?.to_owned(),
            msg_id: required(&fields, "msg_id")?.parse().map_err(|_| ())?,
            code,
            reason,
        })
    }

    pub(super) fn request(&self) -> (&str, u64) {
        (&self.client, self.msg_id)
    }
}

fn required<'a>(fields: &'a BTreeMap<&str, &str>, name: &str) -> Result<&'a str, ()> {
    fields.get(name).copied().ok_or(())
}

pub(super) fn bump_lease_count(markers: &mut ScenarioMarkers, phase: &str) {
    let counter = match phase {
        "fast-path-read-ok" => &mut markers.lease_fast_path_read_ok,
        "read-buffered" => &mut markers.lease_read_buffered,
        "lease-expired" => &mut markers.lease_expired_while_leader,
        "post-expiry-released" => &mut markers.lease_post_expiry_released,
        "post-expiry-handler" => &mut markers.lease_post_expiry_handler,
        "post-expiry-unavailable" => &mut markers.lease_post_expiry_unavailable,
        "post-expiry-read-served-violation" => &mut markers.lease_post_expiry_read_served,
        "post-expiry-renewed-violation" => &mut markers.lease_post_expiry_renewed,
        "post-expiry-unexpected-error" => &mut markers.lease_post_expiry_unexpected_error,
        "post-expiry-duplicate-terminal" => &mut markers.lease_duplicate_terminal,
        "coverage-lost" => &mut markers.lease_coverage_lost,
        _ => return,
    };
    *counter += 1;
}
