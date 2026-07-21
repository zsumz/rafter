//! Parsing and counting of bounded lease-isolation node-log markers.

use std::collections::{BTreeMap, BTreeSet};

use crate::verification::AggregateError;

const SIMPLE_MARKERS: [(&str, &str); 5] = [
    ("membership_enter", "action=enter-joint"),
    ("membership_leave", "action=leave-joint"),
    ("membership_complete", "complete target="),
    ("snapshots_compacted", "compacted snapshot"),
    ("snapshots_applied", "applied snapshot"),
];

pub(super) const LIMITS: Limits = Limits {
    events: 16_384,
    line_bytes: 16 * 1024,
};

#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) events: usize,
    pub(crate) line_bytes: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LeaseMarker {
    pub(super) source_node: String,
    pub(super) sequence: u64,
    pub(super) node: String,
    pub(super) term: u64,
    pub(super) phase: String,
    pub(super) client: String,
    pub(super) message: u64,
    pub(super) code: Option<u64>,
    pub(super) reason: Option<String>,
}

impl LeaseMarker {
    pub(super) fn parse(line: &str, source_node: &str) -> Result<Self, ()> {
        let body = line
            .strip_prefix("rafter-maelstrom lease-isolation ")
            .ok_or(())?;
        let mut fields = BTreeMap::new();
        for component in body.split_ascii_whitespace() {
            let (key, value) = component.split_once('=').ok_or(())?;
            if fields.insert(key, value).is_some() {
                return Err(());
            }
        }
        let allowed = BTreeSet::from([
            "seq", "node", "term", "phase", "client", "msg_id", "code", "reason",
        ]);
        if fields.keys().any(|key| !allowed.contains(key)) {
            return Err(());
        }
        let phase = take(&fields, "phase")?.to_owned();
        let code = fields
            .get("code")
            .map(|value| value.parse::<u64>())
            .transpose()
            .map_err(|_| ())?;
        let reason = fields.get("reason").map(|value| (*value).to_owned());
        if !known_phase(&phase)
            || (phase == "post-expiry-unexpected-error") != code.is_some()
            || (phase == "coverage-lost") != reason.is_some()
        {
            return Err(());
        }
        Ok(Self {
            source_node: source_node.to_owned(),
            sequence: take(&fields, "seq")?.parse().map_err(|_| ())?,
            node: take(&fields, "node")?.to_owned(),
            term: take(&fields, "term")?.parse().map_err(|_| ())?,
            phase,
            client: take(&fields, "client")?.to_owned(),
            message: take(&fields, "msg_id")?.parse().map_err(|_| ())?,
            code,
            reason,
        })
    }

    pub(super) fn identity(&self) -> (&str, u64) {
        (&self.client, self.message)
    }
}

pub(crate) fn scan(
    source: &str,
    source_node: &str,
    values: &mut BTreeMap<&'static str, u64>,
    lease_events: &mut Vec<LeaseMarker>,
    lease_parse_errors: &mut u64,
) -> Result<(), AggregateError> {
    scan_with_limits(
        source,
        source_node,
        values,
        lease_events,
        lease_parse_errors,
        LIMITS,
    )
}

pub(crate) fn scan_with_limits(
    source: &str,
    source_node: &str,
    values: &mut BTreeMap<&'static str, u64>,
    lease_events: &mut Vec<LeaseMarker>,
    lease_parse_errors: &mut u64,
    limits: Limits,
) -> Result<(), AggregateError> {
    let mut saw_restart = false;
    let mut saw_crash = false;
    for line in source.lines() {
        for (name, needle) in SIMPLE_MARKERS {
            bump(values, name, line.contains(needle));
        }
        if line.contains("proxy restarting child") {
            bump(values, "restarts", true);
            saw_restart = true;
        }
        if line.contains("crashpoint=RAFTER_MAELSTROM_CRASH_AFTER_APP_PERSIST_ONCE fired") {
            bump(values, "crashpoints", true);
            saw_crash = true;
        }
        let progress = line.contains(" role=leader ") || line.contains("compacted snapshot");
        bump(values, "post_restart_progress", saw_restart && progress);
        bump(values, "post_crash_progress", saw_crash && progress);
        bump(
            values,
            "post_restart_snapshots_applied",
            saw_restart && line.contains("applied snapshot"),
        );
        if line.starts_with("rafter-maelstrom lease-isolation ") {
            if line.len() > limits.line_bytes {
                return Err(error(format!(
                    "Maelstrom lease marker exceeds {} bytes",
                    limits.line_bytes
                )));
            }
            if lease_events.len() == limits.events {
                return Err(error(format!(
                    "Maelstrom lease marker count exceeds {}",
                    limits.events
                )));
            }
            match LeaseMarker::parse(line, source_node) {
                Ok(event) => {
                    bump_phase(values, &event.phase);
                    lease_events.push(event);
                }
                Err(()) => *lease_parse_errors += 1,
            }
        }
    }
    Ok(())
}

fn take<'a>(fields: &'a BTreeMap<&str, &str>, key: &str) -> Result<&'a str, ()> {
    fields.get(key).copied().ok_or(())
}

fn known_phase(phase: &str) -> bool {
    matches!(
        phase,
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
    )
}

fn bump_phase(values: &mut BTreeMap<&'static str, u64>, phase: &str) {
    let name = match phase {
        "fast-path-read-ok" => "lease_fast_path_read_ok",
        "read-buffered" => "lease_read_buffered",
        "lease-expired" => "lease_expired_while_leader",
        "post-expiry-released" => "lease_post_expiry_released",
        "post-expiry-handler" => "lease_post_expiry_handler",
        "post-expiry-unavailable" => "lease_post_expiry_unavailable",
        "post-expiry-read-served-violation" => "lease_post_expiry_read_served",
        "post-expiry-renewed-violation" => "lease_post_expiry_renewed",
        "post-expiry-unexpected-error" => "lease_post_expiry_unexpected_error",
        "post-expiry-duplicate-terminal" => "lease_duplicate_terminal",
        "coverage-lost" => "lease_coverage_lost",
        _ => return,
    };
    *values.entry(name).or_default() += 1;
}

fn bump(values: &mut BTreeMap<&'static str, u64>, name: &'static str, matched: bool) {
    *values.entry(name).or_default() += u64::from(matched);
}

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}
