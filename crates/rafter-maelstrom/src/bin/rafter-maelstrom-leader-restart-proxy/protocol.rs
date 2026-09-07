//! Reading Maelstrom's stdin and the child's two pipes as facts.
//!
//! Each parser answers one question about one line and returns nothing when
//! the line does not answer it, so a log format the child changed degrades
//! into silence rather than into a wrong fact. It holds no state and decides
//! nothing; the supervisor and the lease machine do that.

use std::time::Duration;

use serde_json::Value;

use super::lease_isolation::{ClientResponse, RequestId};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct LeaseState {
    pub active: bool,
    pub leader: bool,
    pub term: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LeaseRead {
    pub request: RequestId,
    pub active: bool,
    pub leader: bool,
    pub term: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct RoleState {
    pub leader: bool,
    pub term: u64,
}

pub(super) fn reports_leader(line: &str) -> bool {
    line.contains("rafter-maelstrom role ") && line.contains(" role=leader ")
}

pub(super) fn init_node_id(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    value
        .get("body")?
        .get("node_id")?
        .as_str()
        .map(str::to_string)
}

pub(super) fn node_restart_stagger(node_id: &str) -> Duration {
    let ordinal = node_id
        .chars()
        .filter(char::is_ascii_digit)
        .collect::<String>()
        .parse::<u64>()
        .unwrap_or(0);
    Duration::from_millis(ordinal.saturating_mul(125))
}

pub(super) fn body_type(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    value.get("body")?.get("type")?.as_str().map(str::to_string)
}

pub(super) fn client_request(line: &str) -> Option<(RequestId, bool)> {
    let value: Value = serde_json::from_str(line).ok()?;
    let body = value.get("body")?;
    match body.get("type")?.as_str()? {
        "read" => Some((
            RequestId::new(value.get("src")?.as_str()?, body.get("msg_id")?.as_u64()?),
            true,
        )),
        "client_forward" if body.get("request")?.get("type")?.as_str()? == "read" => Some((
            RequestId::new(
                body.get("client")?.as_str()?,
                body.get("in_reply_to")?.as_u64()?,
            ),
            false,
        )),
        _ => None,
    }
}

pub(super) fn client_response(line: &str) -> Option<(RequestId, ClientResponse)> {
    let value: Value = serde_json::from_str(line).ok()?;
    let body = value.get("body")?;
    let msg_id = body.get("in_reply_to")?.as_u64()?;
    match body.get("type")?.as_str()? {
        "read_ok" => Some((
            RequestId::new(value.get("dest")?.as_str()?, msg_id),
            ClientResponse::ReadOk,
        )),
        "error" => Some((
            RequestId::new(value.get("dest")?.as_str()?, msg_id),
            error_response(body.get("code")?.as_u64()?),
        )),
        "client_result" => {
            let request = RequestId::new(body.get("client")?.as_str()?, msg_id);
            let result = body.get("result")?;
            match result.get("kind")?.as_str()? {
                "read_ok" => Some((request, ClientResponse::ReadOk)),
                "error" => Some((request, error_response(result.get("code")?.as_u64()?))),
                _ => None,
            }
        }
        _ => None,
    }
}

pub(super) fn lease_state(line: &str) -> Option<LeaseState> {
    line.starts_with("rafter-maelstrom lease ").then_some(())?;
    Some(LeaseState {
        active: field(line, "state")? == "active",
        leader: field(line, "role")? == "leader",
        term: field(line, "term")?.parse().ok()?,
    })
}

pub(super) fn lease_read(line: &str) -> Option<LeaseRead> {
    line.starts_with("rafter-maelstrom lease-read ")
        .then_some(())?;
    Some(LeaseRead {
        request: RequestId::new(field(line, "client")?, field(line, "msg_id")?.parse().ok()?),
        active: field(line, "active")? == "true",
        leader: field(line, "role")? == "leader",
        term: field(line, "term")?.parse().ok()?,
    })
}

pub(super) fn role_state(line: &str) -> Option<RoleState> {
    line.starts_with("rafter-maelstrom role ").then_some(())?;
    Some(RoleState {
        leader: field(line, "role")? == "leader",
        term: field(line, "term")?.parse().ok()?,
    })
}

fn error_response(code: u64) -> ClientResponse {
    if matches!(code, 0 | 11) {
        ClientResponse::Unavailable(code)
    } else {
        ClientResponse::UnexpectedError(code)
    }
}

fn field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}=");
    line.split_ascii_whitespace()
        .find_map(|part| part.strip_prefix(&prefix))
}

#[cfg(test)]
#[path = "protocol_test.rs"]
mod tests;
