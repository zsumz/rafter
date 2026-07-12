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
    if code == 11 {
        ClientResponse::TemporarilyUnavailable
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
mod tests {
    use super::*;

    #[test]
    fn detects_init_and_init_ok_body_types() {
        assert_eq!(
            body_type(r#"{"src":"c0","dest":"n1","body":{"type":"init"}}"#),
            Some("init".to_string())
        );
        assert_eq!(
            body_type(r#"{"src":"n1","dest":"c0","body":{"type":"init_ok"}}"#),
            Some("init_ok".to_string())
        );
        assert_eq!(body_type("not json"), None);
    }

    #[test]
    fn detects_init_node_id_for_staggered_restarts() {
        assert_eq!(
            init_node_id(r#"{"src":"c0","dest":"n2","body":{"type":"init","node_id":"n2"}}"#),
            Some("n2".to_string())
        );
        assert_eq!(node_restart_stagger("n2"), Duration::from_millis(250));
    }

    #[test]
    fn detects_structured_leader_marker() {
        assert!(reports_leader(
            "rafter-maelstrom role node=n1 role=leader term=3"
        ));
        assert!(!reports_leader(
            "rafter-maelstrom role node=n1 role=follower term=3"
        ));
    }

    #[test]
    fn parses_client_request_and_correlated_read_responses() {
        assert_eq!(
            client_request(r#"{"src":"c0","dest":"n1","body":{"type":"read","msg_id":41}}"#),
            Some((RequestId::new("c0", 41), true))
        );
        assert_eq!(
            client_request(
                r#"{"src":"n2","dest":"n1","body":{"type":"client_forward","client":"c0","in_reply_to":41,"request":{"type":"read"}}}"#
            ),
            Some((RequestId::new("c0", 41), false))
        );
        assert_eq!(
            client_response(
                r#"{"src":"n1","dest":"c0","body":{"type":"read_ok","in_reply_to":41}}"#
            ),
            Some((RequestId::new("c0", 41), ClientResponse::ReadOk))
        );
        assert_eq!(
            client_response(
                r#"{"src":"n1","dest":"c0","body":{"type":"error","in_reply_to":41,"code":20}}"#
            ),
            Some((
                RequestId::new("c0", 41),
                ClientResponse::UnexpectedError(20)
            ))
        );
        assert_eq!(
            client_response(
                r#"{"src":"n1","dest":"c0","body":{"type":"error","in_reply_to":41,"code":11}}"#
            ),
            Some((
                RequestId::new("c0", 41),
                ClientResponse::TemporarilyUnavailable
            ))
        );
        assert_eq!(
            client_response(
                r#"{"src":"n1","dest":"n2","body":{"type":"client_result","client":"c0","in_reply_to":41,"result":{"kind":"read_ok","value":7}}}"#
            ),
            Some((RequestId::new("c0", 41), ClientResponse::ReadOk))
        );
    }

    #[test]
    fn parses_structured_lease_markers() {
        assert_eq!(
            lease_state("rafter-maelstrom lease node=n1 state=inactive role=leader term=3"),
            Some(LeaseState {
                active: false,
                leader: true,
                term: 3
            })
        );
        assert_eq!(
            lease_read("rafter-maelstrom lease-read node=n1 phase=request role=leader term=3 active=false client=c0 msg_id=41"),
            Some(LeaseRead {
                request: RequestId::new("c0", 41),
                active: false,
                leader: true,
                term: 3
            })
        );
        assert_eq!(
            role_state("rafter-maelstrom role node=n1 role=follower term=4"),
            Some(RoleState {
                leader: false,
                term: 4
            })
        );
    }
}
