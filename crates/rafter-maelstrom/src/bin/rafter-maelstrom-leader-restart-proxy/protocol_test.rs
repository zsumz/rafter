//! Reading one fact per line from Maelstrom stdin and the child's pipes.
//!
//! Body types, the init node identity, structured leader and lease markers, and
//! correlated client requests and responses must each be recognized from the
//! exact line carrying them, so a line that answers no question yields silence
//! rather than a wrong fact.

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
        client_response(r#"{"src":"n1","dest":"c0","body":{"type":"read_ok","in_reply_to":41}}"#),
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
