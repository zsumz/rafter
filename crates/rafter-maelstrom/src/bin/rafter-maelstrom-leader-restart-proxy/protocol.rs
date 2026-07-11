use std::time::Duration;

use serde_json::Value;

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
}
