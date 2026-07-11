use rafter::NodeId;

use super::plan::TlaRenderedAction;

pub(super) fn render_tla_action_formula(action: TlaRenderedAction) -> String {
    match action {
        TlaRenderedAction::Timeout { node_id } => {
            format!("  /\\ Timeout({})\n", tla_node(node_id))
        }
        TlaRenderedAction::ClientAppend { node_id, value } => {
            format!("  /\\ ClientAppend({}, {})\n", tla_node(node_id), value)
        }
        TlaRenderedAction::RegisterRead {
            node_id,
            read_request,
        } => format!(
            "  /\\ RegisterRead({}, {})\n",
            tla_node(node_id),
            read_request
        ),
        TlaRenderedAction::Restart { node_id } => {
            format!("  /\\ Restart({})\n", tla_node(node_id))
        }
        TlaRenderedAction::DeliverRequestVote { from, to } => {
            render_tla_deliver_formula("RequestVote", "DeliverRequestVote", from, to)
        }
        TlaRenderedAction::DeliverAppend { from, to } => {
            render_tla_deliver_formula("AppendEntries", "DeliverAppend", from, to)
        }
    }
}

fn render_tla_deliver_formula(
    message_type: &str,
    action_name: &str,
    from: NodeId,
    to: NodeId,
) -> String {
    format!(
        "  /\\ \\E m \\in messages :\n     /\\ m.type = {message_type}\n     /\\ m.from = {}\n     /\\ m.to = {}\n     /\\ {action_name}(m)\n",
        tla_node(from),
        tla_node(to)
    )
}

fn tla_node(node_id: NodeId) -> String {
    format!("n{}", node_id.0)
}
