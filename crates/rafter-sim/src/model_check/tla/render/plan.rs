use rafter::NodeId;

use super::super::errors::TlaTraceRenderError;
use super::super::types::{TlaAction, TLA_NODE_COUNT, TLA_READ_REQUEST_SYMBOLS, TLA_VALUE_SYMBOLS};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum TlaRenderedAction {
    Timeout {
        node_id: NodeId,
    },
    ClientAppend {
        node_id: NodeId,
        value: &'static str,
    },
    RegisterRead {
        node_id: NodeId,
        read_request: &'static str,
    },
    Restart {
        node_id: NodeId,
    },
    DeliverRequestVote {
        from: NodeId,
        to: NodeId,
    },
    DeliverAppend {
        from: NodeId,
        to: NodeId,
    },
}

pub(super) fn plan_tla_trace_render(
    actions: &[TlaAction],
) -> Result<Vec<TlaRenderedAction>, TlaTraceRenderError> {
    let mut value_count = 0;
    let mut read_request_count = 0;
    let mut rendered = Vec::with_capacity(actions.len());

    for (action_index, action) in actions.iter().copied().enumerate() {
        rendered.push(match action {
            TlaAction::Timeout { node_id } => {
                validate_tla_node(action_index, action, node_id)?;
                TlaRenderedAction::Timeout { node_id }
            }
            TlaAction::ClientAppend { node_id } => {
                validate_tla_node(action_index, action, node_id)?;
                let value = TLA_VALUE_SYMBOLS.get(value_count).copied().ok_or(
                    TlaTraceRenderError::TooManyValues {
                        action_index,
                        action,
                        requested_value: value_count + 1,
                        available_values: TLA_VALUE_SYMBOLS.len(),
                    },
                )?;
                value_count += 1;
                TlaRenderedAction::ClientAppend { node_id, value }
            }
            TlaAction::RegisterRead { node_id } => {
                validate_tla_node(action_index, action, node_id)?;
                let read_request = TLA_READ_REQUEST_SYMBOLS
                    .get(read_request_count)
                    .copied()
                    .ok_or(TlaTraceRenderError::TooManyReadRequests {
                        action_index,
                        action,
                        requested_read_request: read_request_count + 1,
                        available_read_requests: TLA_READ_REQUEST_SYMBOLS.len(),
                    })?;
                read_request_count += 1;
                TlaRenderedAction::RegisterRead {
                    node_id,
                    read_request,
                }
            }
            TlaAction::Restart { node_id } => {
                validate_tla_node(action_index, action, node_id)?;
                TlaRenderedAction::Restart { node_id }
            }
            TlaAction::DeliverRequestVote { from, to } => {
                validate_tla_node(action_index, action, from)?;
                validate_tla_node(action_index, action, to)?;
                TlaRenderedAction::DeliverRequestVote { from, to }
            }
            TlaAction::DeliverAppend { from, to } => {
                validate_tla_node(action_index, action, from)?;
                validate_tla_node(action_index, action, to)?;
                TlaRenderedAction::DeliverAppend { from, to }
            }
        });
    }

    Ok(rendered)
}

fn validate_tla_node(
    action_index: usize,
    action: TlaAction,
    node_id: NodeId,
) -> Result<(), TlaTraceRenderError> {
    if (1..=TLA_NODE_COUNT).contains(&node_id.0) {
        Ok(())
    } else {
        Err(TlaTraceRenderError::NodeOutOfBounds {
            action_index,
            action,
            node_id,
        })
    }
}
