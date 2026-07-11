use std::{error::Error, fmt};

use rafter::NodeId;

use super::super::Action;
use super::types::{TlaAbstractionGap, TlaAction, TLA_NODE_COUNT};

/// Failure returned when a trace is required to fit the TLA+ action subset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlaProjectionFailure {
    pub(super) action_index: usize,
    pub(super) action: Action,
    pub(super) gap: TlaAbstractionGap,
}

impl TlaProjectionFailure {
    /// Returns the zero-based Rust trace action index.
    #[must_use]
    pub const fn action_index(&self) -> usize {
        self.action_index
    }

    /// Returns the Rust action that could not be projected to a TLA+ action.
    #[must_use]
    pub const fn action(&self) -> &Action {
        &self.action
    }

    /// Returns the named abstraction gap for the action.
    #[must_use]
    pub const fn gap(&self) -> TlaAbstractionGap {
        self.gap
    }
}

impl fmt::Display for TlaProjectionFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "trace action {} `{}` is outside the TLA+ model: {}",
            self.action_index, self.action, self.gap
        )
    }
}

impl Error for TlaProjectionFailure {}

/// Failure returned when a projectable Rust trace cannot be rendered with the
/// bounded TLA+ symbol sets in the generated config.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum TlaTraceRenderError {
    /// The Rust action is outside the current abstract TLA+ action vocabulary.
    Projection(TlaProjectionFailure),
    /// The action references a node that is not present in the generated
    /// `Nodes` constant set.
    NodeOutOfBounds {
        action_index: usize,
        action: TlaAction,
        node_id: NodeId,
    },
    /// The trace needs more distinct client proposal values than the generated
    /// `Values` constant set provides.
    TooManyValues {
        action_index: usize,
        action: TlaAction,
        requested_value: usize,
        available_values: usize,
    },
    /// The trace needs more distinct read request symbols than the generated
    /// `ReadRequests` constant set provides.
    TooManyReadRequests {
        action_index: usize,
        action: TlaAction,
        requested_read_request: usize,
        available_read_requests: usize,
    },
}

impl fmt::Display for TlaTraceRenderError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Projection(failure) => failure.fmt(formatter),
            Self::NodeOutOfBounds {
                action_index,
                action,
                node_id,
            } => write!(
                formatter,
                "trace action {action_index} `{action}` references {node_id} (n{}), but the generated TLA+ config defines only n1..n{TLA_NODE_COUNT}",
                node_id.0
            ),
            Self::TooManyValues {
                action_index,
                action,
                requested_value,
                available_values,
            } => write!(
                formatter,
                "trace action {action_index} `{action}` needs proposal value v{requested_value}, but the generated TLA+ config defines only {available_values} Values"
            ),
            Self::TooManyReadRequests {
                action_index,
                action,
                requested_read_request,
                available_read_requests,
            } => write!(
                formatter,
                "trace action {action_index} `{action}` needs read request r{requested_read_request}, but the generated TLA+ config defines only {available_read_requests} ReadRequests"
            ),
        }
    }
}

impl Error for TlaTraceRenderError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Projection(failure) => Some(failure),
            Self::NodeOutOfBounds { .. }
            | Self::TooManyValues { .. }
            | Self::TooManyReadRequests { .. } => None,
        }
    }
}

impl From<TlaProjectionFailure> for TlaTraceRenderError {
    fn from(failure: TlaProjectionFailure) -> Self {
        Self::Projection(failure)
    }
}
