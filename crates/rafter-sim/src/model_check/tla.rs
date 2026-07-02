use std::{error::Error, fmt};

use rafter::NodeId;

use super::{Action, MessageKind};

const TLA_NODE_COUNT: u64 = 3;
const TLA_VALUE_SYMBOLS: [&str; 2] = ["v1", "v2"];
const TLA_READ_REQUEST_SYMBOLS: [&str; 2] = ["r1", "r2"];

/// Abstract TLA+ action vocabulary that a Rust simulator trace can project to.
///
/// This enum is exhaustive because it mirrors the current supported abstract
/// action subset in `specs/tla/raft/Raft.tla`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlaAction {
    Timeout { node_id: NodeId },
    ClientAppend { node_id: NodeId },
    RegisterRead { node_id: NodeId },
    Restart { node_id: NodeId },
    DeliverRequestVote { from: NodeId, to: NodeId },
    DeliverAppend { from: NodeId, to: NodeId },
}

impl TlaAction {
    /// Returns the corresponding action name in `specs/tla/raft/Raft.tla`.
    #[must_use]
    pub const fn tla_name(self) -> &'static str {
        match self {
            Self::Timeout { .. } => "Timeout",
            Self::ClientAppend { .. } => "ClientAppend",
            Self::RegisterRead { .. } => "RegisterRead",
            Self::Restart { .. } => "Restart",
            Self::DeliverRequestVote { .. } => "DeliverRequestVote",
            Self::DeliverAppend { .. } => "DeliverAppend",
        }
    }
}

impl fmt::Display for TlaAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout { node_id } => write!(formatter, "Timeout({node_id})"),
            Self::ClientAppend { node_id } => write!(formatter, "ClientAppend({node_id})"),
            Self::RegisterRead { node_id } => write!(formatter, "RegisterRead({node_id})"),
            Self::Restart { node_id } => write!(formatter, "Restart({node_id})"),
            Self::DeliverRequestVote { from, to } => {
                write!(formatter, "DeliverRequestVote({from}->{to})")
            }
            Self::DeliverAppend { from, to } => write!(formatter, "DeliverAppend({from}->{to})"),
        }
    }
}

/// Named reason a Rust trace step is outside the current abstract TLA+ model.
///
/// This enum is exhaustive because abstraction gaps are reported from a closed
/// set of known unsupported trace shapes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlaAbstractionGap {
    RequestVoteResponse,
    AppendEntriesResponse,
    SnapshotTransfer,
    PreVote,
    MembershipChange,
}

impl TlaAbstractionGap {
    /// Returns a stable identifier for the abstraction gap.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::RequestVoteResponse => "request_vote_response_abstracted",
            Self::AppendEntriesResponse => "append_entries_response_abstracted",
            Self::SnapshotTransfer => "snapshot_transfer_not_in_tla_model",
            Self::PreVote => "pre_vote_not_in_tla_model",
            Self::MembershipChange => "membership_change_not_in_tla_model",
        }
    }
}

impl fmt::Display for TlaAbstractionGap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

/// Projection result for one Rust model-checking action.
///
/// This enum is exhaustive because every projection is either an abstract TLA+
/// action or a named abstraction gap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlaProjection {
    Action(TlaAction),
    Gap(TlaAbstractionGap),
}

/// A Rust trace step paired with its TLA+ projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlaTraceStep {
    rust_action: Action,
    projection: TlaProjection,
}

impl TlaTraceStep {
    /// Returns the original Rust model-checking action.
    #[must_use]
    pub const fn rust_action(&self) -> &Action {
        &self.rust_action
    }

    /// Returns the TLA+ action or named abstraction gap for this step.
    #[must_use]
    pub const fn projection(&self) -> TlaProjection {
        self.projection
    }
}

/// Failure returned when a trace is required to fit the TLA+ action subset.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlaProjectionFailure {
    action_index: usize,
    action: Action,
    gap: TlaAbstractionGap,
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

/// TLC-checkable TLA+ module and config generated from a projected Rust trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlaTraceSpec {
    module: String,
    config: String,
}

impl TlaTraceSpec {
    /// Returns the generated TLA+ module text.
    #[must_use]
    pub fn module(&self) -> &str {
        &self.module
    }

    /// Returns the generated TLC config text.
    #[must_use]
    pub fn config(&self) -> &str {
        &self.config
    }
}

/// Projects a Rust model-checking trace into the abstract TLA+ action boundary.
#[must_use]
pub fn project_raft_trace_to_tla(trace: &[Action]) -> Vec<TlaTraceStep> {
    trace
        .iter()
        .cloned()
        .map(|rust_action| {
            let projection = project_raft_action_to_tla(&rust_action);
            TlaTraceStep {
                rust_action,
                projection,
            }
        })
        .collect()
}

/// Returns TLA+ actions only when every Rust trace step fits the abstract model.
///
/// # Errors
///
/// Returns [`TlaProjectionFailure`] at the first Rust action that belongs to an
/// implementation-level behavior the current TLA+ spec intentionally omits.
pub fn require_tla_projectable_raft_trace(
    trace: &[Action],
) -> Result<Vec<TlaAction>, TlaProjectionFailure> {
    trace
        .iter()
        .cloned()
        .enumerate()
        .map(
            |(action_index, action)| match project_raft_action_to_tla(&action) {
                TlaProjection::Action(tla_action) => Ok(tla_action),
                TlaProjection::Gap(gap) => Err(TlaProjectionFailure {
                    action_index,
                    action,
                    gap,
                }),
            },
        )
        .collect()
}

/// Renders a projected Rust trace as a small TLA+ wrapper around `Raft.tla`.
///
/// The generated module adds a `traceStep` program counter and constrains
/// `TraceNext` to follow the projected action sequence exactly. TLC can then
/// run the wrapper with deadlock checking enabled: if a projected action is not
/// enabled by the abstract model at its step, the trace deadlocks before
/// reaching the final stuttering state.
///
/// # Errors
///
/// Returns [`TlaProjectionFailure`] if any trace action belongs to a named
/// abstraction gap outside the current `Raft.tla` subset.
pub fn render_tla_trace_spec(
    module_name: &str,
    trace: &[Action],
) -> Result<TlaTraceSpec, TlaTraceRenderError> {
    let actions = require_tla_projectable_raft_trace(trace)?;
    let render_plan = plan_tla_trace_render(&actions)?;
    Ok(TlaTraceSpec {
        module: render_tla_trace_module(module_name, &render_plan),
        config: render_tla_trace_config(),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TlaRenderedAction {
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

fn plan_tla_trace_render(
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

fn render_tla_trace_module(module_name: &str, actions: &[TlaRenderedAction]) -> String {
    let mut module = String::new();
    module.push_str("---- MODULE ");
    module.push_str(module_name);
    module.push_str(" ----\n");
    module.push_str("EXTENDS Raft\n\n");
    module.push_str("CONSTANTS n1, n2, n3, v1, v2, r1, r2\n\n");
    module.push_str("VARIABLE traceStep\n\n");
    module.push_str(
        "traceVars == << currentTerm, votedFor, role, log, commitIndex, applied, messages,\n",
    );
    module.push_str("               readRequests, readGrants, membership, traceStep >>\n\n");
    module.push_str("TraceInit == Init /\\ traceStep = 0\n\n");

    for (index, action) in actions.iter().copied().enumerate() {
        module.push_str("TraceAction");
        module.push_str(&index.to_string());
        module.push_str(" ==\n");
        module.push_str("  /\\ traceStep = ");
        module.push_str(&index.to_string());
        module.push('\n');
        module.push_str(&render_tla_action_formula(action));
        module.push_str("  /\\ traceStep' = ");
        module.push_str(&(index + 1).to_string());
        module.push_str("\n\n");
    }

    module.push_str("TraceNext ==\n");
    if actions.is_empty() {
        module.push_str("  /\\ traceStep = 0\n");
        module.push_str("  /\\ UNCHANGED traceVars\n\n");
    } else {
        for index in 0..actions.len() {
            module.push_str("  \\/ TraceAction");
            module.push_str(&index.to_string());
            module.push('\n');
        }
        module.push_str("  \\/ /\\ traceStep = ");
        module.push_str(&actions.len().to_string());
        module.push('\n');
        module.push_str("     /\\ UNCHANGED traceVars\n\n");
    }
    module.push_str("TraceSpec == TraceInit /\\ [][TraceNext]_traceVars\n\n");
    module.push_str("TraceComplete == traceStep = ");
    module.push_str(&actions.len().to_string());
    module.push_str("\n\n====\n");
    module
}

fn render_tla_action_formula(action: TlaRenderedAction) -> String {
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

fn render_tla_trace_config() -> String {
    let mut config = String::new();
    config.push_str("SPECIFICATION TraceSpec\n\n");
    config.push_str("CONSTANTS\n");
    config.push_str("  n1 = n1\n");
    config.push_str("  n2 = n2\n");
    config.push_str("  n3 = n3\n");
    config.push_str("  v1 = v1\n");
    config.push_str("  v2 = v2\n");
    config.push_str("  r1 = r1\n");
    config.push_str("  r2 = r2\n");
    config.push_str("  Nodes = {n1, n2, n3}\n");
    config.push_str("  Values = {v1, v2}\n");
    config.push_str("  MaxTerm = 3\n");
    config.push_str("  MaxLogLen = 3\n");
    config.push_str("  ReadRequests = {r1, r2}\n\n");
    config.push_str("INVARIANTS\n");
    config.push_str("  TypeOK\n");
    config.push_str("  ElectionSafety\n");
    config.push_str("  LogMatching\n");
    config.push_str("  LeaderCompleteness\n");
    config.push_str("  CommittedPrefixStability\n");
    config.push_str("  StateMachineSafety\n");
    config.push_str("  StaleLeaderFencing\n");
    config.push_str("  CommittedEntriesHaveQuorum\n");
    config.push_str("  ReadBarrierLinearizability\n");
    config
}

fn tla_node(node_id: NodeId) -> String {
    format!("n{}", node_id.0)
}

fn project_raft_action_to_tla(action: &Action) -> TlaProjection {
    match action {
        Action::Tick(node_id) => TlaProjection::Action(TlaAction::Timeout { node_id: *node_id }),
        Action::Propose { to, .. } => {
            TlaProjection::Action(TlaAction::ClientAppend { node_id: *to })
        }
        Action::ReadIndex { to, .. } => {
            TlaProjection::Action(TlaAction::RegisterRead { node_id: *to })
        }
        Action::Restart(node_id) => TlaProjection::Action(TlaAction::Restart { node_id: *node_id }),
        Action::Deliver {
            from,
            to,
            message: MessageKind::RequestVote,
        } => TlaProjection::Action(TlaAction::DeliverRequestVote {
            from: *from,
            to: *to,
        }),
        Action::Deliver {
            from,
            to,
            message: MessageKind::AppendEntries,
        } => TlaProjection::Action(TlaAction::DeliverAppend {
            from: *from,
            to: *to,
        }),
        Action::Deliver {
            message: MessageKind::RequestVoteResponse,
            ..
        } => TlaProjection::Gap(TlaAbstractionGap::RequestVoteResponse),
        Action::Deliver {
            message: MessageKind::AppendEntriesResponse,
            ..
        } => TlaProjection::Gap(TlaAbstractionGap::AppendEntriesResponse),
        Action::Deliver {
            message:
                MessageKind::InstallSnapshot
                | MessageKind::InstallSnapshotChunk
                | MessageKind::InstallSnapshotResponse,
            ..
        } => TlaProjection::Gap(TlaAbstractionGap::SnapshotTransfer),
        // The pre-vote extension (thesis 9.6) is not part of the abstract
        // TLA+ election model, which only knows term-incrementing elections.
        Action::Deliver {
            message: MessageKind::PreVote | MessageKind::PreVoteResponse | MessageKind::TimeoutNow,
            ..
        } => TlaProjection::Gap(TlaAbstractionGap::PreVote),
        Action::AddLearner { .. }
        | Action::RemoveLearner { .. }
        | Action::PromoteLearner { .. }
        | Action::RemoveVoter { .. }
        | Action::EnterJoint { .. }
        | Action::LeaveJoint { .. } => TlaProjection::Gap(TlaAbstractionGap::MembershipChange),
    }
}
