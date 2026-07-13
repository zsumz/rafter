use super::super::Action;
use super::errors::TlaTraceRenderError;
use super::projection::require_tla_projectable_raft_trace;
use super::types::TlaTraceSpec;

mod formula;
mod plan;

use formula::render_tla_action_formula;
use plan::{plan_tla_trace_render, TlaRenderedAction};

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
/// Returns [`TlaTraceRenderError`] if any trace action belongs to a named
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
    module.push_str("               readRequests, readGrants, membership, appliedConfigIndex,\n");
    module.push_str("               effectiveMembership, effectiveConfigIndex,\n");
    module.push_str("               electedLeaders,\n");
    module.push_str("               higherTermEvidenceSeen, higherTermStepDownFailed,\n");
    module.push_str("               staleAuthorityAccepted, traceStep >>\n\n");
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
