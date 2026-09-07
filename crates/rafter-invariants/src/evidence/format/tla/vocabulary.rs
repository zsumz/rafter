//! Reviewed TLA+ vocabulary shared by the producer and the verifier.
//!
//! These names and thresholds are the serialized contract both sides rederive
//! independently: the registered predicates, the transitions a trace must
//! execute, its minimum shape, and the metrics an obligation contributes.

pub(crate) const REGISTERED_PREDICATES: [&str; 8] = [
    "ElectionSafety",
    "LogMatching",
    "LeaderCompleteness",
    "CommittedPrefixStability",
    "StateMachineSafety",
    "StaleLeaderFencing",
    "CommittedEntriesHaveQuorum",
    "ReadBarrierLinearizability",
];

pub(crate) const REQUIRED_MODEL_TRANSITIONS: [&str; 18] = [
    "Timeout",
    "SendRequestVote",
    "DeliverRequestVote",
    "BecomeLeader",
    "ClientAppend",
    "SendAppend",
    "DeliverAppend",
    "Commit",
    "Apply",
    "ApplicationStateLoss",
    "Restart",
    "CreateSnapshot",
    "TransferSnapshot",
    "InstallSnapshot",
    "EnterJoint",
    "LeaveJoint",
    "RegisterRead",
    "GrantRead",
];

// One lower than they were: folding snapshot creation and compaction into one
// atomic action removed `CompactSnapshot` from the model, and with it the trace
// step that executed it. The trace is one step shorter, not one step weaker --
// it still executes every transition in REQUIRED_MODEL_TRANSITIONS.
pub(crate) const MEMBERSHIP_TRACE_MIN_DISTINCT_STATES: u64 = 45;
pub(crate) const MEMBERSHIP_TRACE_MIN_DEPTH: u64 = 45;
pub(crate) const DEFAULT_FIXTURE_MODE: &str = "Default";

/// Observation metrics every executed proof obligation contributes to the
/// layer receipt, in the order a reader wants them: what it explored, how deep
/// it went, and whether it actually drained its queue.
pub(crate) const OBLIGATION_METRICS: [&str; 4] = [
    "generated_states",
    "distinct_states",
    "search_depth",
    "frontier_exhausted",
];
