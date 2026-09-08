//! Reading links for existing invariant failures; never a second decision oracle.

use rafter_sim::model_check::reviewed_invariant_id;

pub(super) struct RuleGuide {
    pub explanation: &'static str,
    pub source: &'static str,
    pub transition: &'static str,
    pub guide_anchor: &'static str,
}

pub(super) fn rule_guide(invariant: &str) -> Option<RuleGuide> {
    let (explanation, source, transition, guide_anchor) = match reviewed_invariant_id(invariant)? {
        "EL-01" | "EL-03" => (
            "Learn a higher term before deciding whether the candidate's log earns a vote.",
            "crates/rafter/src/node/election.rs", "handle_request_vote", "vote-authority",
        ),
        "EL-07" => (
            "Fence term and role before an acknowledgement can contribute authority or replication evidence.",
            "crates/rafter/src/node/replication/response.rs", "handle_append_entries_response", "acknowledgement-authority",
        ),
        "CM-03" => (
            "A quorum-replicated candidate advances commitment only when its entry is from the current term.",
            "crates/rafter/src/node/commit/advance.rs", "advance_commit_index_into", "current-term-commitment",
        ),
        "CM-02" => (
            "Joint commitment requires a majority of each constituent voter set; learners add no votes.",
            "crates/rafter/src/node/commit/advance.rs", "quorum_replicated_index", "joint-majorities",
        ),
        _ => return None,
    };
    Some(RuleGuide {
        explanation,
        source,
        transition,
        guide_anchor,
    })
}
