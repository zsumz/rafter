//! Load-bearing node-state mutations and the modules that own them.

#[derive(Clone, Copy)]
pub(super) struct MutationRule {
    pub(super) token: &'static str,
    pub(super) owners: &'static [&'static str],
}

impl MutationRule {
    const fn new(token: &'static str, owners: &'static [&'static str]) -> Self {
        Self { token, owners }
    }
}

pub(super) const MUTATION_RULES: &[MutationRule] = &[
    MutationRule::new(
        "self.persistent.current_term=",
        &["node/election.rs", "node/lifecycle.rs"],
    ),
    MutationRule::new(
        "self.persistent.voted_for=",
        &["node/election.rs", "node/lifecycle.rs"],
    ),
    MutationRule::new(
        "self.volatile.role=",
        &["node/election.rs", "node/lifecycle.rs"],
    ),
    MutationRule::new(
        "volatile.commit_index=",
        &[
            "node/construction.rs",
            "node/commit/apply.rs",
            "node/log.rs",
            "node/replication/receive.rs",
        ],
    ),
    MutationRule::new(
        "volatile.applied_index=",
        &[
            "node/construction.rs",
            "node/commit/apply.rs",
            "node/log.rs",
        ],
    ),
    MutationRule::new("self.leader=LeaderState::default()", &["node/lifecycle.rs"]),
    MutationRule::new("self.derived=", &["node/log.rs"]),
    MutationRule::new("self.derived.configuration.clear(", &["node/log.rs"]),
    MutationRule::new("self.derived.configuration.truncate(", &["node/log.rs"]),
    MutationRule::new(
        "self.derived.configuration.record_append(",
        &["node/log.rs"],
    ),
    MutationRule::new("self.election.advance_timeout(", &["node/election.rs"]),
    MutationRule::new("self.election.begin_pre_vote(", &["node/election.rs"]),
    MutationRule::new("self.election.begin_election(", &["node/election.rs"]),
    MutationRule::new("self.election.record_vote(", &["node/election.rs"]),
    MutationRule::new("self.election.record_pre_vote(", &["node/election.rs"]),
    MutationRule::new("self.election.enter_leadership(", &["node/lifecycle.rs"]),
    MutationRule::new("self.election.reset_for_follower(", &["node/lifecycle.rs"]),
    MutationRule::new(
        "self.election.reset_timeout(",
        &[
            "node/election.rs",
            "node/replication/receive.rs",
            "node/replication/snapshot/receive/mod.rs",
        ],
    ),
];
