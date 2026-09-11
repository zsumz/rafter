//! Mutation rules outside rc.8 coverage: scalar index writes, exact leader
//! replacement, and nested configuration mutators. Other owners live in zrail.toml.

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
        "volatile.commit_index=",
        &[
            "node/construction.rs",
            "node/commit/advance.rs",
            "node/log.rs",
            "node/replication/receive.rs",
        ],
    ),
    MutationRule::new(
        "volatile.dispatched_index=",
        &["node/construction.rs", "node/commit/emit.rs", "node/log.rs"],
    ),
    MutationRule::new("self.leader=LeaderState::default()", &["node/lifecycle.rs"]),
    MutationRule::new("self.derived.configuration.clear(", &["node/log.rs"]),
    MutationRule::new("self.derived.configuration.truncate(", &["node/log.rs"]),
    MutationRule::new(
        "self.derived.configuration.record_append(",
        &["node/log.rs"],
    ),
];
