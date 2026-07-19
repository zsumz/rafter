//! Reviewed ownership and debt baselines for the invariant-tooling migration.

pub(super) const PRODUCTION_TARGET_LINES: usize = 300;
pub(super) const TEST_TARGET_LINES: usize = 400;
pub(super) const MAX_PRODUCTION_FILES_OVER_TARGET: usize = 58;
pub(super) const MAX_TEST_FILES_OVER_TARGET: usize = 20;
pub(super) const MAX_FILES_WITHOUT_MODULE_CONTRACTS: usize = 136;
pub(super) const MAX_LEGACY_VERIFIER_PRODUCER_REFERENCES: usize = 154;

pub(super) const INVARIANT_SOURCE_ROOTS: &[&str] = &[
    "crates/rafter-invariant-test-macros/src",
    "crates/rafter-invariant-test/src",
    "crates/rafter-invariants/src",
];

#[derive(Clone, Copy)]
pub(super) struct InvariantDomain {
    pub name: &'static str,
    pub may_depend_on: &'static [&'static str],
}

/// Ordered from foundational vocabulary to the outer command adapter.
pub(super) const INVARIANT_DOMAINS: &[InvariantDomain] = &[
    InvariantDomain {
        name: "contract",
        may_depend_on: &[],
    },
    InvariantDomain {
        name: "evidence",
        may_depend_on: &[],
    },
    InvariantDomain {
        name: "provenance",
        may_depend_on: &[],
    },
    InvariantDomain {
        name: "execution",
        may_depend_on: &[],
    },
    InvariantDomain {
        name: "plan",
        may_depend_on: &["contract", "evidence", "provenance"],
    },
    InvariantDomain {
        name: "producer",
        may_depend_on: &["contract", "evidence", "provenance", "execution", "plan"],
    },
    InvariantDomain {
        name: "verification",
        may_depend_on: &["contract", "evidence", "provenance"],
    },
    InvariantDomain {
        name: "verdict",
        may_depend_on: &["contract", "evidence", "verification"],
    },
    InvariantDomain {
        name: "gate",
        may_depend_on: &[
            "contract",
            "evidence",
            "plan",
            "producer",
            "verification",
            "verdict",
        ],
    },
    InvariantDomain {
        name: "cli",
        may_depend_on: &["contract", "gate"],
    },
];
