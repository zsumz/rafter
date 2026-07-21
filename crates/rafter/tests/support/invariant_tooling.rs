//! Reviewed ownership and debt baselines for the invariant-tooling migration.

pub(super) const PRODUCTION_TARGET_LINES: usize = 300;
pub(super) const TEST_TARGET_LINES: usize = 400;
pub(super) const MAX_PRODUCTION_FILES_OVER_TARGET: usize = 35;
pub(super) const MAX_TEST_FILES_OVER_TARGET: usize = 15;
pub(super) const MAX_FILES_WITHOUT_MODULE_CONTRACTS: usize = 47;
pub(super) const MAX_LEGACY_VERIFIER_PRODUCER_REFERENCES: usize = 41;
pub(super) const MAX_LEGACY_VERIFIER_PRODUCER_IMAGE_REFERENCES: usize = 0;
pub(super) const MAX_LEGACY_VERIFIER_RUST_TARGET_REFERENCES: usize = 0;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct EnforcedDomainSource {
    pub domain: &'static str,
    pub path: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct ReviewedDomainImportException {
    pub owner_domain: &'static str,
    pub source: &'static str,
    pub import: &'static [&'static str],
    pub reason: &'static str,
    pub tracking_label: &'static str,
}

/// Ordered from foundational vocabulary to the outer command adapter.
pub(super) const INVARIANT_DOMAINS: &[InvariantDomain] = &[
    InvariantDomain {
        name: "contract",
        may_depend_on: &[],
    },
    InvariantDomain {
        name: "evidence",
        may_depend_on: &["contract"],
    },
    InvariantDomain {
        name: "execution",
        may_depend_on: &[],
    },
    InvariantDomain {
        name: "provenance",
        may_depend_on: &["execution"],
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
        may_depend_on: &["contract", "evidence", "execution", "provenance"],
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

/// Source trees that have completed ownership migration and are fail-closed.
pub(super) const ENFORCED_DOMAIN_SOURCES: &[EnforcedDomainSource] = &[
    EnforcedDomainSource {
        domain: "contract",
        path: "crates/rafter-invariants/src/contract",
    },
    EnforcedDomainSource {
        domain: "evidence",
        path: "crates/rafter-invariants/src/evidence",
    },
    EnforcedDomainSource {
        domain: "execution",
        path: "crates/rafter-invariants/src/execution",
    },
    EnforcedDomainSource {
        domain: "provenance",
        path: "crates/rafter-invariants/src/provenance",
    },
    EnforcedDomainSource {
        domain: "producer",
        path: "crates/rafter-invariants/src/producer/process",
    },
    EnforcedDomainSource {
        domain: "producer",
        path: "crates/rafter-invariants/src/producer/source.rs",
    },
    EnforcedDomainSource {
        domain: "producer",
        path: "crates/rafter-invariants/src/producer/test_compile.rs",
    },
    EnforcedDomainSource {
        domain: "producer",
        path: "crates/rafter-invariants/src/producer/test_compile",
    },
    EnforcedDomainSource {
        domain: "producer",
        path: "crates/rafter-invariants/src/producer/test_exec.rs",
    },
    EnforcedDomainSource {
        domain: "producer",
        path: "crates/rafter-invariants/src/producer/test_exec",
    },
    EnforcedDomainSource {
        domain: "verification",
        path: "crates/rafter-invariants/src/verification",
    },
    EnforcedDomainSource {
        domain: "verification",
        path: "crates/rafter-invariants/src/artifact_verify/test_logs",
    },
    EnforcedDomainSource {
        domain: "verification",
        path: "crates/rafter-invariants/src/artifact_verify/test_logs.rs",
    },
    EnforcedDomainSource {
        domain: "verdict",
        path: "crates/rafter-invariants/src/verdict",
    },
    EnforcedDomainSource {
        domain: "gate",
        path: "crates/rafter-invariants/src/gate",
    },
    EnforcedDomainSource {
        domain: "cli",
        path: "crates/rafter-invariants/src/cli",
    },
];

/// Exact compatibility edges retained while physical modules move to their owning domains.
pub(super) const REVIEWED_DOMAIN_IMPORT_EXCEPTIONS: &[ReviewedDomainImportException] = &[
    ReviewedDomainImportException {
        owner_domain: "verification",
        source: "crates/rafter-invariants/src/verification/artifact.rs",
        import: &["crate", "artifact_verify", "verify"],
        reason: "the verification facade still delegates to the legacy artifact-verifier mount",
        tracking_label: "INV-ARCH-ARTIFACT-VERIFIER-MIGRATION",
    },
    ReviewedDomainImportException {
        owner_domain: "verification",
        source: "crates/rafter-invariants/src/verification/intake/verify.rs",
        import: &["crate", "receipt", "collect_results"],
        reason: "typed intake delegates to the legacy root receipt validator until runner-family validators move together",
        tracking_label: "INV-ARCH-RECEIPT-VALIDATION-MIGRATION",
    },
];
