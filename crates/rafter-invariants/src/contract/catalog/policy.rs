//! Reviewed evidence classifications and qualifying executable witnesses.

use std::collections::BTreeSet;

use super::CatalogError;
use crate::contract::registry::{PersistenceEvidenceKind, RegistryEvidence, RegistryInvariant};

const CLIENT_INVARIANT_IDS: &[&str] = &["RD-06"];
const PERSISTENCE_INVARIANT_IDS: &[&str] = &[
    "PS-01", "PS-02", "PS-03", "PS-04", "SS-01", "SS-02", "SS-03", "SS-04", "SS-05",
];

const CLIENT_WITNESS: EvidenceWitness = EvidenceWitness {
    invariant_id: "RD-06",
    clauses: ClauseWitness::Many(&["RD-06.a", "RD-06.b"]),
    layer: "maelstrom",
    strength: "e2e",
    path: "scripts/maelstrom-lin-kv",
    symbol: "--workload lin-kv",
    test: None,
    persistence: None,
};

const PERSISTENCE_WITNESSES: &[EvidenceWitness] = &[
    EvidenceWitness::test(
        "PS-01",
        "PS-01.c",
        "crates/rafter-runtime/src/tests/persistence_ordering.rs",
        "committed_configuration_write_failure_suppresses_dependent_membership_output",
        "rafter-runtime",
        "lib",
        "rafter_runtime",
        "tests::persistence_ordering::committed_configuration_write_failure_suppresses_dependent_membership_output",
        PersistenceEvidenceKind::FailureInjection,
    ),
    EvidenceWitness::test(
        "PS-02",
        "PS-02.c",
        "crates/rafter-runtime/src/tests/group_commit/failure.rs",
        "group_commit_failure_preserves_last_successful_state_across_crash_and_reopen",
        "rafter-runtime",
        "lib",
        "rafter_runtime",
        "tests::group_commit::failure::group_commit_failure_preserves_last_successful_state_across_crash_and_reopen",
        PersistenceEvidenceKind::CrashReopen,
    ),
    EvidenceWitness::test(
        "PS-03",
        "PS-03.e",
        "crates/rafter-runtime/src/tests/local_ids/recovery.rs",
        "restart_replays_committed_tracked_entry_without_local_id",
        "rafter-runtime",
        "lib",
        "rafter_runtime",
        "tests::local_ids::recovery::restart_replays_committed_tracked_entry_without_local_id",
        PersistenceEvidenceKind::CrashReopen,
    ),
    EvidenceWitness::test(
        "PS-04",
        "PS-04.a",
        "crates/rafter-maelstrom/src/app/ps04_tests.rs",
        "ps04_app_persist_interrupt_reopens_at_durable_floor_and_replays_suffix_once",
        "rafter-maelstrom",
        "bin",
        "rafter-maelstrom",
        "app::ps04_tests::ps04_app_persist_interrupt_reopens_at_durable_floor_and_replays_suffix_once",
        PersistenceEvidenceKind::CrashReopen,
    ),
    EvidenceWitness::test(
        "SS-01",
        "SS-01.b",
        "crates/rafter-storage/src/raft_snapshot_store_test.rs",
        "file_snapshot_store_reopens_manifest_selected_snapshot",
        "rafter-storage",
        "lib",
        "rafter_storage",
        "raft_snapshot_store::raft_snapshot_store_test::file_snapshot_store_reopens_manifest_selected_snapshot",
        PersistenceEvidenceKind::CrashReopen,
    ),
    EvidenceWitness::test(
        "SS-02",
        "SS-02.a",
        "crates/rafter-runtime/src/tests/crash_window/repair.rs",
        "file_backed_reopen_persists_repaired_compaction_after_snapshot_crash_window",
        "rafter-runtime",
        "lib",
        "rafter_runtime",
        "tests::crash_window::repair::file_backed_reopen_persists_repaired_compaction_after_snapshot_crash_window",
        PersistenceEvidenceKind::CrashReopen,
    ),
    EvidenceWitness::test(
        "SS-03",
        "SS-03.b",
        "crates/rafter-runtime/src/tests/crash_window/repair.rs",
        "file_backed_reopen_persists_repaired_compaction_after_snapshot_crash_window",
        "rafter-runtime",
        "lib",
        "rafter_runtime",
        "tests::crash_window::repair::file_backed_reopen_persists_repaired_compaction_after_snapshot_crash_window",
        PersistenceEvidenceKind::CrashReopen,
    ),
    EvidenceWitness::test(
        "SS-04",
        "SS-04.b",
        "crates/rafter-storage/src/raft_snapshot_store/pending_transfer_test.rs",
        "file_snapshot_store_promotes_staged_transfer_resumed_after_reopen",
        "rafter-storage",
        "lib",
        "rafter_storage",
        "raft_snapshot_store::pending_transfer_test::file_snapshot_store_promotes_staged_transfer_resumed_after_reopen",
        PersistenceEvidenceKind::CrashReopen,
    ),
    EvidenceWitness::test(
        "SS-05",
        "SS-05.b",
        "crates/rafter-runtime/src/tests/snapshot/chunk_transfer/promotion.rs",
        "file_backed_snapshot_install_discards_covered_divergent_suffix_across_reopen",
        "rafter-runtime",
        "lib",
        "rafter_runtime",
        "tests::snapshot::chunk_transfer::promotion::file_backed_snapshot_install_discards_covered_divergent_suffix_across_reopen",
        PersistenceEvidenceKind::CrashReopen,
    ),
];

pub(super) fn validate(
    invariants: &[RegistryInvariant],
    evidence: &[RegistryEvidence],
) -> Result<(), CatalogError> {
    validate_exact_classification(invariants, "client", CLIENT_INVARIANT_IDS, |invariant| {
        invariant.tier.as_str()
    })?;
    validate_exact_classification(
        invariants,
        "persistence",
        PERSISTENCE_INVARIANT_IDS,
        |invariant| invariant.family.as_str(),
    )?;

    require_witness(evidence, &CLIENT_WITNESS, "client-visible")?;
    for witness in PERSISTENCE_WITNESSES {
        require_witness(evidence, witness, "persistence")?;
    }
    for record in evidence
        .iter()
        .filter(|record| record.persistence_evidence.is_some())
    {
        if !PERSISTENCE_WITNESSES
            .iter()
            .any(|witness| witness.matches(record))
        {
            return Err(CatalogError(format!(
                "{} persistence evidence is not a reviewed executable witness",
                record.id
            )));
        }
    }
    Ok(())
}

fn validate_exact_classification(
    invariants: &[RegistryInvariant],
    classification: &str,
    expected_ids: &[&str],
    value: impl Fn(&RegistryInvariant) -> &str,
) -> Result<(), CatalogError> {
    let actual = invariants
        .iter()
        .filter(|invariant| value(invariant) == classification)
        .map(|invariant| invariant.id.as_str())
        .collect::<BTreeSet<_>>();
    let expected = expected_ids.iter().copied().collect::<BTreeSet<_>>();
    if actual != expected {
        return Err(CatalogError(format!(
            "reviewed {classification} invariant IDs changed: expected {expected:?}, found {actual:?}"
        )));
    }
    Ok(())
}

fn require_witness(
    evidence: &[RegistryEvidence],
    witness: &EvidenceWitness,
    policy: &str,
) -> Result<(), CatalogError> {
    if evidence.iter().any(|record| witness.matches(record)) {
        return Ok(());
    }
    Err(CatalogError(format!(
        "{policy} invariant {} must retain its reviewed executable witness",
        witness.invariant_id
    )))
}

#[derive(Clone, Copy)]
struct EvidenceWitness {
    invariant_id: &'static str,
    clauses: ClauseWitness,
    layer: &'static str,
    strength: &'static str,
    path: &'static str,
    symbol: &'static str,
    test: Option<TestWitness>,
    persistence: Option<PersistenceEvidenceKind>,
}

impl EvidenceWitness {
    #[allow(clippy::too_many_arguments)]
    const fn test(
        invariant_id: &'static str,
        clause: &'static str,
        path: &'static str,
        symbol: &'static str,
        package: &'static str,
        target_kind: &'static str,
        target: &'static str,
        test_name: &'static str,
        persistence: PersistenceEvidenceKind,
    ) -> Self {
        Self {
            invariant_id,
            clauses: ClauseWitness::One(clause),
            layer: "tests",
            strength: "direct",
            path,
            symbol,
            test: Some(TestWitness {
                package,
                target_kind,
                target,
                test_name,
            }),
            persistence: Some(persistence),
        }
    }

    fn matches(self, record: &RegistryEvidence) -> bool {
        record.id == self.invariant_id
            && self.clauses.matches(&record.clauses)
            && record.layer == self.layer
            && record.strength == self.strength
            && record.path == self.path
            && record.symbol == self.symbol
            && record.persistence_evidence == self.persistence
            && self.test_matches(record)
            && record.simulator.is_none()
            && record.atomic_group.is_none()
            && record.negative_fixture.is_none()
            && record.negative_fixture_path.is_none()
            && record.negative_fixture_detector.is_none()
            && record.negative_fixture_exemption.is_none()
    }

    fn test_matches(self, record: &RegistryEvidence) -> bool {
        match (self.test, record.test.as_ref()) {
            (None, None) => true,
            (Some(expected), Some(actual)) => {
                actual.package == expected.package
                    && actual.target_kind == expected.target_kind
                    && actual.target == expected.target
                    && actual.test_name == expected.test_name
            }
            _ => false,
        }
    }
}

#[derive(Clone, Copy)]
enum ClauseWitness {
    One(&'static str),
    Many(&'static [&'static str]),
}

impl ClauseWitness {
    fn matches(self, actual: &[String]) -> bool {
        match self {
            Self::One(expected) => actual.len() == 1 && actual[0] == expected,
            Self::Many(expected) => actual
                .iter()
                .map(String::as_str)
                .eq(expected.iter().copied()),
        }
    }
}

#[derive(Clone, Copy)]
struct TestWitness {
    package: &'static str,
    target_kind: &'static str,
    target: &'static str,
    test_name: &'static str,
}
