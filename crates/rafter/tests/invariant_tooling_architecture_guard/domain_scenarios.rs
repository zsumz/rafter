//! Scenarios: domain ordering, facades, and migrated ownership remain explicit.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use super::{
    architecture_support::{
        assert_domain_source_imports_follow_manifest, declared_module_graph,
        declares_implementation, display_path, domain, invariant_rust_files, read, rust_files,
        starts_with_module_contract, workspace_root,
    },
    invariant_tooling::{
        ENFORCED_DOMAIN_SOURCES, INVARIANT_DOMAINS, REVIEWED_DOMAIN_IMPORT_EXCEPTIONS,
    },
    readability_support::{FACADE_PATHS, TEST_FACADE_PATHS},
};

#[test]
fn target_domain_dependencies_are_known_and_one_way() {
    let expected = [
        "contract",
        "evidence",
        "provenance",
        "execution",
        "plan",
        "producer",
        "verification",
        "verdict",
        "gate",
        "cli",
    ];
    assert_eq!(
        INVARIANT_DOMAINS
            .iter()
            .map(|domain| domain.name)
            .collect::<Vec<_>>(),
        expected
    );

    let positions = INVARIANT_DOMAINS
        .iter()
        .enumerate()
        .map(|(index, domain)| (domain.name, index))
        .collect::<BTreeMap<_, _>>();
    for (index, domain) in INVARIANT_DOMAINS.iter().enumerate() {
        for dependency in domain.may_depend_on {
            let dependency_index = positions
                .get(dependency)
                .unwrap_or_else(|| panic!("{} names unknown dependency {dependency}", domain.name));
            assert!(
                *dependency_index < index,
                "{} may only depend on an earlier domain; found {dependency}",
                domain.name
            );
        }
    }
    let producer = domain("producer");
    let verification = domain("verification");
    assert!(!producer.may_depend_on.contains(&"verification"));
    assert!(!verification.may_depend_on.contains(&"producer"));
}

#[test]
fn mature_invariant_facades_remain_declarative() {
    let root = workspace_root();
    let facades = invariant_facades();
    assert_eq!(
        facades,
        [
            "crates/rafter-invariant-test/src/lib.rs",
            "crates/rafter-invariant-test/src/detector/mod.rs",
            "crates/rafter-invariant-test/src/oracle/mod.rs",
            "crates/rafter-invariants/src/lib.rs",
            "crates/rafter-invariants/src/artifact_verify/test_logs.rs",
            "crates/rafter-invariants/src/contract/mod.rs",
            "crates/rafter-invariants/src/contract/catalog/mod.rs",
            "crates/rafter-invariants/src/contract/profile/liveness/mod.rs",
            "crates/rafter-invariants/src/contract/profile/mod.rs",
            "crates/rafter-invariants/src/contract/profile/runner_contract/mod.rs",
            "crates/rafter-invariants/src/contract/registry/mod.rs",
            "crates/rafter-invariants/src/contract/registry/parse/mod.rs",
            "crates/rafter-invariants/src/contract/schema/mod.rs",
            "crates/rafter-invariants/src/evidence/format/mod.rs",
            "crates/rafter-invariants/src/evidence/format/process/mod.rs",
            "crates/rafter-invariants/src/evidence/liveness/mod.rs",
            "crates/rafter-invariants/src/evidence/mod.rs",
            "crates/rafter-invariants/src/evidence/receipt/mod.rs",
            "crates/rafter-invariants/src/execution/filesystem/mod.rs",
            "crates/rafter-invariants/src/execution/mod.rs",
            "crates/rafter-invariants/src/execution/process/mod.rs",
            "crates/rafter-invariants/src/producer/process/mod.rs",
            "crates/rafter-invariants/src/producer/process/budget/mod.rs",
            "crates/rafter-invariants/src/producer/simulator/liveness/mod.rs",
            "crates/rafter-invariants/src/provenance/invocation/mod.rs",
            "crates/rafter-invariants/src/provenance/mod.rs",
            "crates/rafter-invariants/src/verification/mod.rs",
            "crates/rafter-invariants/src/verification/simulator/mod.rs",
            "crates/rafter-invariants/src/verification/simulator/liveness/mod.rs",
            "crates/rafter-invariants/src/verdict/mod.rs",
            "crates/rafter-invariants/src/contract/registry/parse/tests/mod.rs",
            "crates/rafter-invariants/src/producer/process/tests/mod.rs",
            "crates/rafter-invariants/src/execution/process/tests/mod.rs",
            "crates/rafter-invariants/src/verification/simulator/liveness/tests/mod.rs",
        ]
    );

    for relative in facades {
        let source = read(&root.join(relative));
        assert!(
            starts_with_module_contract(&source),
            "{relative} needs a `//!` contract"
        );
        for (line_index, line) in source.lines().enumerate() {
            assert!(
                !declares_implementation(line.trim_start()),
                "{relative}:{} contains implementation",
                line_index + 1
            );
        }
    }
}

#[test]
fn modeled_invariant_domains_require_module_contracts_without_legacy_allowance() {
    let root = workspace_root();
    let mut modeled = ENFORCED_DOMAIN_SOURCES
        .iter()
        .map(|source| source.path)
        .collect::<BTreeSet<_>>();
    modeled.insert("crates/rafter-invariants/src/producer/simulator/liveness");
    for relative in modeled {
        for path in rust_files(&root.join(relative)) {
            assert!(
                starts_with_module_contract(&read(&path)),
                "{} needs a `//!` module contract",
                display_path(&root, &path)
            );
        }
    }
}

#[test]
fn migrated_domain_sources_follow_the_reviewed_dependency_graph() {
    let root = workspace_root();
    let modules = declared_module_graph(&root);
    assert_eq!(
        ENFORCED_DOMAIN_SOURCES
            .iter()
            .map(|source| (source.domain, source.path))
            .collect::<Vec<_>>(),
        [
            ("contract", "crates/rafter-invariants/src/contract"),
            ("evidence", "crates/rafter-invariants/src/evidence"),
            ("execution", "crates/rafter-invariants/src/execution"),
            ("provenance", "crates/rafter-invariants/src/provenance"),
            ("producer", "crates/rafter-invariants/src/producer/process"),
            ("verification", "crates/rafter-invariants/src/verification"),
            (
                "verification",
                "crates/rafter-invariants/src/artifact_verify/test_logs",
            ),
            (
                "verification",
                "crates/rafter-invariants/src/artifact_verify/test_logs.rs",
            ),
            ("verdict", "crates/rafter-invariants/src/verdict"),
        ]
    );
    for source in ENFORCED_DOMAIN_SOURCES {
        assert_domain_source_imports_follow_manifest(&root, &modules, source.domain, source.path);
    }
}

#[test]
fn reviewed_domain_import_exceptions_are_narrow_current_and_tracked() {
    let root = workspace_root();
    let mut identities = BTreeSet::new();
    for exception in REVIEWED_DOMAIN_IMPORT_EXCEPTIONS {
        assert!(domain(exception.owner_domain).name == exception.owner_domain);
        assert!(
            root.join(exception.source).is_file(),
            "{} points to missing source {}",
            exception.tracking_label,
            exception.source
        );
        assert!(
            !exception.reason.trim().is_empty(),
            "{} needs a reason",
            exception.tracking_label
        );
        assert!(
            exception.tracking_label.starts_with("INV-ARCH-"),
            "invalid architecture tracking label {}",
            exception.tracking_label
        );
        assert_eq!(exception.import.first(), Some(&"crate"));
        assert!(exception.import.len() >= 3, "exception import is too broad");
        assert!(
            identities.insert((exception.owner_domain, exception.source, exception.import,)),
            "duplicate reviewed exception {}",
            exception.tracking_label
        );
        let owners = ENFORCED_DOMAIN_SOURCES
            .iter()
            .filter(|source| {
                source.domain == exception.owner_domain
                    && modeled_source_contains(source.path, exception.source)
            })
            .count();
        assert_eq!(
            owners, 1,
            "{} must belong to exactly one modeled source root",
            exception.tracking_label
        );
    }
}

#[test]
fn detector_transcript_acceptance_has_independent_policies() {
    let root = workspace_root();
    let neutral_path = "crates/rafter-invariants/src/evidence/detector_proof.rs";
    let producer_path = "crates/rafter-invariants/src/producer/test_exec/detector_policy.rs";
    let verifier_path = "crates/rafter-invariants/src/artifact_verify/test_logs/detector.rs";
    let neutral = read(&root.join(neutral_path));

    assert!(starts_with_module_contract(&neutral));
    assert!(
        neutral.contains("pub(crate) fn decode_transcript"),
        "{neutral_path} must expose neutral detector transcript decoding"
    );
    assert!(
        !neutral.contains("fn verify_transcript("),
        "{neutral_path} cannot own shared detector acceptance"
    );

    let producer_root = read(&root.join("crates/rafter-invariants/src/producer/test_exec.rs"));
    assert!(
        producer_root.contains("mod detector_policy;"),
        "producer test execution must declare its detector policy module"
    );
    for (owner, relative) in [("producer", producer_path), ("verifier", verifier_path)] {
        let source = read(&root.join(relative));
        assert!(
            starts_with_module_contract(&source),
            "{relative} needs an ownership contract"
        );
        assert!(
            source.contains("decode_transcript"),
            "{owner} policy must independently consume the neutral transcript"
        );
        assert!(
            source.lines().any(|line| line.contains("fn ")),
            "{owner} policy module contains no acceptance policy"
        );
    }

    for path in invariant_rust_files(&root) {
        let source = read(&path);
        assert!(
            !source.contains("fn verify_transcript(") && !source.contains("::verify_transcript("),
            "{} reintroduced the shared detector transcript reducer",
            display_path(&root, &path)
        );
    }
}

#[test]
fn retired_producer_filesystem_ownership_cannot_return() {
    let root = workspace_root();
    for relative in [
        "crates/rafter-invariants/src/producer/filesystem.rs",
        "crates/rafter-invariants/src/producer/filesystem",
        "crates/rafter-invariants/src/producer/filesystem_tests.rs",
    ] {
        assert!(
            !root.join(relative).exists(),
            "retired producer filesystem path returned: {relative}"
        );
    }

    for path in invariant_rust_files(&root) {
        let source = read(&path);
        assert!(
            !source.contains("crate::producer::filesystem")
                && !source.contains("producer::filesystem::"),
            "{} imports retired producer filesystem ownership",
            display_path(&root, &path)
        );
    }
}

#[test]
fn retired_root_producer_image_ownership_cannot_return() {
    let root = workspace_root();
    for relative in [
        "crates/rafter-invariants/src/producer_image.rs",
        "crates/rafter-invariants/src/producer_image",
    ] {
        assert!(
            !root.join(relative).exists(),
            "retired root producer-image path returned: {relative}"
        );
    }

    for path in invariant_rust_files(&root) {
        let source = read(&path);
        assert!(
            !source.contains("crate::producer_image") && !source.contains("producer_image::"),
            "{} imports retired root producer-image ownership",
            display_path(&root, &path)
        );
    }
}

#[test]
fn retired_internal_catalog_alias_cannot_return() {
    let root = workspace_root();
    for path in invariant_rust_files(&root) {
        let source = read(&path);
        assert!(
            !source.contains("crate::catalog"),
            "{} imports the retired internal catalog alias",
            display_path(&root, &path)
        );
    }
    assert!(
        !read(&root.join("crates/rafter-invariants/src/lib.rs"))
            .contains("pub(crate) use contract::catalog"),
        "the retired crate-root catalog alias returned"
    );
}

#[test]
fn liveness_wire_binding_cannot_absorb_raw_event_acceptance() {
    let root = workspace_root();
    let evidence = read(&root.join("crates/rafter-invariants/src/evidence/liveness/binding.rs"));
    for raw_acceptance in [
        "SimulatorIdentity",
        "expected_execution_contract",
        "LivenessReportError",
        "BTreeMap<String, Vec<Value>>",
        "validate_liveness_report",
    ] {
        assert!(
            !evidence.contains(raw_acceptance),
            "neutral evidence binding absorbed `{raw_acceptance}`"
        );
    }

    for relative in [
        "crates/rafter-invariants/src/producer/simulator/liveness/raw.rs",
        "crates/rafter-invariants/src/verification/simulator/liveness/raw.rs",
    ] {
        assert!(
            root.join(relative).is_file(),
            "missing independent {relative}"
        );
    }
}

#[test]
fn retired_flat_contract_files_cannot_return() {
    let root = workspace_root();
    for relative in [
        "crates/rafter-invariants/src/catalog.rs",
        "crates/rafter-invariants/src/registry.rs",
        "crates/rafter-invariants/src/registry_document.rs",
        "crates/rafter-invariants/src/registry_parse.rs",
        "crates/rafter-invariants/src/schema.rs",
        "crates/rafter-invariants/src/types.rs",
    ] {
        assert!(
            !root.join(relative).exists(),
            "retired flat contract file returned: {relative}"
        );
    }
}

#[test]
fn detector_test_macro_trust_is_bound_to_exact_domain_sources() {
    let root = workspace_root();
    let source = read(&root.join("crates/rafter-invariants/src/rust_target.rs"));

    for expected in [
        "crates/rafter-invariant-test/src/oracle/macros.rs",
        "crates/rafter-invariant-test/src/oracle/call.rs",
        "crates/rafter-invariant-test/src/detector/session.rs",
    ] {
        assert!(
            source.contains(expected),
            "missing exact trust path {expected}"
        );
    }
    assert!(
        !source.contains("Some(Path::new(\"crates/rafter-invariant-test/src/lib.rs\"))"),
        "the detector facade must not retain the old broad item-macro exception"
    );
}

#[test]
fn detector_proc_macro_root_is_a_thin_entrypoint() {
    let root = workspace_root();
    let relative = "crates/rafter-invariant-test-macros/src/lib.rs";
    let source = read(&root.join(relative));
    assert!(starts_with_module_contract(&source));
    assert!(
        source.lines().count() <= 20,
        "{relative} stopped being thin"
    );
    assert_eq!(source.matches("pub fn detector_test").count(), 1);
    for implementation_detail in ["parse_quote", "quote!", "ItemFn", "ReturnType"] {
        assert!(
            !source.contains(implementation_detail),
            "{relative} absorbed parser implementation `{implementation_detail}`"
        );
    }
}

fn invariant_facades() -> Vec<&'static str> {
    FACADE_PATHS
        .iter()
        .chain(TEST_FACADE_PATHS)
        .copied()
        .filter(|path| path.starts_with("crates/rafter-invariant"))
        .collect()
}

fn modeled_source_contains(source: &str, candidate: &str) -> bool {
    source == candidate
        || (!Path::new(source)
            .extension()
            .is_some_and(|extension| extension.eq_ignore_ascii_case("rs"))
            && candidate
                .strip_prefix(source)
                .is_some_and(|suffix| suffix.starts_with('/')))
}
