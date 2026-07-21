//! Scenarios: domain ordering, facades, and migrated ownership remain explicit.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use super::{
    architecture_support::{
        assert_domain_source_imports_follow_manifest, declared_module_graph,
        declares_implementation, display_path, domain, invariant_rust_files, is_test_module, read,
        rust_files, starts_with_module_contract, workspace_root,
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
        "execution",
        "provenance",
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
            "crates/rafter-invariants/src/artifact_verify/simulator.rs",
            "crates/rafter-invariants/src/artifact_verify/simulator_schedule.rs",
            "crates/rafter-invariants/src/artifact_verify/simulator_schedule/events.rs",
            "crates/rafter-invariants/src/artifact_verify/test_logs.rs",
            "crates/rafter-invariants/src/artifact_verify_tla.rs",
            "crates/rafter-invariants/src/cli/mod.rs",
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
            "crates/rafter-invariants/src/gate/mod.rs",
            "crates/rafter-invariants/src/producer/process/mod.rs",
            "crates/rafter-invariants/src/producer/process/budget/mod.rs",
            "crates/rafter-invariants/src/producer/simulator.rs",
            "crates/rafter-invariants/src/producer/simulator/liveness/mod.rs",
            "crates/rafter-invariants/src/producer/test_compile.rs",
            "crates/rafter-invariants/src/producer/test_exec.rs",
            "crates/rafter-invariants/src/provenance/invocation/mod.rs",
            "crates/rafter-invariants/src/provenance/source/mod.rs",
            "crates/rafter-invariants/src/provenance/mod.rs",
            "crates/rafter-invariants/src/receipt_tla.rs",
            "crates/rafter-invariants/src/verification/mod.rs",
            "crates/rafter-invariants/src/verification/intake/mod.rs",
            "crates/rafter-invariants/src/verification/detector_replay/artifact/mod.rs",
            "crates/rafter-invariants/src/verification/simulator/mod.rs",
            "crates/rafter-invariants/src/verification/simulator/event/mod.rs",
            "crates/rafter-invariants/src/verification/simulator/liveness/mod.rs",
            "crates/rafter-invariants/src/verification/simulator/observation/mod.rs",
            "crates/rafter-invariants/src/verification/simulator/schedule/mod.rs",
            "crates/rafter-invariants/src/verification/tla/mod.rs",
            "crates/rafter-invariants/src/verification/target/mod.rs",
            "crates/rafter-invariants/src/verdict/mod.rs",
            "crates/rafter-invariants/src/verdict/report/mod.rs",
            "crates/rafter-invariants/src/contract/registry/parse/tests/mod.rs",
            "crates/rafter-invariants/src/producer/process/tests/mod.rs",
            "crates/rafter-invariants/src/producer/simulator_tests.rs",
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
            (
                "producer",
                "crates/rafter-invariants/src/producer/simulator.rs"
            ),
            (
                "producer",
                "crates/rafter-invariants/src/producer/simulator"
            ),
            (
                "producer",
                "crates/rafter-invariants/src/producer/simulator_events.rs"
            ),
            (
                "producer",
                "crates/rafter-invariants/src/producer/simulator_model.rs"
            ),
            (
                "producer",
                "crates/rafter-invariants/src/producer/source.rs"
            ),
            (
                "producer",
                "crates/rafter-invariants/src/producer/test_compile.rs"
            ),
            (
                "producer",
                "crates/rafter-invariants/src/producer/test_compile"
            ),
            (
                "producer",
                "crates/rafter-invariants/src/producer/test_exec.rs"
            ),
            (
                "producer",
                "crates/rafter-invariants/src/producer/test_exec"
            ),
            ("verification", "crates/rafter-invariants/src/verification"),
            (
                "verification",
                "crates/rafter-invariants/src/artifact_verify/simulator.rs",
            ),
            (
                "verification",
                "crates/rafter-invariants/src/artifact_verify/simulator_schedule.rs",
            ),
            (
                "verification",
                "crates/rafter-invariants/src/artifact_verify/simulator_schedule",
            ),
            (
                "verification",
                "crates/rafter-invariants/src/artifact_verify/test_logs",
            ),
            (
                "verification",
                "crates/rafter-invariants/src/artifact_verify/test_logs.rs",
            ),
            (
                "verification",
                "crates/rafter-invariants/src/artifact_verify_tla.rs",
            ),
            (
                "verification",
                "crates/rafter-invariants/src/receipt_tla.rs",
            ),
            ("verdict", "crates/rafter-invariants/src/verdict"),
            ("gate", "crates/rafter-invariants/src/gate"),
            ("cli", "crates/rafter-invariants/src/cli"),
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
    let verifier_path = "crates/rafter-invariants/src/verification/detector/transcript.rs";
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
fn source_receipt_acceptance_has_independent_producer_and_verifier_policies() {
    let root = workspace_root();
    let producer_path = "crates/rafter-invariants/src/producer/source.rs";
    let verifier_path = "crates/rafter-invariants/src/verification/source/policy.rs";
    let generated_path = "crates/rafter-invariants/src/verification/source/generated_outputs.rs";
    let producer = read(&root.join(producer_path));
    let verifier = read(&root.join(verifier_path));
    let generated = read(&root.join(generated_path));

    for (owner, source) in [("producer", &producer), ("verifier", &verifier)] {
        for layer in ["tests", "simulator", "tla", "maelstrom"] {
            assert!(
                source.contains(&format!("\"{layer}\" =>")),
                "{owner} source policy omitted {layer}"
            );
        }
        assert!(
            source.contains("fn layer_contract("),
            "{owner} source policy has no independent layer reducer"
        );
    }
    assert!(producer.contains("fn reviewed_generated_output("));
    assert!(generated.contains("impl GeneratedOutputPolicy for VerifierGeneratedOutputs"));
    assert!(!producer.contains("crate::verification"));
    assert!(!verifier.contains("crate::producer"));
    assert!(!generated.contains("crate::producer"));

    let neutral = read(&root.join("crates/rafter-invariants/src/provenance/source/checkout.rs"));
    assert!(!neutral.contains("fn layer_contract("));
    assert!(!neutral.contains("tla-detector"));
}

#[test]
fn protected_compiler_artifacts_have_independent_acceptance_policies() {
    let root = workspace_root();
    let producer = "crates/rafter-invariants/src/producer/test_compile/protected.rs";
    let verifier = "crates/rafter-invariants/src/verification/target/protected_compiler.rs";

    for (owner, relative) in [("producer", producer), ("verifier", verifier)] {
        let source = read(&root.join(relative));
        assert!(starts_with_module_contract(&source));
        assert!(
            source.contains("fn verify_protected_compiler_artifacts("),
            "{owner} does not own protected compiler-artifact acceptance"
        );
        assert!(
            source.contains("serde_json") && source.contains("canonicalize"),
            "{owner} policy does not independently decode and bind protected artifacts"
        );
    }

    let declarations = invariant_rust_files(&root)
        .iter()
        .map(|path| {
            read(path)
                .matches("fn verify_protected_compiler_artifacts(")
                .count()
        })
        .sum::<usize>();
    assert_eq!(
        declarations, 2,
        "protected compiler-artifact acceptance must have exactly two independent owners"
    );
    for path in invariant_rust_files(&root) {
        let relative = display_path(&root, &path);
        if relative.starts_with("crates/rafter-invariants/src/provenance/") {
            assert!(
                !read(&path).contains("verify_protected_compiler_artifacts"),
                "neutral provenance absorbed protected compiler-artifact policy in {relative}"
            );
        }
    }
}

#[test]
fn detector_oracle_macro_vocabulary_is_verifier_owned() {
    let root = workspace_root();
    let policy =
        read(&root.join("crates/rafter-invariants/src/verification/detector/source/policy.rs"));
    let target = read(&root.join("crates/rafter-invariants/src/verification/target/mod.rs"));
    assert!(policy.contains("pub(super) use crate::verification::target::ORACLE_MACROS"));
    assert!(target.contains("reserved_macros: &[&str]"));
    assert!(target.contains("pub(crate) const ORACLE_MACROS"));
    assert!(!root
        .join("crates/rafter-invariants/src/artifact_verify/detector_source.rs")
        .exists());
    assert!(!root
        .join("crates/rafter-invariants/src/artifact_verify/detector_source")
        .exists());

    for path in invariant_rust_files(&root) {
        let relative = display_path(&root, &path);
        if relative.starts_with("crates/rafter-invariants/src/provenance/") {
            assert!(
                !read(&path).contains("ORACLE_MACROS"),
                "neutral provenance owns verifier oracle vocabulary in {relative}"
            );
        }
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
fn retired_root_rust_target_ownership_cannot_return() {
    let root = workspace_root();
    for relative in [
        "crates/rafter-invariants/src/rust_target.rs",
        "crates/rafter-invariants/src/rust_target",
    ] {
        assert!(
            !root.join(relative).exists(),
            "retired root Rust-target path returned: {relative}"
        );
    }

    for path in invariant_rust_files(&root) {
        let source = read(&path);
        assert!(
            !source.contains("crate::rust_target") && !source.contains("rust_target::"),
            "{} imports retired root Rust-target ownership",
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
fn verdict_reduction_consumes_only_typed_evidence_intake() {
    let root = workspace_root();
    let reducer_path = "crates/rafter-invariants/src/verdict/aggregate.rs";
    let reducer = read(&root.join(reducer_path));

    assert!(reducer.contains("pub(crate) fn reduce("));
    assert!(reducer.contains("intake: &EvidenceIntake"));
    for forbidden in [
        "ResultBundle",
        "LoadedEvidence",
        "collect_results",
        "load_evidence",
        "aggregate_with_harness_errors",
        "Vec<String>",
        "std::fs",
        "PathBuf",
    ] {
        assert!(
            !reducer.contains(forbidden),
            "{reducer_path} regained raw intake dependency `{forbidden}`"
        );
    }
    assert!(
        !root
            .join("crates/rafter-invariants/src/aggregate.rs")
            .exists(),
        "the retired root aggregate module returned"
    );
    for path in rust_files(&root.join("crates/rafter-invariants/src/verdict")) {
        assert!(
            !read(&path).contains("ResultBundle"),
            "{} accepts unverified result bundles",
            display_path(&root, &path)
        );
    }

    let intake = read(&root.join("crates/rafter-invariants/src/verification/intake/model.rs"));
    for kind in ["Missing", "Malformed", "Stale", "Unverifiable"] {
        assert!(intake.contains(kind), "typed intake is missing `{kind}`");
    }
    assert!(intake.contains("profile: String"));
    assert!(intake.contains("source_ref: String"));

    let gate = read(&root.join("crates/rafter-invariants/src/gate/check.rs"));
    let verify = gate
        .find("verification::verify_aggregate_paths")
        .expect("gate must verify evidence intake");
    let reduce = gate
        .find("verdict::reduce")
        .expect("gate must reduce verified intake");
    assert!(verify < reduce, "gate reduced evidence before verification");
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
fn retired_root_report_mounts_cannot_return() {
    let root = workspace_root();
    for path in [
        "crates/rafter-invariants/src/aggregate.rs",
        "crates/rafter-invariants/src/render.rs",
        "crates/rafter-invariants/src/run_all.rs",
    ] {
        assert!(
            !root.join(path).exists(),
            "retired root mount returned: {path}"
        );
    }
    let library = read(&root.join("crates/rafter-invariants/src/lib.rs"));
    assert!(!library.contains("mod render;"));
}

#[test]
fn report_rendering_is_verdict_owned() {
    let root = workspace_root();
    let expected = [
        "crates/rafter-invariants/src/verdict/report/mod.rs",
        "crates/rafter-invariants/src/verdict/report/junit.rs",
        "crates/rafter-invariants/src/verdict/report/markdown.rs",
    ];
    for path in expected {
        assert!(
            root.join(path).is_file(),
            "missing verdict report module {path}"
        );
    }
    let definitions = invariant_rust_files(&root)
        .into_iter()
        .filter_map(|path| {
            let source = read(&path);
            (source.contains("fn render_junit(") || source.contains("fn render_markdown("))
                .then(|| display_path(&root, &path))
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        definitions,
        BTreeSet::from([
            "crates/rafter-invariants/src/verdict/report/junit.rs".to_owned(),
            "crates/rafter-invariants/src/verdict/report/markdown.rs".to_owned(),
        ])
    );
}

#[test]
fn migrated_domains_keep_test_bodies_in_separate_files() {
    let root = workspace_root();
    let production_files = ENFORCED_DOMAIN_SOURCES
        .iter()
        .flat_map(|source| rust_files(&root.join(source.path)))
        .collect::<BTreeSet<_>>();

    for path in production_files {
        let relative = display_path(&root, &path);
        if is_test_module(&relative) {
            continue;
        }
        let source = read(&path);
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("could not parse {relative}: {error}"));
        let inline_tests = inline_test_bodies(&syntax.items);
        assert!(
            inline_tests.is_empty(),
            "{relative} embeds test bodies instead of declaring sibling test modules: {}",
            inline_tests.join(", ")
        );
    }
}

fn inline_test_bodies(items: &[syn::Item]) -> Vec<String> {
    let mut bodies = Vec::new();
    for item in items {
        match item {
            syn::Item::Fn(function) if function.attrs.iter().any(is_test_attribute) => {
                bodies.push(format!("function {}", function.sig.ident));
            }
            syn::Item::Mod(module) => {
                if module.content.is_some() && attributes_enable_tests(&module.attrs) {
                    bodies.push(format!("module {}", module.ident));
                }
                if let Some((_, nested)) = &module.content {
                    bodies.extend(inline_test_bodies(nested));
                }
            }
            _ => {}
        }
    }
    bodies
}

fn is_test_attribute(attribute: &syn::Attribute) -> bool {
    attribute
        .path()
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "test")
}

fn attributes_enable_tests(attributes: &[syn::Attribute]) -> bool {
    attributes.iter().any(|attribute| {
        if !attribute.path().is_ident("cfg") {
            return false;
        }
        let syn::Meta::List(arguments) = &attribute.meta else {
            return false;
        };
        arguments
            .parse_args::<syn::Meta>()
            .is_ok_and(|condition| cfg_enables_tests(&condition))
    })
}

fn cfg_enables_tests(condition: &syn::Meta) -> bool {
    match condition {
        syn::Meta::Path(path) => path.is_ident("test"),
        syn::Meta::List(arguments) if arguments.path.is_ident("not") => false,
        syn::Meta::List(arguments)
            if arguments.path.is_ident("all") || arguments.path.is_ident("any") =>
        {
            arguments
                .parse_args_with(
                    syn::punctuated::Punctuated::<syn::Meta, syn::Token![,]>::parse_terminated,
                )
                .is_ok_and(|conditions| conditions.iter().any(cfg_enables_tests))
        }
        syn::Meta::List(_) | syn::Meta::NameValue(_) => false,
    }
}

#[test]
fn detector_test_macro_trust_is_bound_to_exact_domain_sources() {
    let root = workspace_root();
    let source = read(&root.join("crates/rafter-invariants/src/verification/target/mod.rs"));

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
fn detector_proof_transport_is_descriptor_only() {
    let root = workspace_root();
    let transport_sources = [
        "crates/rafter-invariant-test/src/detector/proof.rs",
        "crates/rafter-invariant-test/src/detector/wire.rs",
        "crates/rafter-invariants/src/execution/detector_proof.rs",
        "crates/rafter-invariants/src/execution/detector_proof/channel.rs",
        "crates/rafter-invariants/src/execution/detector_proof/responder.rs",
        "crates/rafter-invariants/src/execution/detector_proof/wire.rs",
        "crates/rafter-invariants/src/producer/test_exec/detector_proof.rs",
    ];
    let source = transport_sources
        .iter()
        .map(|path| read(&root.join(path)))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(source.contains("RAFTER_INVARIANT_DETECTOR_PROOF_FD"));
    assert!(source.contains("UnixStream::pair"));
    for retired in [
        "RAFTER_INVARIANT_DETECTOR_PROOF_SOCKET",
        "UnixListener",
        "managed_socket",
        "PROOF_SOCKET",
    ] {
        assert!(
            !source.contains(retired),
            "detector proof transport reintroduced pathname capability `{retired}`"
        );
    }
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
