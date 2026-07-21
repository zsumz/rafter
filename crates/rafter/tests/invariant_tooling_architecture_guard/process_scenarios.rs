//! Scenarios: process lifecycle and evidence ownership remain explicit and non-aliasable.

use std::collections::BTreeMap;

use syn::visit::Visit;

use super::architecture_support::{
    declared_module_graph, declared_module_path, display_path, invariant_rust_files,
    is_declared_test_module, is_legacy_verifier, normalize_rust_path, read, rust_files,
    workspace_root, BlockingProcessCollector, PathContext, RustPathCollector,
};

#[test]
fn retired_producer_process_format_ownership_cannot_return() {
    let root = workspace_root();
    let modules = declared_module_graph(&root);
    for path in rust_files(&root.join("crates/rafter-invariants/src/producer")) {
        let relative = display_path(&root, &path);
        let source = read(&path);
        for retired in [
            "struct ProcessLog",
            "struct ProcessMetrics",
            "struct LabeledProcess",
            "struct TerminationReceipt",
            "fn parse_combined_processes",
            "fn digest_environment",
        ] {
            assert!(
                !source.contains(retired),
                "{relative} reclaimed evidence or provenance ownership through `{retired}`"
            );
        }
    }

    for relative in [
        "crates/rafter-invariants/src/artifact_verify/compile.rs",
        "crates/rafter-invariants/src/artifact_verify/test_logs/runner.rs",
        "crates/rafter-invariants/src/verification/simulator/schedule/compiler.rs",
        "crates/rafter-invariants/src/verification/simulator/schedule/invocation.rs",
    ] {
        assert!(
            read(&root.join(relative)).contains("parse_combined_v4"),
            "{relative} does not enforce the canonical non-detector process schema"
        );
    }

    for path in invariant_rust_files(&root) {
        let relative = display_path(&root, &path);
        if is_legacy_verifier(&relative) && !is_declared_test_module(&modules, &relative) {
            assert!(
                !read(&path).contains("producer::process"),
                "{relative} imports process evidence from its producer"
            );
        }
    }
}

#[test]
fn retired_producer_process_lifecycle_ownership_cannot_return() {
    let root = workspace_root();
    let modules = declared_module_graph(&root);
    for relative in [
        "crates/rafter-invariants/src/producer/process.rs",
        "crates/rafter-invariants/src/producer/process/launch.rs",
        "crates/rafter-invariants/src/producer/process/managed.rs",
        "crates/rafter-invariants/src/producer/process/telemetry.rs",
        "crates/rafter-invariants/src/producer/process/termination.rs",
    ] {
        assert!(
            !root.join(relative).exists(),
            "retired producer process-lifecycle path returned: {relative}"
        );
    }

    for path in rust_files(&root.join("crates/rafter-invariants/src/producer")) {
        let relative = display_path(&root, &path);
        if is_declared_test_module(&modules, &relative) {
            continue;
        }
        let source = read(&path);
        for retired in [
            "Command::new(\"/usr/bin/time\")",
            "TARGET_GROUP_LAUNCHER",
            "collect_process_output(",
            "finish_managed_process(",
            "struct ManagedProcess",
            "fn allocate_telemetry_path",
            "fn parse_peak_rss",
            "fn process_group_observation",
            "fn signal_process_group",
            "fn terminate_after_timeout",
            ".process_group(",
        ] {
            assert!(
                !source.contains(retired),
                "{relative} reclaimed process-lifecycle ownership through `{retired}`"
            );
        }
    }
}

#[test]
fn raw_process_execution_has_exact_non_aliasable_callsites() {
    let root = workspace_root();
    let modules = declared_module_graph(&root);
    let expected = expected_raw_process_accesses();
    let mut observed = BTreeMap::new();

    for path in invariant_rust_files(&root) {
        let relative = display_path(&root, &path);
        if !relative.starts_with("crates/rafter-invariants/src/")
            || relative.starts_with("crates/rafter-invariants/src/execution/process/")
        {
            continue;
        }
        let source = read(&path);
        let syntax = syn::parse_file(&source)
            .unwrap_or_else(|error| panic!("parse {relative} for process ownership: {error}"));
        let mut paths = RustPathCollector::new(declared_module_path(&modules, &relative));
        paths.visit_file(&syntax);
        assert!(
            paths.crate_root_aliases.is_empty(),
            "{relative} aliases the crate root through {:?}",
            paths.crate_root_aliases
        );
        assert!(
            paths.process_macro_tokens.is_empty(),
            "{relative} hides raw process execution in macro tokens: {:?}",
            paths.process_macro_tokens
        );
        for occurrence in paths.occurrences {
            let crate_root_alias =
                occurrence.context == PathContext::Import && occurrence.normalized == ["crate"];
            if occurrence.normalized.first().map(String::as_str) != Some("crate")
                || (!crate_root_alias
                    && occurrence.normalized.get(1).map(String::as_str) != Some("execution"))
            {
                continue;
            }
            if occurrence.normalized.get(2).map(String::as_str) != Some("process")
                && occurrence.normalized.len() > 2
            {
                continue;
            }
            let canonical = occurrence.normalized.join("::");
            assert!(
                occurrence.written == occurrence.normalized,
                "{relative} accesses raw process execution through non-canonical path {:?}; use {canonical}",
                occurrence.written
            );
            let key = (relative.as_str(), occurrence.context, canonical.as_str());
            assert!(
                expected.contains_key(&key),
                "{relative} accesses unreviewed raw process execution via {canonical} ({:?})",
                occurrence.context
            );
            *observed
                .entry((relative.clone(), occurrence.context, canonical))
                .or_insert(0) += 1;
        }
    }

    let expected = expected
        .into_iter()
        .map(|((path, context, dependency), count)| {
            ((path.to_owned(), context, dependency.to_owned()), count)
        })
        .collect::<BTreeMap<_, _>>();
    assert_eq!(observed, expected, "raw process callsite inventory changed");
}

type RawProcessAccess = ((&'static str, PathContext, &'static str), usize);

fn expected_raw_process_accesses() -> BTreeMap<(&'static str, PathContext, &'static str), usize> {
    expected_checkout_process_accesses()
        .into_iter()
        .chain(expected_producer_process_accesses())
        .chain(expected_replay_process_accesses())
        .collect()
}

fn expected_checkout_process_accesses() -> [RawProcessAccess; 1] {
    [(
        (
            "crates/rafter-invariants/src/provenance/source/checkout.rs",
            PathContext::Expression,
            "crate::execution::process::run_identity_command_in",
        ),
        1,
    )]
}

fn expected_producer_process_accesses() -> [RawProcessAccess; 12] {
    [
        (
            (
                "crates/rafter-invariants/src/producer/process/mod.rs",
                PathContext::Import,
                "crate::execution::process::FinalizationPolicy",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/producer/process/mod.rs",
                PathContext::Import,
                "crate::execution::process::TerminationPolicy",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/producer/process/mod.rs",
                PathContext::Import,
                "crate::execution::process::base_environment",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/producer/process/mod.rs",
                PathContext::Import,
                "crate::execution::process::duration_ms",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/producer/process/adapter.rs",
                PathContext::Import,
                "crate::execution::process::ProcessDeadlines",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/producer/process/evidence.rs",
                PathContext::Import,
                "crate::execution::process::BoundCommand",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/producer/process/evidence.rs",
                PathContext::Import,
                "crate::execution::process::FinalizationPolicy",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/producer/process/evidence.rs",
                PathContext::Import,
                "crate::execution::process::PendingProcessOutput",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/producer/process/evidence.rs",
                PathContext::Import,
                "crate::execution::process::ProcessDeadlines",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/producer/process/evidence.rs",
                PathContext::Import,
                "crate::execution::process::TerminationPolicy",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/producer/process/runtime.rs",
                PathContext::Import,
                "crate::execution::process::capture_runtime_identities",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/producer/process/output.rs",
                PathContext::Import,
                "crate::execution::process::ProcessOutput",
            ),
            1,
        ),
    ]
}

fn expected_replay_process_accesses() -> [RawProcessAccess; 4] {
    [
        (
            (
                "crates/rafter-invariants/src/verification/detector_replay/process.rs",
                PathContext::Import,
                "crate::execution::process::base_environment",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/verification/detector_replay/process.rs",
                PathContext::Import,
                "crate::execution::process::run_bounded",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/verification/detector_replay/process.rs",
                PathContext::Import,
                "crate::execution::process::BoundCommand",
            ),
            1,
        ),
        (
            (
                "crates/rafter-invariants/src/verification/detector_replay/process.rs",
                PathContext::Expression,
                "crate::execution::process::retained_diagnostics",
            ),
            1,
        ),
    ]
}

#[test]
fn architecture_paths_normalize_relative_and_module_alias_forms() {
    let module = ["producer", "process", "adapter"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        normalize_rust_path(
            &module,
            &[
                "super".to_owned(),
                "super".to_owned(),
                "super".to_owned(),
                "execution".to_owned(),
                "process".to_owned(),
                "run".to_owned(),
            ],
            PathContext::Expression,
        ),
        Some(
            ["crate", "execution", "process", "run"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        )
    );
    assert_eq!(
        normalize_rust_path(
            &module,
            &[
                "crate".to_owned(),
                "execution".to_owned(),
                "self".to_owned(),
            ],
            PathContext::Import,
        ),
        Some(
            ["crate", "execution"]
                .into_iter()
                .map(str::to_owned)
                .collect()
        )
    );
}

#[test]
fn architecture_path_collector_rejects_crate_alias_and_macro_indirection() {
    let source = r"
        extern crate self as root;
        use crate as another_root;
        macro_rules! hidden_run {
            () => { root::execution::process::run() };
        }
    ";
    let syntax = syn::parse_file(source).expect("parse architecture bypass fixture");
    let mut paths = RustPathCollector::new(Vec::new());
    paths.visit_file(&syntax);

    assert_eq!(paths.crate_root_aliases, ["root"]);
    assert_eq!(paths.process_macro_tokens.len(), 1);
    assert!(paths.occurrences.iter().any(|occurrence| {
        occurrence.context == PathContext::Import && occurrence.normalized == ["crate"]
    }));
}

#[test]
fn production_test_runner_cannot_hide_behind_test_module_classification() {
    let root = workspace_root();
    let modules = declared_module_graph(&root);
    let production = "crates/rafter-invariants/src/producer/test_runner.rs";
    assert!(root.join(production).is_file());
    assert!(!is_declared_test_module(&modules, production));
    assert!(is_declared_test_module(
        &modules,
        "crates/rafter-invariants/src/producer/process/tests/policy.rs"
    ));
    assert!(!root
        .join("crates/rafter-invariants/src/producer/tests.rs")
        .exists());
}

#[test]
fn process_execution_cannot_reintroduce_blocking_child_waits() {
    let root = workspace_root();
    let modules = declared_module_graph(&root);
    for path in rust_files(&root.join("crates/rafter-invariants/src/execution/process")) {
        let relative = display_path(&root, &path);
        if is_declared_test_module(&modules, &relative) {
            continue;
        }
        let syntax = syn::parse_file(&read(&path))
            .unwrap_or_else(|error| panic!("parse {relative} for blocking waits: {error}"));
        let mut blocking = BlockingProcessCollector::default();
        blocking.visit_file(&syntax);
        assert!(
            blocking.calls.is_empty(),
            "{relative} contains blocking process calls {:?}; use deadline-bounded polling",
            blocking.calls
        );
    }
}

#[test]
fn no_signal_reaper_cannot_acquire_a_signal_capability() {
    let root = workspace_root();
    let facade = root.join("crates/rafter-invariants/src/execution/process/reaper.rs");
    let mut paths = vec![facade];
    paths.extend(rust_files(
        &root.join("crates/rafter-invariants/src/execution/process/reaper"),
    ));
    let mut has_observation_only_reap = false;
    for path in paths {
        let relative = display_path(&root, &path);
        let source = read(&path);
        for forbidden in [
            "signal_process_group",
            "ProcessSignal",
            "kill_process_group",
            "kill_process(",
            ".kill(",
        ] {
            assert!(
                !source.contains(forbidden),
                "{relative} acquired forbidden signal capability `{forbidden}`"
            );
        }
        has_observation_only_reap |= source.contains("try_wait()");
    }
    assert!(
        has_observation_only_reap,
        "no-signal reaper module tree must retain an observation-only reap path"
    );
}
