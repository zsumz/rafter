const ROOT_WORKSPACE: &str = include_str!("../../../Cargo.toml");
const REFERENCE_WORKSPACE: &str = include_str!("../../Cargo.toml");
const SHARDED_COUNTER_MANIFEST: &str = include_str!("../Cargo.toml");
const REAL_ADAPTER_SOURCES: &[(&str, &str)] = &[
    ("adapter/mod.rs", include_str!("../src/adapter/mod.rs")),
    ("adapter/audit.rs", include_str!("../src/adapter/audit.rs")),
    (
        "adapter/cluster.rs",
        include_str!("../src/adapter/cluster.rs"),
    ),
    (
        "adapter/cluster/admission.rs",
        include_str!("../src/adapter/cluster/admission.rs"),
    ),
    (
        "adapter/cluster/checkpoint.rs",
        include_str!("../src/adapter/cluster/checkpoint.rs"),
    ),
    (
        "adapter/cluster/drive.rs",
        include_str!("../src/adapter/cluster/drive.rs"),
    ),
    (
        "adapter/cluster/lifecycle.rs",
        include_str!("../src/adapter/cluster/lifecycle.rs"),
    ),
    (
        "adapter/state_machine.rs",
        include_str!("../src/adapter/state_machine.rs"),
    ),
];

#[test]
fn reference_workspace_is_explicitly_isolated_from_the_root() {
    assert!(
        ROOT_WORKSPACE.contains("exclude = [\"reference\"]"),
        "the root workspace must not absorb reference consumers"
    );
    assert!(
        REFERENCE_WORKSPACE.starts_with("[workspace]\n"),
        "reference consumers need their own workspace root"
    );
    assert!(
        REFERENCE_WORKSPACE
            .contains("members = [\"fenced-lock\", \"harness\", \"ledger\", \"sharded-counter\"]"),
        "reference consumers must be listed one by one, never globbed"
    );
}

#[test]
fn canonical_consumer_manifest_has_no_checkout_or_internal_hook_dependency() {
    assert!(
        !SHARDED_COUNTER_MANIFEST.contains("path ="),
        "canonical consumer dependencies must not point into the checkout"
    );
    assert!(
        !SHARDED_COUNTER_MANIFEST.contains("internal-test-hooks"),
        "reference consumers must not use unpublished internal hooks"
    );
    assert!(
        SHARDED_COUNTER_MANIFEST.contains("publish = false"),
        "reference consumers are acceptance systems, not published products"
    );
}

#[test]
fn the_real_adapter_uses_only_versioned_public_rafter_crates() {
    assert!(SHARDED_COUNTER_MANIFEST.contains("[dependencies]"));
    for dependency in [
        "rafter",
        "rafter-app",
        "rafter-multiraft",
        "rafter-runtime",
        "rafter-service",
        "rafter-storage",
        "rafter-transport-tls",
    ] {
        let requirement = format!("{dependency} = \"0.0.1\"");
        assert!(
            SHARDED_COUNTER_MANIFEST
                .lines()
                .any(|line| line.trim() == requirement),
            "the real adapter must name {dependency} by version"
        );
    }
    assert!(
        !SHARDED_COUNTER_MANIFEST.contains("[dev-dependencies]")
            && !SHARDED_COUNTER_MANIFEST.contains("[build-dependencies]"),
        "the adapter needs no test-only or build-only dependency path"
    );
}

#[test]
fn no_reference_consumer_depends_on_another() {
    for sibling in ["rafter-reference-ledger", "rafter-reference-fenced-lock"] {
        assert!(
            !SHARDED_COUNTER_MANIFEST.contains(sibling),
            "reference consumers must not share code with one another"
        );
    }
}

#[test]
fn the_real_adapter_cannot_borrow_model_or_oracle_answers() {
    for (path, source) in REAL_ADAPTER_SOURCES {
        for forbidden in [
            "crate::model",
            "crate::oracle",
            "ManagedScheduler",
            "ReferenceScheduler",
            "SchedulerState",
            "OracleState",
        ] {
            assert!(
                !source.contains(forbidden),
                "{path} borrows the independent answer through {forbidden}"
            );
        }
    }
}
