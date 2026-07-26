const ROOT_WORKSPACE: &str = include_str!("../../../Cargo.toml");
const REFERENCE_WORKSPACE: &str = include_str!("../../Cargo.toml");
const SHARDED_COUNTER_MANIFEST: &str = include_str!("../Cargo.toml");

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
            .contains("members = [\"fenced-lock\", \"ledger\", \"sharded-counter\"]"),
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

/// This consumer's boundary is stricter than its siblings', and deliberately so.
///
/// The managed scheduler it specifies does not exist in Rafter yet. A Rafter
/// dependency here would mean this contract had been shaped by a surface it was
/// supposed to be shaping, so the absence is the invariant, not an accident of
/// the crate being small.
#[test]
fn the_scheduler_contract_depends_on_nothing_at_all() {
    for section in [
        "[dependencies]",
        "[dev-dependencies]",
        "[build-dependencies]",
    ] {
        assert!(
            !SHARDED_COUNTER_MANIFEST.contains(section),
            "the sharded counter's contract must be arguable before any API exists to argue it against, so {section} may not appear"
        );
    }

    // The package's own name starts with `rafter-`, so a substring search would
    // find itself. A requirement is a `name = ` line outside `[package]`.
    let requirements: Vec<&str> = SHARDED_COUNTER_MANIFEST
        .lines()
        .skip_while(|line| line.trim() != "[package]")
        .skip(1)
        .take_while(|line| !line.trim_start().starts_with('[') || line.trim() == "[package]")
        .filter(|line| line.contains(" = ") && line.trim_start().starts_with("rafter-"))
        .collect();
    assert!(
        requirements.is_empty(),
        "no Rafter crate may be required until the managed scheduler this contract specifies exists: {requirements:?}"
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
