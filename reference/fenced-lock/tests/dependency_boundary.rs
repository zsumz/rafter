const ROOT_WORKSPACE: &str = include_str!("../../../Cargo.toml");
const REFERENCE_WORKSPACE: &str = include_str!("../../Cargo.toml");
const FENCED_LOCK_MANIFEST: &str = include_str!("../Cargo.toml");

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
        REFERENCE_WORKSPACE.contains("members = [\"fenced-lock\", \"ledger\"]"),
        "reference consumers must be listed one by one, never globbed"
    );
}

#[test]
fn canonical_consumer_manifest_has_no_checkout_or_internal_hook_dependency() {
    assert!(
        !FENCED_LOCK_MANIFEST.contains("path ="),
        "canonical consumer dependencies must not point into the checkout"
    );
    assert!(
        !FENCED_LOCK_MANIFEST.contains("internal-test-hooks"),
        "reference consumers must not use unpublished internal hooks"
    );
    assert!(
        FENCED_LOCK_MANIFEST.contains("publish = false"),
        "reference consumers are acceptance systems, not published products"
    );
}

#[test]
fn this_slice_declares_no_dependencies_at_all() {
    assert!(
        !FENCED_LOCK_MANIFEST.contains("[dependencies]"),
        "the foundation slice is dependency-free, including of Rafter itself"
    );
    assert!(
        !FENCED_LOCK_MANIFEST.contains("rafter-reference-ledger"),
        "reference consumers must not share code with one another"
    );
}
