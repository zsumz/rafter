//! Recorded producer workspace mapping scenarios.

use super::*;

#[test]
fn recorded_paths_map_by_workspace_relative_identity() {
    let active = fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("../.."))
        .expect("canonical workspace");
    let (catalog, manifest) = crate::tests::loaded();
    let mut bundle = crate::tests::passing_bundles(&catalog, &manifest).remove(0);
    bundle.execution.invocation.current_dir = "/producer/rafter".to_owned();
    let workspace = RecordedWorkspace::new(&bundle, &active).expect("recorded workspace");
    let recorded = Path::new("/producer/rafter/crates/rafter/src/lib.rs");
    assert_eq!(
        workspace.map(recorded, "test source").expect("mapped path"),
        active.join("crates/rafter/src/lib.rs")
    );
    workspace
        .verify_active_file(recorded, "test source")
        .expect("active source verifies");
    assert!(workspace
        .map(Path::new("/producer/sibling/lib.rs"), "escaped source")
        .is_err());
}
