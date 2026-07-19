//! Producer integration stories for execution-filesystem deadline enforcement.

use std::{fs, path::PathBuf, time::Instant};

use crate::{execution::filesystem::HeldDirectory, ArtifactRef};

use super::{maelstrom_exec, simulator_model, test_compile, test_exec};

fn test_path(label: &str) -> PathBuf {
    PathBuf::from("target/rafter-invariants/filesystem-tests")
        .join(format!("{label}-{}", std::process::id()))
}

#[test]
fn layer_scratch_cleanup_rejects_expired_deadlines_before_mutation() {
    let source_ref = format!("deadline-{}", std::process::id());
    let compile_profile = format!("compile-expired-{}", std::process::id());
    let compile_path = PathBuf::from("target/rafter-invariants/build")
        .join(&source_ref)
        .join(format!("{compile_profile}-tests"));
    let test_scratch_path = test_path("test-expired");
    let simulator_path = test_path("simulator-expired");
    let maelstrom_path = test_path("maelstrom-expired");
    for path in [
        &compile_path,
        &test_scratch_path,
        &simulator_path,
        &maelstrom_path,
    ] {
        let _ = fs::remove_dir_all(path);
    }

    assert!(
        test_compile::prepare_target_dir(&compile_profile, &source_ref, Instant::now()).is_err()
    );
    assert!(test_exec::reset_test_scratch(&test_scratch_path, Instant::now()).is_err());
    assert!(
        simulator_model::reset_simulator_build_scratch(&simulator_path, Instant::now()).is_err()
    );
    assert!(maelstrom_exec::reset_state_directory(&maelstrom_path, Instant::now()).is_err());

    for path in [
        &compile_path,
        &test_scratch_path,
        &simulator_path,
        &maelstrom_path,
    ] {
        assert!(!path.exists(), "expired cleanup created {}", path.display());
    }
}

#[test]
fn maelstrom_discovery_and_evidence_traversal_obey_expired_deadlines() {
    let root = test_path("maelstrom-traversal-expired");
    let output = test_path("maelstrom-traversal-output");
    let _ = fs::remove_dir_all(&root);
    let _ = fs::remove_dir_all(&output);
    fs::create_dir_all(root.join("store/lin-kv/run/node-logs"))
        .expect("create Maelstrom traversal fixture");
    fs::write(root.join("store/lin-kv/run/results.edn"), b"{}").expect("write results fixture");
    let held = HeldDirectory::open(&root).expect("hold Maelstrom fixture");

    assert!(maelstrom_exec::discover_store(&held, Instant::now()).is_err());
    let mut artifacts = Vec::<ArtifactRef>::new();
    assert!(maelstrom_exec::capture_tree(
        &output,
        std::path::Path::new("fixture"),
        &held,
        &mut artifacts,
        Instant::now(),
    )
    .is_err());
    assert!(artifacts.is_empty());
    assert!(!output.exists());

    fs::remove_dir_all(root).expect("remove Maelstrom traversal fixture");
}
