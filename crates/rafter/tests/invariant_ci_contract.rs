use std::path::{Path, PathBuf};

#[test]
fn pr_invariant_aggregate_is_stable_and_fail_closed() {
    let root = workspace_root();
    let workflow = read(&root.join(".github/workflows/ci.yml"));

    for (job, layer) in [
        ("invariants-tests", "tests"),
        ("invariants-simulator", "simulator"),
        ("invariants-tla", "tla"),
    ] {
        let block = job_block(&workflow, job);
        assert!(
            block.contains(&format!(
                "cargo run --locked -p rafter-invariants -- run --profile pr --layer {layer}"
            )),
            "{job} must invoke its source-bound producer"
        );
        assert!(
            block.contains("if: always()") && block.contains("actions/upload-artifact@v4"),
            "{job} must preserve evidence even when the producer fails"
        );
    }

    let maelstrom = job_block(&workflow, "invariants-maelstrom");
    assert!(maelstrom.contains("Validate scheduled Maelstrom evidence contract"));
    assert!(!maelstrom.contains("--profile pr --layer maelstrom"));

    let aggregate = job_block(&workflow, "invariants-pr");
    for dependency in [
        "invariants-tests",
        "invariants-simulator",
        "invariants-tla",
        "invariants-maelstrom",
    ] {
        assert!(aggregate.contains(&format!("- {dependency}")));
    }
    for required in [
        "if: always()",
        "actions/download-artifact@v4",
        "continue-on-error: true",
        "check --profile pr --source-ref \"$GITHUB_SHA\"",
        "GITHUB_STEP_SUMMARY",
        "actions/upload-artifact@v4",
        "needs.invariants-tests.result",
        "needs.invariants-simulator.result",
        "needs.invariants-tla.result",
        "needs.invariants-maelstrom.result",
        ".summary.total == 44",
        ".summary.green == 44",
        "(.invariants | length) == 44",
    ] {
        assert!(
            aggregate.contains(required),
            "invariants-pr omitted required contract fragment: {required}"
        );
    }

    let readme = read(&root.join("README.md"));
    assert!(readme.contains("Branch protection on `main` requires the stable `invariants-pr`"));
}

#[test]
fn scheduled_profiles_run_real_maelstrom_evidence() {
    let root = workspace_root();
    for (workflow, profile) in [
        (".github/workflows/nightly.yml", "nightly"),
        (".github/workflows/weekly.yml", "weekly"),
    ] {
        let source = read(&root.join(workflow));
        let block = job_block(&source, "invariants-maelstrom");
        assert!(block.contains(&format!(
            "cargo run --locked -p rafter-invariants -- run --profile {profile} --layer maelstrom"
        )));
        assert!(block.contains("cargo run --locked -p rafter-invariants -- verify-layer"));
        assert!(block.contains("if: always()"));
        assert!(block.contains("actions/upload-artifact@v4"));
    }
}

fn job_block<'a>(workflow: &'a str, id: &str) -> &'a str {
    let marker = format!("\n  {id}:\n");
    let start = workflow
        .find(&marker)
        .unwrap_or_else(|| panic!("workflow job {id} is missing"))
        + marker.len();
    let tail = &workflow[start..];
    let end = tail
        .match_indices("\n  ")
        .find_map(|(offset, _)| {
            let line = tail[offset + 1..].lines().next()?;
            (line.starts_with("  ") && !line.starts_with("    ") && line.trim_end().ends_with(':'))
                .then_some(offset)
        })
        .unwrap_or(tail.len());
    &tail[..end]
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
}
