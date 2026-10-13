//! Local inventory and optional archive qualification keep their full contracts.

use std::path::Path;

pub(super) fn assert_durable_inventory(root: &Path, selection: &str) {
    let local = super::read(&root.join("zcheck.toml"));
    let local_task = local
        .split("[tasks.durable-client]")
        .nth(1)
        .and_then(|task| task.split("\n[").next())
        .expect("local durable-client task");
    let arguments = selection
        .split_whitespace()
        .map(|argument| format!("{argument:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    assert!(
        local_task.contains(&format!("run = [{arguments}]")),
        "local durable inventory must match the reviewed CI selection"
    );
}

pub(super) fn assert_nightly_archive_profile(nightly: &str) {
    let (header, jobs) = nightly.split_once("jobs:\n").expect("nightly jobs");
    let input = header
        .lines()
        .skip_while(|line| line.trim() != "archives_only:")
        .skip(1)
        .take_while(|line| line.starts_with("        "))
        .collect::<Vec<_>>();
    assert!(input.contains(&"        type: boolean"));
    assert!(input.contains(&"        default: false"));
    for required in [
        "group: ${{ inputs.archives_only && 'candidate-archives' || 'nightly' }}-${{ github.ref }}",
        "cancel-in-progress: false",
        "inputs.archives_only && 'Candidate archive qualification' || 'Nightly'",
    ] {
        assert!(
            header.contains(required),
            "nightly profile omitted {required}"
        );
    }
    for name in jobs.lines().filter_map(|line| {
        line.strip_prefix("  ")
            .filter(|line| !line.starts_with(' '))
            .and_then(|line| line.strip_suffix(':'))
    }) {
        let job = super::workflow_job(nightly, name);
        let conditions = job
            .lines()
            .filter(|line| line.starts_with("    if:"))
            .collect::<Vec<_>>();
        let expected = match name {
            "candidate-archives" => "    if: ${{ inputs.archives_only }}",
            "invariants-nightly" => {
                "    if: ${{ !inputs.archives_only && always() && !cancelled() }}"
            }
            _ => "    if: ${{ !inputs.archives_only }}",
        };
        assert_eq!(conditions, [expected], "nightly routing differs for {name}");
    }
    let archives = super::workflow_job(nightly, "candidate-archives");
    for required in [
        "toolchain: \"1.88.0\"",
        "toolchain: \"1.97.1\"",
        "scripts/reference-package-process-check --msrv-toolchain 1.88.0",
        "cargo install cargo-audit --version 0.22.2 --locked",
        "python3 scripts/archive_dependencies.py --audit",
        "git rev-parse HEAD",
        "$GITHUB_SHA",
        "git status --porcelain",
        "git ls-files -z | xargs -0 sha256sum",
        "sha256sum --check --quiet",
        "source-preserved.txt",
        "include-hidden-files: true",
        "if-no-files-found: error",
    ] {
        assert!(
            archives.contains(required),
            "archive lane omitted {required}"
        );
    }
    for forbidden in ["--process-only", "--msrv-only", "continue-on-error:"] {
        assert!(
            !archives.contains(forbidden),
            "archive lane weakened: {forbidden}"
        );
    }
}
