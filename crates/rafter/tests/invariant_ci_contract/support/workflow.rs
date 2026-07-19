//! Workflow text extraction and executable shell-step fixtures.

use std::{
    path::{Path, PathBuf},
    process::{Command, Output},
};

pub(crate) fn workflow_step<'a>(job: &'a str, name: &str) -> &'a str {
    let marker = format!("      - name: {name}\n");
    let start = job
        .find(&marker)
        .unwrap_or_else(|| panic!("workflow step {name} is missing"));
    let tail = &job[start..];
    let end = tail[marker.len()..]
        .find("\n      - name: ")
        .map_or(tail.len(), |offset| marker.len() + offset);
    &tail[..end]
}

pub(crate) fn run_workflow_script(
    step: &str,
    current_dir: &Path,
    environment: &[(&str, &str)],
) -> Output {
    let marker = "        run: |\n";
    let script = step
        .split_once(marker)
        .unwrap_or_else(|| panic!("workflow step omitted a shell script: {step}"))
        .1
        .lines()
        .take_while(|line| line.is_empty() || line.starts_with("          "))
        .map(|line| line.strip_prefix("          ").unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n");
    let mut command = Command::new("bash");
    command
        .args(["-eu", "-o", "pipefail", "-c", &script])
        .current_dir(current_dir);
    for (name, value) in environment {
        command.env(name, value);
    }
    command.output().expect("execute workflow shell fixture")
}

pub(crate) fn assert_success(output: &Output, label: &str) {
    assert!(
        output.status.success(),
        "{label} failed:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(crate) fn assert_failure(output: &Output, label: &str) {
    assert!(
        !output.status.success(),
        "{label} unexpectedly succeeded:\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub(crate) fn job_block<'a>(workflow: &'a str, id: &str) -> &'a str {
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

pub(crate) fn read(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

pub(crate) fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
}
