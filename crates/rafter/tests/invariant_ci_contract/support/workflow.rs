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

/// Every step of every job, in file order, bounded the way `workflow_step`
/// bounds one: at the next step or at the next line that leaves step
/// indentation. Deriving the step set is what lets a guard state a property
/// over *all* steps -- a hand-listed inventory only ever states it over the
/// steps somebody remembered.
pub(crate) fn workflow_steps(workflow: &str) -> Vec<&str> {
    let mut starts = Vec::new();
    for (offset, line) in line_offsets(workflow) {
        if line.starts_with("      - ") {
            starts.push(offset);
        }
    }
    starts
        .iter()
        .map(|&start| {
            let end = line_offsets(&workflow[start..])
                .skip(1)
                .find(|(_, line)| {
                    line.starts_with("      - ")
                        || (!line.trim().is_empty() && !line.starts_with("       "))
                })
                .map_or(workflow.len() - start, |(offset, _)| offset);
            &workflow[start..start + end]
        })
        .collect()
}

/// The `path:` entries a step declares, in either the scalar or the block
/// form.
pub(crate) fn workflow_step_paths(step: &str) -> Vec<&str> {
    let mut lines = step.lines().skip_while(|line| !is_step_key(line, "path:"));
    let Some(head) = lines.next() else {
        return Vec::new();
    };
    let scalar = head.trim_start().trim_start_matches("path:").trim();
    if scalar != "|" {
        return if scalar.is_empty() {
            Vec::new()
        } else {
            vec![scalar]
        };
    }
    lines
        .take_while(|line| line.trim().is_empty() || line.starts_with("            "))
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect()
}

fn is_step_key(line: &str, key: &str) -> bool {
    line.strip_prefix("          ")
        .is_some_and(|rest| rest.starts_with(key))
}

fn line_offsets(source: &str) -> impl Iterator<Item = (usize, &str)> {
    let mut offset = 0;
    source.split_inclusive('\n').map(move |line| {
        let start = offset;
        offset += line.len();
        (start, line.trim_end_matches('\n'))
    })
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
