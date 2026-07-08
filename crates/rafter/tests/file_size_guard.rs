use std::{
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

const LIBRARY_TARGET_LINES: usize = 300;
const LIBRARY_SOFT_LINES: usize = 700;
const LIBRARY_HARD_LINES: usize = 1_000;
const AUXILIARY_TARGET_LINES: usize = 600;
const AUXILIARY_SOFT_LINES: usize = 1_000;
const AUXILIARY_HARD_LINES: usize = 1_500;
const MAX_RATCHET_WARNINGS_TO_PRINT: usize = 25;

const SIZE_ALLOWLIST: &[SizeAllow] = &[];

#[derive(Clone, Copy)]
struct SizeAllow {
    path: &'static str,
    tracking_label: &'static str,
    reason: &'static str,
}

#[derive(Clone, Copy)]
struct SizeLimits {
    target: usize,
    soft: usize,
    hard: usize,
    label: &'static str,
}

#[derive(Debug, Eq, PartialEq)]
struct SizeWarning {
    line_count: usize,
    message: String,
}

#[test]
fn file_size_guard_enforces_module_size_limits() {
    let workspace = workspace_root();
    let files = guarded_rust_files(&workspace);
    let mut ratchet_warnings = Vec::new();
    let mut warnings = Vec::new();
    let mut violations = Vec::new();

    validate_allowlist(&workspace, &files, &mut violations);
    for path in files {
        check_file_size(
            &workspace,
            &path,
            &mut ratchet_warnings,
            &mut warnings,
            &mut violations,
        );
    }

    ratchet_warnings.sort_by(|left, right| {
        right
            .line_count
            .cmp(&left.line_count)
            .then(left.message.cmp(&right.message))
    });

    if !ratchet_warnings.is_empty() {
        eprintln!(
            "file-size ratchet targets (warning-only; {} files over target, showing up to {}):\n{}",
            ratchet_warnings.len(),
            MAX_RATCHET_WARNINGS_TO_PRINT,
            render_ratchet_warnings(&ratchet_warnings)
        );
    }
    if !warnings.is_empty() {
        eprintln!("file-size guard warnings:\n{}", warnings.join("\n"));
    }
    assert!(
        violations.is_empty(),
        "file-size guard violations:\n{}\n\nSplit the file, or add a reviewed temporary allowlist entry with a reason and tracking label.",
        violations.join("\n")
    );
}

fn validate_allowlist(workspace: &Path, files: &[PathBuf], violations: &mut Vec<String>) {
    for allow in SIZE_ALLOWLIST {
        if allow.reason.trim().is_empty() || allow.tracking_label.trim().is_empty() {
            violations.push(format!(
                "{}: allowlist entries must include a reason and tracking label",
                allow.path
            ));
            continue;
        }

        let path = workspace.join(allow.path);
        if !files.iter().any(|candidate| candidate == &path) {
            violations.push(format!(
                "{}: allowlist entry points to a missing Rust file",
                allow.path
            ));
            continue;
        }

        let line_count = count_lines(&path);
        let limits = limits_for(allow.path);
        if line_count <= limits.hard {
            violations.push(format!(
                "{}:{}: allowlist entry is no longer needed; remove it from SIZE_ALLOWLIST ({}, tracking label {})",
                allow.path, line_count, allow.reason, allow.tracking_label
            ));
        }
    }
}

fn check_file_size(
    workspace: &Path,
    path: &Path,
    ratchet_warnings: &mut Vec<SizeWarning>,
    warnings: &mut Vec<String>,
    violations: &mut Vec<String>,
) {
    let relative_path = display_path(workspace, path);
    let line_count = count_lines(path);
    let limits = limits_for(&relative_path);
    let allow = allowlist_entry(&relative_path);

    if line_count > limits.target {
        ratchet_warnings.push(SizeWarning {
            line_count,
            message: format!(
                "{}:{}: {} file exceeds ratchet target of {} lines",
                relative_path, line_count, limits.label, limits.target
            ),
        });
    }

    if line_count > limits.soft {
        warnings.push(format!(
            "{}:{}: {} file exceeds soft limit of {} lines",
            relative_path, line_count, limits.label, limits.soft
        ));
    }

    if line_count <= limits.hard {
        return;
    }

    if let Some(allow) = allow {
        warnings.push(format!(
            "{}:{}: over hard limit of {} lines, temporarily allowed for {} ({})",
            relative_path, line_count, limits.hard, allow.tracking_label, allow.reason
        ));
    } else {
        violations.push(format!(
            "{}:{}: {} file exceeds hard limit of {} lines",
            relative_path, line_count, limits.label, limits.hard
        ));
    }
}

fn limits_for(relative_path: &str) -> SizeLimits {
    if is_auxiliary_file(relative_path) {
        SizeLimits {
            target: AUXILIARY_TARGET_LINES,
            soft: AUXILIARY_SOFT_LINES,
            hard: AUXILIARY_HARD_LINES,
            label: "test/example/binary",
        }
    } else {
        SizeLimits {
            target: LIBRARY_TARGET_LINES,
            soft: LIBRARY_SOFT_LINES,
            hard: LIBRARY_HARD_LINES,
            label: "library implementation",
        }
    }
}

fn is_auxiliary_file(relative_path: &str) -> bool {
    relative_path.starts_with("fuzz/")
        || relative_path.contains("/examples/")
        || relative_path.contains("/tests/")
        || relative_path.contains("/src/bin/")
        || relative_path.ends_with("/src/main.rs")
        || relative_path.ends_with("/src/tests.rs")
        || relative_path.ends_with("_test.rs")
}

fn allowlist_entry(relative_path: &str) -> Option<&'static SizeAllow> {
    SIZE_ALLOWLIST
        .iter()
        .find(|allow| allow.path == relative_path)
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
}

fn guarded_rust_files(workspace: &Path) -> Vec<PathBuf> {
    let mut files = rust_files(&workspace.join("crates"));
    let fuzz_root = workspace.join("fuzz");
    if fuzz_root.exists() {
        files.extend(rust_files(&fuzz_root));
    }
    files.sort();
    files
}

fn rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_rust_files(root, &mut files);
    files.sort();
    files
}

fn collect_rust_files(root: &Path, files: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(root).unwrap_or_else(|error| panic!("read {}: {error}", root.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry under {}: {error}", root.display()))
            .path();
        if path.is_dir() {
            collect_rust_files(&path, files);
        } else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
            files.push(path);
        }
    }
}

fn count_lines(path: &Path) -> usize {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
        .lines()
        .count()
}

fn display_path(workspace: &Path, path: &Path) -> String {
    path.strip_prefix(workspace)
        .unwrap_or(path)
        .display()
        .to_string()
}

fn render_ratchet_warnings(warnings: &[SizeWarning]) -> String {
    let mut output = warnings
        .iter()
        .take(MAX_RATCHET_WARNINGS_TO_PRINT)
        .map(|warning| warning.message.as_str())
        .collect::<Vec<_>>()
        .join("\n");
    let omitted = warnings.len().saturating_sub(MAX_RATCHET_WARNINGS_TO_PRINT);
    if omitted > 0 {
        write!(
            output,
            "\n... {omitted} more files over ratchet target omitted"
        )
        .expect("writing to a String should not fail");
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn library_files_use_three_hundred_line_ratchet_target() {
        let limits = limits_for("crates/rafter/src/node/replication.rs");

        assert_eq!(limits.target, LIBRARY_TARGET_LINES);
        assert_eq!(limits.soft, LIBRARY_SOFT_LINES);
        assert_eq!(limits.hard, LIBRARY_HARD_LINES);
        assert_eq!(limits.label, "library implementation");
    }

    #[test]
    fn fuzz_targets_use_auxiliary_size_limits() {
        let limits = limits_for("fuzz/fuzz_targets/cluster_schedules.rs");

        assert_eq!(limits.target, AUXILIARY_TARGET_LINES);
        assert_eq!(limits.soft, AUXILIARY_SOFT_LINES);
        assert_eq!(limits.hard, AUXILIARY_HARD_LINES);
        assert_eq!(limits.label, "test/example/binary");
    }
}
