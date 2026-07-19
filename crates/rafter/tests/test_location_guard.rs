use std::{
    fs,
    path::{Path, PathBuf},
};

const INLINE_TEST_SOFT_LINES: usize = 120;
const INLINE_TEST_HARD_LINES: usize = 250;
const INLINE_TEST_ALLOWLIST: &[InlineTestAllow] = &[];

#[derive(Clone, Copy)]
struct InlineTestAllow {
    path: &'static str,
    module: &'static str,
    tracking_label: &'static str,
    reason: &'static str,
}

#[derive(Debug, Eq, PartialEq)]
struct InlineTestModule {
    path: String,
    module: String,
    start_line: usize,
    line_count: usize,
}

#[test]
fn test_location_guard_limits_large_inline_test_modules() {
    let workspace = workspace_root();
    let mut modules = Vec::new();
    for path in guarded_implementation_rust_files(&workspace) {
        let relative_path = display_path(&workspace, &path);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        modules.extend(inline_test_modules(&relative_path, &source));
    }

    let mut warnings = Vec::new();
    let mut violations = Vec::new();
    validate_allowlist(&modules, &mut violations);
    for module in &modules {
        check_inline_test_module(module, &mut warnings, &mut violations);
    }

    if !warnings.is_empty() {
        eprintln!("test-location guard warnings:\n{}", warnings.join("\n"));
    }
    assert!(
        violations.is_empty(),
        "test-location guard violations:\n{}\n\nMove behavior tests into crate `tests/`, a dedicated `src/tests/` module tree, or add a reviewed temporary allowlist entry with a tracking label.",
        violations.join("\n")
    );
}

#[test]
fn test_location_guard_detects_inline_test_module_spans() {
    let source = r#"
pub fn production() {}

#[cfg(test)]
mod tests {
    #[test]
    fn works() {
        assert_eq!(1, 1);
    }
}

#[cfg(test)]
#[path = "tests.rs"]
mod external_tests;
"#;

    assert_eq!(
        inline_test_modules("crates/demo/src/lib.rs", source),
        vec![InlineTestModule {
            path: "crates/demo/src/lib.rs".to_owned(),
            module: "tests".to_owned(),
            start_line: 5,
            line_count: 6,
        }]
    );
}

fn validate_allowlist(modules: &[InlineTestModule], violations: &mut Vec<String>) {
    for allow in INLINE_TEST_ALLOWLIST {
        if allow.reason.trim().is_empty() || allow.tracking_label.trim().is_empty() {
            violations.push(format!(
                "{}::{}: allowlist entries must include a reason and tracking label",
                allow.path, allow.module
            ));
            continue;
        }

        let Some(module) = modules
            .iter()
            .find(|candidate| candidate.path == allow.path && candidate.module == allow.module)
        else {
            violations.push(format!(
                "{}::{}: allowlist entry points to a missing inline test module",
                allow.path, allow.module
            ));
            continue;
        };

        if module.line_count <= INLINE_TEST_HARD_LINES {
            violations.push(format!(
                "{}:{}: inline test module `{}` allowlist entry is stale; remove it ({}, tracking label {})",
                module.path, module.start_line, module.module, allow.reason, allow.tracking_label
            ));
        }
    }
}

fn check_inline_test_module(
    module: &InlineTestModule,
    warnings: &mut Vec<String>,
    violations: &mut Vec<String>,
) {
    let allow = allowlist_entry(module);
    if module.line_count > INLINE_TEST_SOFT_LINES {
        warnings.push(format!(
            "{}:{}: inline test module `{}` is {} lines, above the soft limit of {}",
            module.path,
            module.start_line,
            module.module,
            module.line_count,
            INLINE_TEST_SOFT_LINES
        ));
    }

    if module.line_count <= INLINE_TEST_HARD_LINES {
        return;
    }

    if let Some(allow) = allow {
        warnings.push(format!(
            "{}:{}: inline test module `{}` is above the hard limit of {} lines, temporarily allowed for {} ({})",
            module.path,
            module.start_line,
            module.module,
            INLINE_TEST_HARD_LINES,
            allow.tracking_label,
            allow.reason
        ));
    } else {
        violations.push(format!(
            "{}:{}: inline test module `{}` is {} lines, above the hard limit of {}",
            module.path,
            module.start_line,
            module.module,
            module.line_count,
            INLINE_TEST_HARD_LINES
        ));
    }
}

fn allowlist_entry(module: &InlineTestModule) -> Option<&'static InlineTestAllow> {
    INLINE_TEST_ALLOWLIST
        .iter()
        .find(|allow| allow.path == module.path && allow.module == module.module)
}

fn inline_test_modules(relative_path: &str, source: &str) -> Vec<InlineTestModule> {
    let lines = source.lines().collect::<Vec<_>>();
    let mut modules = Vec::new();
    for (index, line) in lines.iter().enumerate() {
        if line.trim() != "#[cfg(test)]" {
            continue;
        }

        let Some(module_line_index) = next_item_line_index(&lines, index + 1) else {
            continue;
        };
        let module_line = lines[module_line_index].trim();
        let Some(module_name) = module_decl_name(module_line) else {
            continue;
        };
        if module_line.ends_with(';') {
            continue;
        }
        let Some(end_line_index) = module_end_line_index(&lines, module_line_index) else {
            continue;
        };

        modules.push(InlineTestModule {
            path: relative_path.to_owned(),
            module: module_name,
            start_line: module_line_index + 1,
            line_count: end_line_index - module_line_index + 1,
        });
    }
    modules
}

fn next_item_line_index(lines: &[&str], start: usize) -> Option<usize> {
    lines
        .iter()
        .enumerate()
        .skip(start)
        .find_map(|(index, line)| {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with("//") || trimmed.starts_with("#[") {
                None
            } else {
                Some(index)
            }
        })
}

fn module_decl_name(line: &str) -> Option<String> {
    let rest = line
        .strip_prefix("mod ")
        .or_else(|| line.strip_prefix("pub mod "))
        .or_else(|| line.strip_prefix("pub(crate) mod "))
        .or_else(|| line.strip_prefix("pub(super) mod "))?;
    let name = rest
        .split(|ch: char| !ch.is_ascii_alphanumeric() && ch != '_')
        .next()?;
    (!name.is_empty()).then(|| name.to_owned())
}

fn module_end_line_index(lines: &[&str], start: usize) -> Option<usize> {
    let mut depth = 0usize;
    let mut seen_open = false;
    for (offset, line) in lines.iter().enumerate().skip(start) {
        for ch in line.chars() {
            match ch {
                '{' => {
                    depth += 1;
                    seen_open = true;
                }
                '}' if seen_open => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        return Some(offset);
                    }
                }
                _ => {}
            }
        }
    }
    None
}

fn implementation_rust_files(root: &Path) -> Vec<PathBuf> {
    let workspace = workspace_root();
    let mut files = Vec::new();
    collect_rust_files(root, &mut files);
    files.retain(|path| {
        let relative_path = display_path(&workspace, path);
        is_implementation_source(&relative_path)
    });
    files.sort();
    files
}

fn is_implementation_source(relative_path: &str) -> bool {
    relative_path.starts_with("fuzz/")
        || relative_path.contains("/src/")
            && !relative_path.contains("/src/tests/")
            && !relative_path.ends_with("/src/tests.rs")
            && !relative_path.ends_with("_test.rs")
            && !relative_path.ends_with("_tests.rs")
            && !relative_path.contains("/tests/")
            && !relative_path.contains("/examples/")
}

fn guarded_implementation_rust_files(workspace: &Path) -> Vec<PathBuf> {
    let mut files = implementation_rust_files(&workspace.join("crates"));
    let fuzz_root = workspace.join("fuzz");
    if fuzz_root.exists() {
        files.extend(implementation_rust_files(&fuzz_root));
    }
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

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
}

fn display_path(workspace: &Path, path: &Path) -> String {
    path.strip_prefix(workspace)
        .unwrap_or(path)
        .display()
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fuzz_targets_are_scanned_as_relevant_sources() {
        assert!(is_implementation_source(
            "fuzz/fuzz_targets/cluster_schedules.rs"
        ));
        assert!(is_implementation_source("fuzz/seeds.rs"));
    }
}
