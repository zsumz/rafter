use std::{
    env, fs,
    path::{Path, PathBuf},
};

const EXTRA_FORBIDDEN_PATTERNS_ENV: &str = "RAFTER_SOURCE_BOUNDARY_EXTRA_PATTERNS";

#[derive(Clone, Copy)]
struct SourceBoundary {
    crate_name: &'static str,
    source_dir: &'static str,
    forbidden: &'static [ForbiddenSource],
}

#[derive(Clone, Copy)]
struct ForbiddenSource {
    token: &'static str,
    reason: &'static str,
}

const SOURCE_BOUNDARIES: &[SourceBoundary] = &[
    SourceBoundary {
        crate_name: "rafter",
        source_dir: "crates/rafter/src",
        forbidden: RAFTER_FORBIDDEN,
    },
    SourceBoundary {
        crate_name: "rafter-app",
        source_dir: "crates/rafter-app/src",
        forbidden: RAFTER_APP_FORBIDDEN,
    },
    SourceBoundary {
        crate_name: "rafter-runtime-api",
        source_dir: "crates/rafter-runtime-api/src",
        forbidden: RUNTIME_API_FORBIDDEN,
    },
];

const RAFTER_FORBIDDEN: &[ForbiddenSource] = &[
    ForbiddenSource {
        token: "std::fs",
        reason: "core Raft logic must stay storage-agnostic",
    },
    ForbiddenSource {
        token: "std::net",
        reason: "core Raft logic must stay transport-agnostic",
    },
    ForbiddenSource {
        token: "tokio",
        reason: "core Raft logic must stay runtime-agnostic",
    },
    ForbiddenSource {
        token: "async_trait",
        reason: "core Raft logic must stay async-runtime agnostic",
    },
    ForbiddenSource {
        token: "rafter_runtime_api",
        reason: "core Raft must not depend on runtime abstractions",
    },
    ForbiddenSource {
        token: "rafter_storage",
        reason: "core Raft must not depend on concrete storage",
    },
    ForbiddenSource {
        token: "rafter_runtime",
        reason: "core Raft must not depend on concrete runtime",
    },
    ForbiddenSource {
        token: "rafter_app",
        reason: "core Raft must not depend on the app layer",
    },
    ForbiddenSource {
        token: "rafter_service",
        reason: "core Raft must not depend on service integration",
    },
    ForbiddenSource {
        token: "rafter_multiraft",
        reason: "core Raft must not depend on multi-Raft integration",
    },
    ForbiddenSource {
        token: "rafter_transport_tcp_insecure",
        reason: "core Raft must not depend on transport examples",
    },
    ForbiddenSource {
        token: "rafter_codec",
        reason: "core Raft must not depend on wire codecs",
    },
];

const RAFTER_APP_FORBIDDEN: &[ForbiddenSource] = &[
    ForbiddenSource {
        token: "std::fs",
        reason: "app layer must not perform direct filesystem I/O",
    },
    ForbiddenSource {
        token: "std::net",
        reason: "app layer must not perform direct network I/O",
    },
    ForbiddenSource {
        token: "tokio",
        reason: "app layer should stay runtime-neutral",
    },
    ForbiddenSource {
        token: "rafter_runtime",
        reason: "app layer must use rafter-runtime-api, not concrete runtime",
    },
    ForbiddenSource {
        token: "rafter_storage",
        reason: "app layer must not depend on concrete storage",
    },
    ForbiddenSource {
        token: "rafter_service",
        reason: "app layer must not depend on service integration",
    },
    ForbiddenSource {
        token: "rafter_multiraft",
        reason: "app layer must not depend on multi-Raft integration",
    },
    ForbiddenSource {
        token: "rafter_transport_tcp_insecure",
        reason: "app layer must not depend on transport examples",
    },
];

const RUNTIME_API_FORBIDDEN: &[ForbiddenSource] = &[
    ForbiddenSource {
        token: "std::fs",
        reason: "runtime API must not perform direct filesystem I/O",
    },
    ForbiddenSource {
        token: "std::net",
        reason: "runtime API must not perform direct network I/O",
    },
    ForbiddenSource {
        token: "tokio",
        reason: "runtime API must stay runtime-neutral",
    },
    ForbiddenSource {
        token: "async_trait",
        reason: "runtime API must not force async trait machinery",
    },
    ForbiddenSource {
        token: "rafter_storage",
        reason: "runtime API must not depend on concrete storage",
    },
    ForbiddenSource {
        token: "rafter_runtime",
        reason: "runtime API must not depend on concrete runtime",
    },
    ForbiddenSource {
        token: "rafter_app",
        reason: "runtime API must not depend on the app layer",
    },
    ForbiddenSource {
        token: "rafter_service",
        reason: "runtime API must not depend on service integration",
    },
    ForbiddenSource {
        token: "rafter_multiraft",
        reason: "runtime API must not depend on multi-Raft integration",
    },
    ForbiddenSource {
        token: "rafter_transport_tcp_insecure",
        reason: "runtime API must not depend on transport examples",
    },
    ForbiddenSource {
        token: "rafter_codec",
        reason: "runtime API must not depend on wire codecs",
    },
];

#[test]
fn source_boundary_guard_enforces_lower_layer_source_rules() {
    let workspace = workspace_root();
    let extra_patterns = extra_forbidden_patterns();
    let mut violations = Vec::new();
    for boundary in SOURCE_BOUNDARIES {
        let source_root = workspace.join(boundary.source_dir);
        for path in rust_files(&source_root) {
            let source = fs::read_to_string(&path)
                .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
            for (line_index, line) in source.lines().enumerate() {
                for forbidden in boundary.forbidden {
                    if matches_forbidden_source(line, forbidden.token) {
                        violations.push(format!(
                            "{}:{}: {} source boundary forbids `{}`: {}",
                            display_path(&workspace, &path),
                            line_index + 1,
                            boundary.crate_name,
                            forbidden.token,
                            forbidden.reason
                        ));
                    }
                }
                for pattern in &extra_patterns {
                    if line.contains(pattern) {
                        violations.push(format!(
                            "{}:{}: {} source boundary forbids externally supplied pattern `{}`",
                            display_path(&workspace, &path),
                            line_index + 1,
                            boundary.crate_name,
                            pattern
                        ));
                    }
                }
            }
        }
    }

    assert!(
        violations.is_empty(),
        "source boundary violations:\n{}\n\nMove the dependency behind the appropriate crate boundary, or update the guard with an explicit reviewed exception. Set {EXTRA_FORBIDDEN_PATTERNS_ENV} to a comma, semicolon, or newline separated list to scan for private-name patterns in CI.",
        violations.join("\n")
    );
}

#[test]
fn source_boundary_extra_pattern_parser_accepts_external_lists() {
    let patterns = parse_extra_patterns(" internal_project,partner-name\nsecret_module; ");
    let expected = ["internal_project", "partner-name", "secret_module"]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();

    assert_eq!(patterns, expected);
}

fn extra_forbidden_patterns() -> Vec<String> {
    env::var(EXTRA_FORBIDDEN_PATTERNS_ENV)
        .map(|raw| parse_extra_patterns(&raw))
        .unwrap_or_default()
}

fn parse_extra_patterns(raw: &str) -> Vec<String> {
    raw.split([',', ';', '\n'])
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
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

fn matches_forbidden_source(line: &str, token: &str) -> bool {
    let trimmed = line.trim_start();
    starts_with_token_boundary(trimmed, "use ", token)
        || starts_with_token_boundary(trimmed, "pub use ", token)
        || starts_with_token_boundary(trimmed, "extern crate ", token)
        || line.contains(&format!("{token}::"))
}

fn starts_with_token_boundary(line: &str, prefix: &str, token: &str) -> bool {
    let Some(rest) = line.strip_prefix(prefix) else {
        return false;
    };
    let Some(after_token) = rest.strip_prefix(token) else {
        return false;
    };
    after_token
        .chars()
        .next()
        .is_none_or(|ch| matches!(ch, ':' | ';' | ',' | '{' | ' ' | '\t'))
}

fn display_path(workspace: &Path, path: &Path) -> String {
    path.strip_prefix(workspace)
        .unwrap_or(path)
        .display()
        .to_string()
}
