use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
};

#[test]
fn rafter_core_dependency_boundary_has_no_higher_layer_dependencies() {
    let manifest = fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
        .expect("read rafter Cargo.toml");
    let dependencies = manifest_section(&manifest, "dependencies");

    assert!(
        dependencies.trim().is_empty(),
        "rafter core normal dependencies must stay empty; found:\n{dependencies}"
    );
}

#[test]
fn workspace_dependency_boundary_matches_declared_crate_graph() {
    let workspace_root = workspace_root();
    let workspace_manifests = workspace_manifests(&workspace_root);
    let expected_workspace_crates = expected_workspace_crates();
    let mut violations = Vec::new();

    let actual_workspace_crates = workspace_manifests.keys().cloned().collect::<BTreeSet<_>>();
    if actual_workspace_crates != expected_workspace_crates {
        violations.push(format!(
            "workspace crate set changed; update the dependency DAG policy\nexpected: {expected_workspace_crates:?}\nactual:   {actual_workspace_crates:?}",
        ));
    }

    for (crate_name, manifest_path) in &workspace_manifests {
        let manifest = fs::read_to_string(manifest_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", manifest_path.display()));
        let dependencies = parse_dependencies(&manifest);
        let normal_workspace_deps = workspace_deps(&dependencies.normal, &actual_workspace_crates);
        let dev_workspace_deps = workspace_deps(&dependencies.dev, &actual_workspace_crates);
        let build_workspace_deps = workspace_deps(&dependencies.build, &actual_workspace_crates);
        let allowed_normal_deps = allowed_normal_workspace_deps(crate_name);
        let allowed_dev_deps = allowed_dev_workspace_deps(crate_name);
        let allowed_dev_dep_names = allowed_dev_deps.keys().cloned().collect();

        collect_set_delta(
            &mut violations,
            crate_name,
            "normal workspace dependencies",
            &allowed_normal_deps,
            &normal_workspace_deps,
        );

        for dep in dev_workspace_deps.difference(&allowed_dev_dep_names) {
            violations.push(format!(
                "{crate_name} has undocumented dev workspace dependency `{dep}`; add a reason to allowed_dev_workspace_deps"
            ));
        }
        for (dep, reason) in &allowed_dev_deps {
            if !dev_workspace_deps.contains(dep) {
                violations.push(format!(
                    "{crate_name} has stale dev-dependency exception `{dep}` ({reason})"
                ));
            }
        }

        if !build_workspace_deps.is_empty() {
            violations.push(format!(
                "{crate_name} has workspace build-dependencies {build_workspace_deps:?}; document them before adding build-time crate edges",
            ));
        }
    }

    assert!(
        violations.is_empty(),
        "workspace dependency boundary violations:\n\n{}",
        violations.join("\n\n")
    );
}

fn manifest_section(manifest: &str, section: &str) -> String {
    let header = format!("[{section}]");
    let Some(start) = manifest.lines().position(|line| line.trim() == header) else {
        return String::new();
    };
    manifest
        .lines()
        .skip(start + 1)
        .take_while(|line| !line.trim_start().starts_with('['))
        .collect::<Vec<_>>()
        .join("\n")
}

#[derive(Debug, Default)]
struct DependencySections {
    normal: BTreeSet<String>,
    dev: BTreeSet<String>,
    build: BTreeSet<String>,
}

#[derive(Clone, Copy, Debug)]
enum DependencyKind {
    Normal,
    Dev,
    Build,
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under crates/ in the workspace")
        .to_path_buf()
}

fn workspace_manifests(workspace_root: &Path) -> BTreeMap<String, PathBuf> {
    let root_manifest_path = workspace_root.join("Cargo.toml");
    let root_manifest = fs::read_to_string(&root_manifest_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", root_manifest_path.display()));
    let workspace_section = manifest_section(&root_manifest, "workspace");
    let mut manifests = BTreeMap::new();

    for member in workspace_members(&workspace_section) {
        let manifest_path = workspace_root.join(&member).join("Cargo.toml");
        let manifest = fs::read_to_string(&manifest_path)
            .unwrap_or_else(|error| panic!("read {}: {error}", manifest_path.display()));
        let crate_name = package_name(&manifest);
        let previous = manifests.insert(crate_name.clone(), manifest_path);
        assert!(
            previous.is_none(),
            "duplicate package name `{crate_name}` in workspace members"
        );
    }

    assert!(
        !manifests.is_empty(),
        "workspace manifest should declare crate members"
    );
    manifests
}

fn workspace_members(workspace_section: &str) -> Vec<String> {
    let mut members = Vec::new();
    let mut in_members = false;

    for raw_line in workspace_section.lines() {
        let line = strip_comment(raw_line).trim();
        if line.starts_with("members") {
            in_members = true;
        }
        if in_members {
            if let Some(member) = quoted_value(line) {
                members.push(member);
            }
            if line.ends_with(']') {
                break;
            }
        }
    }

    members
}

fn package_name(manifest: &str) -> String {
    let package_section = manifest_section(manifest, "package");
    package_section
        .lines()
        .find_map(|line| {
            let line = strip_comment(line).trim();
            let (key, value) = line.split_once('=')?;
            (key.trim() == "name").then(|| unquote(value.trim()))
        })
        .expect("workspace crate manifest should include package.name")
}

fn parse_dependencies(manifest: &str) -> DependencySections {
    let mut dependencies = DependencySections::default();
    let mut current_kind = None;

    for raw_line in manifest.lines() {
        let line = strip_comment(raw_line).trim();
        if line.starts_with('[') && line.ends_with(']') {
            current_kind = dependency_kind(line);
            continue;
        }

        let Some(kind) = current_kind else {
            continue;
        };
        let Some(dep) = dependency_key(line) else {
            continue;
        };

        match kind {
            DependencyKind::Normal => {
                dependencies.normal.insert(dep);
            }
            DependencyKind::Dev => {
                dependencies.dev.insert(dep);
            }
            DependencyKind::Build => {
                dependencies.build.insert(dep);
            }
        }
    }

    dependencies
}

fn dependency_kind(header: &str) -> Option<DependencyKind> {
    let section = header.trim_start_matches('[').trim_end_matches(']');
    match section {
        "dependencies" => Some(DependencyKind::Normal),
        "dev-dependencies" => Some(DependencyKind::Dev),
        "build-dependencies" => Some(DependencyKind::Build),
        _ if section.ends_with(".dependencies") => Some(DependencyKind::Normal),
        _ if section.ends_with(".dev-dependencies") => Some(DependencyKind::Dev),
        _ if section.ends_with(".build-dependencies") => Some(DependencyKind::Build),
        _ => None,
    }
}

fn dependency_key(line: &str) -> Option<String> {
    let (key, _) = line.split_once('=')?;
    let key = key.trim().trim_matches('"');
    (!key.is_empty()).then(|| key.to_owned())
}

fn strip_comment(line: &str) -> &str {
    line.split_once('#')
        .map_or(line, |(before_comment, _)| before_comment)
}

fn quoted_value(line: &str) -> Option<String> {
    let quote_start = line.find('"')?;
    let after_quote_start = &line[quote_start + 1..];
    let quote_end = after_quote_start.find('"')?;
    Some(after_quote_start[..quote_end].to_owned())
}

fn unquote(value: &str) -> String {
    value.trim_matches('"').to_owned()
}

fn expected_workspace_crates() -> BTreeSet<String> {
    set(&[
        "rafter",
        "rafter-app",
        "rafter-codec",
        "rafter-crc32",
        "rafter-invariant-test",
        "rafter-invariant-test-macros",
        "rafter-invariants",
        "rafter-maelstrom",
        "rafter-multiraft",
        "rafter-runtime",
        "rafter-runtime-api",
        "rafter-service",
        "rafter-sim",
        "rafter-storage",
        "rafter-transport-tcp-insecure",
    ])
}

fn allowed_normal_workspace_deps(crate_name: &str) -> BTreeSet<String> {
    set(match crate_name {
        "rafter" | "rafter-crc32" | "rafter-invariant-test-macros" | "rafter-invariants" => &[],
        "rafter-invariant-test" => &["rafter-invariant-test-macros"],
        "rafter-app" => &["rafter", "rafter-runtime-api"],
        "rafter-codec" | "rafter-storage" => &["rafter", "rafter-crc32"],
        "rafter-runtime-api" | "rafter-sim" => &["rafter"],
        "rafter-maelstrom" => &["rafter", "rafter-codec", "rafter-runtime", "rafter-storage"],
        "rafter-multiraft" => &["rafter-app", "rafter-runtime-api"],
        "rafter-runtime" => &["rafter", "rafter-runtime-api", "rafter-storage"],
        "rafter-service" => &["rafter", "rafter-app", "rafter-runtime-api"],
        "rafter-transport-tcp-insecure" => &["rafter", "rafter-codec"],
        unexpected => panic!("missing normal dependency policy for `{unexpected}`"),
    })
}

fn allowed_dev_workspace_deps(crate_name: &str) -> BTreeMap<String, &'static str> {
    let entries: &[(&str, &str)] = match crate_name {
        "rafter-app" => &[
            (
                "rafter-invariant-test",
                "registered app tests emit typed invariant-oracle verdicts",
            ),
            (
                "rafter-runtime",
                "app tests/examples may instantiate DurableRaftNode without making app depend on it",
            ),
            (
                "rafter-storage",
                "app tests/examples may use stores without making app depend on storage",
            ),
        ],
        "rafter-multiraft" => &[
            (
                "rafter",
                "multiraft tests may inspect core IDs and reports without widening normal deps",
            ),
            (
                "rafter-runtime",
                "multiraft tests may instantiate durable runtimes without widening normal deps",
            ),
            (
                "rafter-storage",
                "multiraft tests may use stores without widening normal deps",
            ),
        ],
        "rafter-runtime" => &[
            (
                "rafter-invariant-test",
                "registered runtime tests emit typed invariant-oracle verdicts",
            ),
            (
                "rafter-transport-tcp-insecure",
                "runtime examples may use the demo TCP transport without making runtime depend on transport",
            ),
        ],
        "rafter-invariants" => &[
            (
                "rafter",
                "validator integration tests construct canonical node configs without widening verifier runtime dependencies",
            ),
            (
                "rafter-sim",
                "validator integration tests consume actual simulator liveness JSON without widening verifier runtime dependencies",
            ),
        ],
        "rafter-service" => &[
            (
                "rafter-runtime",
                "service tests/examples may instantiate durable runtimes without making service depend on runtime",
            ),
            (
                "rafter-storage",
                "service tests/examples may use stores without making service depend on storage",
            ),
        ],
        "rafter" => &[(
            "rafter-invariant-test",
            "registered core tests emit typed invariant-oracle verdicts",
        )],
        "rafter-maelstrom" => &[(
            "rafter-invariant-test",
            "registered Maelstrom tests emit typed invariant-oracle verdicts",
        )],
        "rafter-sim" => &[(
            "rafter-invariant-test",
            "registered simulator tests emit typed invariant-oracle verdicts",
        )],
        "rafter-storage" => &[(
            "rafter-invariant-test",
            "registered storage tests emit typed invariant-oracle verdicts",
        )],
        "rafter-codec"
        | "rafter-crc32"
        | "rafter-invariant-test"
        | "rafter-invariant-test-macros"
        | "rafter-runtime-api"
        | "rafter-transport-tcp-insecure" => &[],
        unexpected => panic!("missing dev dependency policy for `{unexpected}`"),
    };

    entries
        .iter()
        .map(|(dep, reason)| ((*dep).to_owned(), *reason))
        .collect()
}

fn workspace_deps(
    deps: &BTreeSet<String>,
    workspace_crates: &BTreeSet<String>,
) -> BTreeSet<String> {
    deps.intersection(workspace_crates).cloned().collect()
}

fn collect_set_delta(
    violations: &mut Vec<String>,
    crate_name: &str,
    label: &str,
    expected: &BTreeSet<String>,
    actual: &BTreeSet<String>,
) {
    for dep in actual.difference(expected) {
        violations.push(format!(
            "{crate_name} has unexpected {label} entry `{dep}`; allowed: {expected:?}"
        ));
    }
    for dep in expected.difference(actual) {
        violations.push(format!(
            "{crate_name} is missing expected {label} entry `{dep}`; actual: {actual:?}"
        ));
    }
}

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|item| (*item).to_owned()).collect()
}
