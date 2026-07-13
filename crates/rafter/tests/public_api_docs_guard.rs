use std::{
    collections::BTreeMap,
    fmt::Write as _,
    fs,
    path::{Path, PathBuf},
};

const MAX_WARNINGS_TO_PRINT: usize = 200;
const TRACKED_TODO_MARKERS: &[&str] = &["TODO(tracked)", "FIXME(tracked)"];

const SIM_INVARIANT_LABEL: &str = "sim-harness-invariant";
const SIM_CLUSTER_INVARIANT: &str =
    "rafter-sim cluster harness invariant; invalid node ids or impossible staging states should fail the verification run loudly";
const SIM_BOOTSTRAP_INVARIANT: &str =
    "static rafter-sim bootstrap fixture is expected to satisfy core bootstrap validation";
const SIM_LITERAL_INVARIANT: &str =
    "static rafter-sim literal fixture is expected to satisfy typed constructor validation";
const SIM_MODEL_CHECK_INVARIANT: &str =
    "bounded model-check setup invariant; impossible setup failure should fail the verification run loudly";
const MAELSTROM_INVARIANT_LABEL: &str = "maelstrom-harness-invariant";
const MAELSTROM_SERIALIZATION_INVARIANT: &str =
    "Maelstrom harness payloads are fixed internal values and should fail loudly if they stop serializing";
const MAELSTROM_PROTOCOL_INVARIANT: &str =
    "Maelstrom harness protocol bookkeeping is internally bounded and should fail loudly on impossible state";
const MAELSTROM_LITERAL_INVARIANT: &str =
    "static Maelstrom snapshot fixture is expected to satisfy typed constructor validation";

const WARNING_ALLOWLIST: &[WarningAllow] = &[
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-maelstrom/src/app.rs",
        symbol: None,
        text: Some(r#".expect("JSON value serializes")"#),
        classification_label: MAELSTROM_INVARIANT_LABEL,
        reason: MAELSTROM_SERIALIZATION_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-maelstrom/src/client.rs",
        symbol: None,
        text: Some(r#".expect("command serializes")"#),
        classification_label: MAELSTROM_INVARIANT_LABEL,
        reason: MAELSTROM_SERIALIZATION_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-maelstrom/src/client.rs",
        symbol: None,
        text: Some(r#".expect("pending read exists")"#),
        classification_label: MAELSTROM_INVARIANT_LABEL,
        reason: MAELSTROM_PROTOCOL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-maelstrom/src/protocol.rs",
        symbol: None,
        text: Some(r#".expect("node count fits u64")"#),
        classification_label: MAELSTROM_INVARIANT_LABEL,
        reason: MAELSTROM_PROTOCOL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-maelstrom/src/raft/snapshots.rs",
        symbol: None,
        text: Some(r#"SnapshotGroupId::new(SNAPSHOT_GROUP_ID).expect("valid snapshot group id")"#),
        classification_label: MAELSTROM_INVARIANT_LABEL,
        reason: MAELSTROM_LITERAL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-maelstrom/src/raft/snapshots.rs",
        symbol: None,
        text: Some(r#"ApplicationSnapshotKind::new(SNAPSHOT_KIND).expect("valid snapshot kind")"#),
        classification_label: MAELSTROM_INVARIANT_LABEL,
        reason: MAELSTROM_LITERAL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-maelstrom/src/raft/snapshots.rs",
        symbol: None,
        text: Some(r#"ApplicationSnapshotVersion::new(1).expect("valid snapshot version")"#),
        classification_label: MAELSTROM_INVARIANT_LABEL,
        reason: MAELSTROM_LITERAL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-maelstrom/src/raft_node.rs",
        symbol: None,
        text: Some(r#".expect("snapshot read chunk fits u32")"#),
        classification_label: MAELSTROM_INVARIANT_LABEL,
        reason: MAELSTROM_PROTOCOL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/restart.rs",
        symbol: None,
        text: Some(r#".expect("simulated node config must exist in cluster")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_CLUSTER_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/restart.rs",
        symbol: None,
        text: Some(r#".expect("a synced mark must be captured before a marked restart")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_CLUSTER_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/restart.rs",
        symbol: None,
        text: Some(r#".expect("marked lossy restart composes a valid bootstrap state")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/restart.rs",
        symbol: None,
        text: Some(r#".expect("floor-truncated lossy restart composes a valid bootstrap state")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/snapshot.rs",
        symbol: None,
        text: Some(r#".expect("seeded snapshot payload must match its descriptor length")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_CLUSTER_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryPanic,
        path: "crates/rafter-sim/src/snapshot.rs",
        symbol: None,
        text: Some("staged a chunk of transfer"),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_CLUSTER_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryPanic,
        path: "crates/rafter-sim/src/snapshot.rs",
        symbol: None,
        text: Some("applied snapshot transfer"),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_CLUSTER_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/snapshot.rs",
        symbol: None,
        text: Some(
            r#".expect("completed staged payload length was validated against the descriptor")"#,
        ),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_CLUSTER_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/inspection.rs",
        symbol: None,
        text: Some(r#".expect("simulated node must exist in cluster")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_CLUSTER_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/application/soak.rs",
        symbol: None,
        text: Some(r#".expect("soak restart from captured durable state must be valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/helpers.rs",
        symbol: None,
        text: Some(r#".expect("model-check Raft node config must be valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_MODEL_CHECK_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/helpers.rs",
        symbol: None,
        text: Some(r#".expect("model-check non-voter config must be valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_MODEL_CHECK_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/helpers.rs",
        symbol: None,
        text: Some(r#".expect("production model-check Raft node config must be valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_MODEL_CHECK_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryPanic,
        path: "crates/rafter-sim/src/model_check/helpers.rs",
        symbol: None,
        text: Some("node-1 did not become leader within the model-check election budget"),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_MODEL_CHECK_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryPanic,
        path: "crates/rafter-sim/src/model_check/helpers.rs",
        symbol: None,
        text: Some("expected one ready message to deliver"),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_MODEL_CHECK_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/helpers.rs",
        symbol: None,
        text: Some(r#"SnapshotGroupId::new("sim-data-group").expect("valid snapshot group id")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_LITERAL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/helpers.rs",
        symbol: None,
        text: Some(r#"ApplicationSnapshotKind::new("stream_data").expect("valid snapshot kind")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_LITERAL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/helpers.rs",
        symbol: None,
        text: Some(r#"ApplicationSnapshotVersion::new(1).expect("valid snapshot version")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_LITERAL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/helpers.rs",
        symbol: None,
        text: Some(r#".expect("valid snapshot metadata")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_LITERAL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/liveness/features/snapshot.rs",
        symbol: None,
        text: Some(r#".expect("snapshot liveness fixture declares expected snapshot")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_MODEL_CHECK_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/scheduling/membership.rs",
        symbol: None,
        text: Some(r#".expect("enabled membership action keeps at least one voter")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_MODEL_CHECK_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/logical_log.rs",
        symbol: None,
        text: Some(r#".expect("observed node must exist")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_CLUSTER_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/seeds.rs",
        symbol: None,
        text: Some(r#".expect("pre-committed follower seed is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/seeds.rs",
        symbol: None,
        text: Some(r#".expect("committed leader-side seed is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/seeds.rs",
        symbol: None,
        text: Some(r#".expect("pre-diverged follower seed is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/seeds.rs",
        symbol: None,
        text: Some(r#".expect("single-voter prior application seed is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/seeds.rs",
        symbol: None,
        text: Some(
            r#"MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("membership is valid")"#,
        ),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_LITERAL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/seeds.rs",
        symbol: None,
        text: Some(r#".expect("single-voter prior configuration seed is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/seeds.rs",
        symbol: None,
        text: Some(r#".expect("joint self-quorum prior application seed is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/restart_snapshot.rs",
        symbol: None,
        text: Some(r#".expect("old snapshot membership is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_LITERAL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/restart_snapshot.rs",
        symbol: None,
        text: Some(r#".expect("new snapshot membership is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_LITERAL_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/restart_snapshot.rs",
        symbol: None,
        text: Some(r#".expect("visible leader bootstrap is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/restart_snapshot.rs",
        symbol: None,
        text: Some(r#".expect("divergent follower bootstrap is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/restart_snapshot.rs",
        symbol: None,
        text: Some(r#".expect("visible voter bootstrap is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
    WarningAllow {
        kind: WarningKind::LibraryExpect,
        path: "crates/rafter-sim/src/model_check/state/restart_snapshot.rs",
        symbol: None,
        text: Some(r#".expect("compacted voter bootstrap is valid")"#),
        classification_label: SIM_INVARIANT_LABEL,
        reason: SIM_BOOTSTRAP_INVARIANT,
    },
];

#[derive(Clone, Copy)]
struct WarningAllow {
    kind: WarningKind,
    path: &'static str,
    symbol: Option<&'static str>,
    text: Option<&'static str>,
    classification_label: &'static str,
    reason: &'static str,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WarningKind {
    MissingRustdoc,
    UndocumentedExhaustiveEnum,
    LibraryUnwrap,
    LibraryExpect,
    LibraryPanic,
    UntrackedTodoFixmeRustdoc,
    AllowlistEntryIncomplete,
}

impl WarningKind {
    const fn label(self) -> &'static str {
        match self {
            Self::MissingRustdoc => "missing-rustdoc",
            Self::UndocumentedExhaustiveEnum => "undocumented-exhaustive-enum",
            Self::LibraryUnwrap => "library-unwrap",
            Self::LibraryExpect => "library-expect",
            Self::LibraryPanic => "library-panic",
            Self::UntrackedTodoFixmeRustdoc => "untracked-todo-fixme-rustdoc",
            Self::AllowlistEntryIncomplete => "allowlist-entry-incomplete",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PublicItemKind {
    Const,
    Enum,
    Function,
    Module,
    Static,
    Struct,
    Trait,
    Type,
}

impl PublicItemKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Const => "const",
            Self::Enum => "enum",
            Self::Function => "function",
            Self::Module => "module",
            Self::Static => "static",
            Self::Struct => "struct",
            Self::Trait => "trait",
            Self::Type => "type",
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
struct PublicItem {
    kind: PublicItemKind,
    symbol: String,
}

#[derive(Debug, Eq, PartialEq)]
struct GuardWarning {
    kind: WarningKind,
    path: String,
    line: usize,
    symbol: Option<String>,
    message: String,
    source_line: String,
}

#[test]
fn public_api_docs_guard_rejects_unallowlisted_warnings() {
    let workspace = workspace_root();
    let warnings = collect_guard_warnings(&workspace);
    let allowed = warnings
        .iter()
        .filter(|warning| warning_allowed(warning))
        .collect::<Vec<_>>();
    let visible = warnings
        .iter()
        .filter(|warning| !warning_allowed(warning))
        .collect::<Vec<_>>();

    if !allowed.is_empty() {
        eprintln!(
            "public API/docs guard allowlisted warnings (0 unallowlisted, {} allowlisted):\n{}\n{}",
            allowed.len(),
            render_warning_summary(&allowed),
            render_warnings(&allowed)
        );
    }

    assert!(
        visible.is_empty(),
        "public API/docs guard found {} unallowlisted warnings ({} allowlisted):\n{}\n{}",
        visible.len(),
        allowed.len(),
        render_warning_summary(&visible),
        render_warnings(&visible)
    );
}

#[test]
fn public_api_docs_guard_allowlist_entries_are_actionable() {
    let warnings = allowlist_warnings();

    assert!(
        warnings.is_empty(),
        "public API/docs guard allowlist entries must include a reason and classification label:\n{}",
        render_warnings(&warnings.iter().collect::<Vec<_>>())
    );
}

#[test]
fn public_api_docs_guard_allowlist_entries_are_used() {
    let workspace = workspace_root();
    let warnings = collect_guard_warnings(&workspace);
    let unused = WARNING_ALLOWLIST
        .iter()
        .filter(|allow| {
            !warnings
                .iter()
                .any(|warning| warning_matches_allow(warning, allow))
        })
        .collect::<Vec<_>>();

    assert!(
        unused.is_empty(),
        "public API/docs guard allowlist entries no longer match current warnings:\n{}",
        render_allowlist_entries(&unused)
    );
}

#[test]
fn public_api_docs_guard_rejects_untracked_todo_fixme_in_rustdoc() {
    let workspace = workspace_root();
    let warnings = collect_guard_warnings(&workspace)
        .into_iter()
        .filter(|warning| warning.kind == WarningKind::UntrackedTodoFixmeRustdoc)
        .collect::<Vec<_>>();

    assert!(
        warnings.is_empty(),
        "public rustdoc TODO/FIXME notes need tracking labels:\n{}",
        render_warnings(&warnings.iter().collect::<Vec<_>>())
    );
}

#[test]
fn public_api_docs_guard_detects_missing_docs_and_enum_policy() {
    let source = r"
pub struct MissingDocs;

/// Deliberately exhaustive for compatibility.
pub enum DocumentedExhaustive {
    Value,
}

#[non_exhaustive]
/// This enum is intentionally open-ended.
pub enum NonExhaustive {
    Value,
}
";

    let warnings = public_item_warnings("crates/demo/src/lib.rs", source);
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].kind, WarningKind::MissingRustdoc);
    assert_eq!(warnings[0].symbol.as_deref(), Some("MissingDocs"));
}

#[test]
fn public_api_docs_guard_warns_on_untracked_todo_in_rustdoc() {
    let source = r"
/// TODO: decide whether this is public.
pub struct MissingTrackingLabel;

/// TODO(tracked): decide whether this is public.
pub struct TrackedFollowUp;
";

    let warnings = public_item_warnings("crates/demo/src/lib.rs", source);
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].kind, WarningKind::UntrackedTodoFixmeRustdoc);
    assert_eq!(warnings[0].symbol.as_deref(), Some("MissingTrackingLabel"));
}

#[test]
fn public_api_docs_guard_ignores_cfg_test_modules_for_risky_macros() {
    let source = r#"
fn production() {
    value.expect("library invariant");
}

#[cfg(test)]
mod tests {
    #[test]
    fn unit() {
        value.expect("test fixture");
        panic!("test failure");
    }
}
"#;

    let warnings = line_risk_warnings("crates/demo/src/lib.rs", source);
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].kind, WarningKind::LibraryExpect);
}

#[test]
fn public_api_docs_guard_excludes_test_support_files_from_library_risk_scan() {
    assert!(!is_library_rust_file(Path::new(
        "crates/demo/src/test_support.rs"
    )));
    assert!(!is_library_rust_file(Path::new(
        "crates/demo/src/nested/test_support.rs"
    )));
    assert!(!is_library_rust_file(Path::new(
        "crates/demo/src/component_tests.rs"
    )));
    assert!(!is_library_rust_file(Path::new(
        "crates/demo/src/component_tests/fixture.rs"
    )));
    assert!(is_library_rust_file(Path::new("crates/demo/src/lib.rs")));
}

fn collect_guard_warnings(workspace: &Path) -> Vec<GuardWarning> {
    let mut warnings = allowlist_warnings();
    for path in library_rust_files(&workspace.join("crates")) {
        let relative_path = display_path(workspace, &path);
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
        warnings.extend(public_item_warnings(&relative_path, &source));
        warnings.extend(line_risk_warnings(&relative_path, &source));
    }
    warnings.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then(left.line.cmp(&right.line))
            .then(left.kind.label().cmp(right.kind.label()))
    });
    warnings
}

fn allowlist_warnings() -> Vec<GuardWarning> {
    WARNING_ALLOWLIST
        .iter()
        .filter(|allow| {
            allow.reason.trim().is_empty() || allow.classification_label.trim().is_empty()
        })
        .map(|allow| GuardWarning {
            kind: WarningKind::AllowlistEntryIncomplete,
            path: allow.path.to_owned(),
            line: 1,
            symbol: allow.symbol.map(ToOwned::to_owned),
            message:
                "public API/docs guard allowlist entry needs a reason and classification label"
                    .to_owned(),
            source_line: allow.text.unwrap_or("").to_owned(),
        })
        .collect()
}

fn public_item_warnings(path: &str, source: &str) -> Vec<GuardWarning> {
    let lines = source.lines().collect::<Vec<_>>();
    let mut warnings = Vec::new();
    let mut cfg_test = CfgTestScanner::default();

    for (index, line) in lines.iter().enumerate() {
        if !cfg_test.in_cfg_test() {
            collect_public_item_line_warnings(path, &lines, index, &mut warnings);
        }
        cfg_test.advance(line);
    }

    warnings
}

fn collect_public_item_line_warnings(
    path: &str,
    lines: &[&str],
    index: usize,
    warnings: &mut Vec<GuardWarning>,
) {
    let Some(item) = parse_public_item(lines[index]) else {
        return;
    };
    let docs = rustdoc_before(lines, index);
    let attrs = attrs_before(lines, index);

    if is_doc_hidden(&attrs) {
        return;
    }

    if docs.is_empty() {
        warnings.push(warning(
            WarningKind::MissingRustdoc,
            path,
            index,
            Some(&item.symbol),
            format!(
                "public {} `{}` has no rustdoc",
                item.kind.label(),
                item.symbol
            ),
            lines[index],
        ));
    }

    if item.kind == PublicItemKind::Enum
        && !has_non_exhaustive_attr(&attrs)
        && !docs.iter().any(|doc| mentions_exhaustive(doc))
    {
        warnings.push(warning(
            WarningKind::UndocumentedExhaustiveEnum,
            path,
            index,
            Some(&item.symbol),
            format!(
                "public enum `{}` is neither #[non_exhaustive] nor documented as exhaustive",
                item.symbol
            ),
            lines[index],
        ));
    }

    for doc in docs {
        if contains_todo_or_fixme(&doc) && !contains_tracking_label(&doc) {
            warnings.push(warning(
                WarningKind::UntrackedTodoFixmeRustdoc,
                path,
                index,
                Some(&item.symbol),
                format!(
                    "public rustdoc for `{}` contains TODO/FIXME without an tracking label",
                    item.symbol
                ),
                &doc,
            ));
        }
    }
}

fn line_risk_warnings(path: &str, source: &str) -> Vec<GuardWarning> {
    let lines = source.lines().collect::<Vec<_>>();
    let mut warnings = Vec::new();
    let mut cfg_test = CfgTestScanner::default();

    for (index, line) in lines.iter().enumerate() {
        if !cfg_test.in_cfg_test() {
            collect_line_risk_warning(path, index, &lines, &mut warnings);
        }
        cfg_test.advance(line);
    }

    warnings
}

fn collect_line_risk_warning(
    path: &str,
    index: usize,
    lines: &[&str],
    warnings: &mut Vec<GuardWarning>,
) {
    let line = lines[index];
    let trimmed = line.trim_start();
    if trimmed.starts_with("//") {
        return;
    }

    let risks = [
        (".unwrap()", WarningKind::LibraryUnwrap, "uses `.unwrap()`"),
        (
            ".expect(",
            WarningKind::LibraryExpect,
            "uses `.expect(...)`",
        ),
        ("panic!", WarningKind::LibraryPanic, "uses `panic!`"),
    ];

    for (needle, kind, description) in risks {
        if line.contains(needle) {
            let source_line = risk_source_line(lines, index, needle);
            warnings.push(warning(
                kind,
                path,
                index,
                None,
                description.to_owned(),
                &source_line,
            ));
        }
    }
}

fn risk_source_line(lines: &[&str], index: usize, needle: &str) -> String {
    let trimmed = lines[index].trim();
    if needle == "panic!" && trimmed == "panic!(" {
        if let Some(next) = lines
            .iter()
            .skip(index + 1)
            .map(|line| line.trim())
            .find(|line| !line.is_empty())
        {
            return format!("{trimmed} {next}");
        }
    }
    trimmed.to_owned()
}

fn warning(
    kind: WarningKind,
    path: &str,
    zero_based_line: usize,
    symbol: Option<&str>,
    message: String,
    source_line: &str,
) -> GuardWarning {
    GuardWarning {
        kind,
        path: path.to_owned(),
        line: zero_based_line + 1,
        symbol: symbol.map(ToOwned::to_owned),
        message,
        source_line: source_line.trim().to_owned(),
    }
}

fn parse_public_item(line: &str) -> Option<PublicItem> {
    let rest = line.trim_start().strip_prefix("pub ")?;
    if rest.starts_with("use ") {
        return None;
    }

    let candidates = [
        ("const fn ", PublicItemKind::Function),
        ("async fn ", PublicItemKind::Function),
        ("unsafe fn ", PublicItemKind::Function),
        ("unsafe const fn ", PublicItemKind::Function),
        ("fn ", PublicItemKind::Function),
        ("struct ", PublicItemKind::Struct),
        ("enum ", PublicItemKind::Enum),
        ("trait ", PublicItemKind::Trait),
        ("type ", PublicItemKind::Type),
        ("const ", PublicItemKind::Const),
        ("static ", PublicItemKind::Static),
        ("mod ", PublicItemKind::Module),
    ];

    candidates.iter().find_map(|(prefix, kind)| {
        rest.strip_prefix(prefix).map(|after_prefix| PublicItem {
            kind: *kind,
            symbol: parse_symbol(after_prefix),
        })
    })
}

fn parse_symbol(text: &str) -> String {
    text.trim_start()
        .split(|ch: char| ch.is_whitespace() || matches!(ch, '(' | '<' | ':' | '{' | ';' | '='))
        .next()
        .filter(|symbol| !symbol.is_empty())
        .unwrap_or("<unknown>")
        .to_owned()
}

fn rustdoc_before(lines: &[&str], index: usize) -> Vec<String> {
    nearby_attribute_lines(lines, index)
        .into_iter()
        .filter(|line| is_rustdoc_line(line))
        .collect()
}

fn attrs_before(lines: &[&str], index: usize) -> Vec<String> {
    nearby_attribute_lines(lines, index)
        .into_iter()
        .filter(|line| line.trim_start().starts_with("#["))
        .collect()
}

fn nearby_attribute_lines(lines: &[&str], index: usize) -> Vec<String> {
    let mut result = Vec::new();
    let mut cursor = index;
    while cursor > 0 {
        cursor -= 1;
        let trimmed = lines[cursor].trim_start();
        if trimmed.starts_with("#[") || is_rustdoc_line(trimmed) {
            result.push(trimmed.to_owned());
            continue;
        }
        if trimmed.is_empty() && result.is_empty() {
            continue;
        }
        break;
    }
    result.reverse();
    result
}

fn is_rustdoc_line(line: &str) -> bool {
    let trimmed = line.trim_start();
    trimmed.starts_with("///") || trimmed.starts_with("#[doc")
}

fn is_doc_hidden(attrs: &[String]) -> bool {
    attrs.iter().any(|attr| attr.contains("doc(hidden)"))
}

fn has_non_exhaustive_attr(attrs: &[String]) -> bool {
    attrs.iter().any(|attr| attr.contains("non_exhaustive"))
}

fn mentions_exhaustive(doc: &str) -> bool {
    doc.to_ascii_lowercase().contains("exhaustive")
}

fn contains_todo_or_fixme(doc: &str) -> bool {
    doc.contains("TODO") || doc.contains("FIXME")
}

fn contains_tracking_label(doc: &str) -> bool {
    TRACKED_TODO_MARKERS
        .iter()
        .any(|marker| doc.contains(marker))
}

#[derive(Default)]
struct CfgTestScanner {
    brace_depth: usize,
    pending_cfg_test: bool,
    test_module_depth: Option<usize>,
}

impl CfgTestScanner {
    const fn in_cfg_test(&self) -> bool {
        self.test_module_depth.is_some()
    }

    fn advance(&mut self, line: &str) {
        let trimmed = line.trim_start();
        if self.pending_cfg_test && is_module_block_start(trimmed) {
            self.test_module_depth = Some(self.brace_depth.saturating_add(1));
            self.pending_cfg_test = false;
        } else if self.pending_cfg_test && !trimmed.starts_with("#[") && !trimmed.is_empty() {
            self.pending_cfg_test = false;
        }

        if is_cfg_test_attr(trimmed) {
            self.pending_cfg_test = true;
        }

        self.brace_depth = next_brace_depth(self.brace_depth, line);
        if self
            .test_module_depth
            .is_some_and(|depth| self.brace_depth < depth)
        {
            self.test_module_depth = None;
        }
    }
}

fn is_cfg_test_attr(line: &str) -> bool {
    line.starts_with("#[cfg(test)]")
}

fn is_module_block_start(line: &str) -> bool {
    (line.starts_with("mod ") || line.starts_with("pub mod ")) && line.contains('{')
}

fn next_brace_depth(current: usize, line: &str) -> usize {
    let opens = line.bytes().filter(|byte| *byte == b'{').count();
    let closes = line.bytes().filter(|byte| *byte == b'}').count();
    current.saturating_add(opens).saturating_sub(closes)
}

fn warning_allowed(warning: &GuardWarning) -> bool {
    WARNING_ALLOWLIST
        .iter()
        .any(|allow| warning_matches_allow(warning, allow))
}

fn warning_matches_allow(warning: &GuardWarning, allow: &WarningAllow) -> bool {
    allow.kind == warning.kind
        && allow.path == warning.path
        && allow
            .symbol
            .is_none_or(|symbol| warning.symbol.as_deref() == Some(symbol))
        && allow
            .text
            .is_none_or(|text| warning.source_line.contains(text))
}

fn render_warnings(warnings: &[&GuardWarning]) -> String {
    let mut output = String::new();
    for warning in warnings.iter().take(MAX_WARNINGS_TO_PRINT) {
        writeln!(
            &mut output,
            "{}:{}: {}: {}\n    {}",
            warning.path,
            warning.line,
            warning.kind.label(),
            warning.message,
            warning.source_line
        )
        .expect("write to string");
    }

    if warnings.len() > MAX_WARNINGS_TO_PRINT {
        writeln!(
            &mut output,
            "... {} more warnings omitted",
            warnings.len() - MAX_WARNINGS_TO_PRINT
        )
        .expect("write to string");
    }
    output
}

fn render_allowlist_entries(entries: &[&WarningAllow]) -> String {
    let mut output = String::new();
    for entry in entries {
        writeln!(
            &mut output,
            "{}: {}: text={:?}, symbol={:?}, classification_label={}",
            entry.path,
            entry.kind.label(),
            entry.text,
            entry.symbol,
            entry.classification_label
        )
        .expect("write to string");
    }
    output
}

fn render_warning_summary(warnings: &[&GuardWarning]) -> String {
    let mut by_kind = BTreeMap::<&'static str, usize>::new();
    let mut by_crate = BTreeMap::<String, usize>::new();
    let mut by_crate_and_kind = BTreeMap::<(String, &'static str), usize>::new();

    for warning in warnings {
        let crate_name = crate_name_from_path(&warning.path);
        let kind = warning.kind.label();
        *by_kind.entry(kind).or_default() += 1;
        *by_crate.entry(crate_name.clone()).or_default() += 1;
        *by_crate_and_kind.entry((crate_name, kind)).or_default() += 1;
    }

    let mut output = String::from("summary by kind:\n");
    for (kind, count) in by_kind {
        writeln!(&mut output, "  {kind}: {count}").expect("write to string");
    }

    output.push_str("summary by crate:\n");
    for (crate_name, count) in &by_crate {
        writeln!(&mut output, "  {crate_name}: {count}").expect("write to string");
    }

    output.push_str("summary by crate and kind:\n");
    for ((crate_name, kind), count) in by_crate_and_kind {
        writeln!(&mut output, "  {crate_name} / {kind}: {count}").expect("write to string");
    }

    output
}

fn crate_name_from_path(path: &str) -> String {
    path.strip_prefix("crates/")
        .and_then(|rest| rest.split('/').next())
        .unwrap_or("<outside-crates>")
        .to_owned()
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .expect("rafter crate should live under <workspace>/crates/rafter")
        .to_path_buf()
}

fn library_rust_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_library_rust_files(root, &mut files);
    files.sort();
    files
}

fn collect_library_rust_files(root: &Path, files: &mut Vec<PathBuf>) {
    let entries =
        fs::read_dir(root).unwrap_or_else(|error| panic!("read {}: {error}", root.display()));
    for entry in entries {
        let path = entry
            .unwrap_or_else(|error| panic!("read entry under {}: {error}", root.display()))
            .path();
        if path.is_dir() {
            collect_library_rust_files(&path, files);
        } else if is_library_rust_file(&path) {
            files.push(path);
        }
    }
}

fn is_library_rust_file(path: &Path) -> bool {
    if path.extension().and_then(|extension| extension.to_str()) != Some("rs") {
        return false;
    }

    let path_text = path.to_string_lossy();
    !path_text.contains("/examples/")
        && !path_text.contains("/benches/")
        && !path_text.contains("/tests/")
        && !path_text.contains("/src/bin/")
        && !path_text.ends_with("/src/main.rs")
        && !path_text.ends_with("/tests.rs")
        && !path_text.ends_with("/test_support.rs")
        && !path_text.ends_with("_test.rs")
        && !path_text.ends_with("_tests.rs")
        && !path_text.contains("_tests/")
}

fn display_path(workspace: &Path, path: &Path) -> String {
    path.strip_prefix(workspace)
        .unwrap_or(path)
        .display()
        .to_string()
}
