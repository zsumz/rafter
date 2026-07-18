use std::{
    collections::BTreeSet,
    error::Error,
    fs,
    path::{Component, Path, PathBuf},
};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use sha1::Sha1;
use sha2::{Digest, Sha256};

use crate::SourceMaterializationReceipt;

use super::{command_output_at, command_output_raw_at, CaptureBudget};

const MATERIALIZATION_CONTRACT: &str = "git-head-worktree-raw-v1";

#[cfg(test)]
#[path = "materialization_tests.rs"]
mod tests;

#[derive(Debug)]
pub(super) struct MaterializedSource {
    pub(super) commit: String,
    pub(super) tree: String,
    pub(super) receipt: SourceMaterializationReceipt,
}

struct TreeEntry {
    mode: String,
    oid: String,
    path: PathBuf,
    path_text: String,
}

#[derive(Clone, Copy)]
enum GitObjectFormat {
    Sha1,
    Sha256,
}

pub(super) fn capture_materialization(
    root: &Path,
    budget: CaptureBudget,
) -> Result<MaterializedSource, Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let commit = git(&root, budget, &["rev-parse", "--verify", "HEAD^{commit}"])?;
    let object_format = match git(&root, budget, &["rev-parse", "--show-object-format"])?.as_str() {
        "sha1" => GitObjectFormat::Sha1,
        "sha256" => GitObjectFormat::Sha256,
        format => return Err(format!("unsupported Git object format: {format:?}").into()),
    };
    validate_ignored_paths(&root, budget)?;
    let tree_expression = format!("{commit}^{{tree}}");
    let tree = git(&root, budget, &["rev-parse", "--verify", &tree_expression])?;
    let inventory = git_raw(
        &root,
        budget,
        &["ls-tree", "-r", "-z", "--full-tree", &tree],
    )?;
    let entries = parse_tree_inventory(&inventory)?;
    if entries.is_empty() {
        return Err("recorded Git tree has no tracked entries".into());
    }

    let mut digest = Sha256::new();
    digest_frame(&mut digest, MATERIALIZATION_CONTRACT.as_bytes());
    for entry in &entries {
        let content = read_bound_entry(&root, entry)?;
        let observed_oid = git_blob_oid(object_format, &content);
        if observed_oid != entry.oid {
            return Err(format!(
                "tracked worktree bytes differ from recorded Git tree: {}",
                entry.path.display()
            )
            .into());
        }
        digest_frame(&mut digest, entry.mode.as_bytes());
        digest_frame(&mut digest, entry.path_text.as_bytes());
        digest_frame(&mut digest, &content);
    }

    let final_commit = git(&root, budget, &["rev-parse", "--verify", "HEAD^{commit}"])?;
    if final_commit != commit {
        return Err("HEAD changed while source materialization was captured".into());
    }

    Ok(MaterializedSource {
        commit,
        tree,
        receipt: SourceMaterializationReceipt {
            contract: MATERIALIZATION_CONTRACT.to_owned(),
            sha256: format!("{:x}", digest.finalize()),
            tracked_entries: entries.len().try_into()?,
            submodules: 0,
        },
    })
}

fn git(root: &Path, budget: CaptureBudget, arguments: &[&str]) -> Result<String, Box<dyn Error>> {
    git_output(root, budget, arguments, false)
}

fn git_raw(
    root: &Path,
    budget: CaptureBudget,
    arguments: &[&str],
) -> Result<String, Box<dyn Error>> {
    let mut bound = Vec::with_capacity(arguments.len() + 1);
    bound.push("--no-replace-objects");
    bound.extend_from_slice(arguments);
    command_output_raw_at("git", &bound, false, root, budget)
}

fn git_output(
    root: &Path,
    budget: CaptureBudget,
    arguments: &[&str],
    allow_empty: bool,
) -> Result<String, Box<dyn Error>> {
    let mut bound = Vec::with_capacity(arguments.len() + 1);
    bound.push("--no-replace-objects");
    bound.extend_from_slice(arguments);
    command_output_at("git", &bound, allow_empty, root, budget)
}

fn validate_ignored_paths(root: &Path, budget: CaptureBudget) -> Result<(), Box<dyn Error>> {
    let inventory = {
        let arguments = [
            "--no-replace-objects",
            "ls-files",
            "-z",
            "--others",
            "--ignored",
            "--exclude-standard",
        ];
        command_output_raw_at("git", &arguments, true, root, budget)?
    };
    validate_ignored_inventory(&inventory)?;
    validate_ignored_path_types(root, &inventory)
}

fn validate_ignored_inventory(inventory: &str) -> Result<(), Box<dyn Error>> {
    for value in inventory.split('\0').filter(|value| !value.is_empty()) {
        let path = Path::new(value);
        if !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        {
            return Err(format!("Git reported a noncanonical ignored path: {value:?}").into());
        }
        if !reviewed_generated_output(path) {
            return Err(format!(
                "ignored path is outside reviewed generated-output roots: {value}"
            )
            .into());
        }
    }
    Ok(())
}

fn validate_ignored_path_types(root: &Path, inventory: &str) -> Result<(), Box<dyn Error>> {
    let mut checked = BTreeSet::new();
    for value in inventory.split('\0').filter(|value| !value.is_empty()) {
        let mut current = root.to_owned();
        for component in Path::new(value).components() {
            let Component::Normal(component) = component else {
                return Err(format!("Git reported a noncanonical ignored path: {value:?}").into());
            };
            current.push(component);
            if !checked.insert(current.clone()) {
                continue;
            }
            let metadata = match fs::symlink_metadata(&current) {
                Ok(metadata) => metadata,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(error) => {
                    return Err(format!(
                        "inspect ignored path component {}: {error}",
                        current.display()
                    )
                    .into());
                }
            };
            if metadata.file_type().is_symlink() {
                return Err(format!(
                    "ignored filesystem symlink is outside the source binding contract: {}",
                    current.display()
                )
                .into());
            }
        }
    }
    Ok(())
}

fn reviewed_generated_output(path: &Path) -> bool {
    let components = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>();
    matches!(components.as_slice(), [first, ..] if first == "target" || first == "store")
        || matches!(components.as_slice(), [first, second, ..]
            if (first == "artifacts" && second == "invariants")
                || (first == "artifacts" && reviewed_tla_evidence_artifact(second))
                || (first == "bench-compare" && second == "target")
                || (first == "fuzz" && second == "target")
                || (first == "tools" && second == "cache"))
        || matches!(components.as_slice(), [first, second, third, ..]
            if first == "crates" && second == "rafter-invariants" && third == "target")
        || matches!(components.as_slice(), [first, second, rest @ ..]
            if first == "specs" && second == "tla" && rest.iter().any(|value| value == "states"))
        || components.iter().any(|value| value == "__pycache__")
        || path.extension().is_some_and(|extension| extension == "pyc")
}

fn reviewed_tla_evidence_artifact(name: &str) -> bool {
    matches!(
        name,
        "tla-log"
            | "tla.log"
            | "tla-trace-log"
            | "tla-tool"
            | "tla-spec"
            | "tla-trace-spec"
            | "tla-detector-spec"
            | "tla-runner"
            | "tla-tool-asset-id"
            | "tla-tool-checksums"
            | "tla-config"
            | "tla-trace-config"
            | "tla-detector-config"
            | "tla-mutation-log"
            | "tla-producer"
            | "tla-checkpoint-contract"
            | "tla-checkpoint-inventory"
            | "tla-checkpoint-recovered-contract"
            | "tla-checkpoint-recovered-inventory"
            | "tla-checkpoint-recovery-report"
    ) || crate::producer::tla_output::DETECTOR_PROBES
        .into_iter()
        .any(|probe| {
            crate::producer::tla_output::detector_log_kind(probe)
                .is_some_and(|kind| normalize_fixture_artifact_name(&kind) == name)
                || crate::producer::tla_output::detector_config_kind(probe)
                    .is_some_and(|kind| normalize_fixture_artifact_name(&kind) == name)
        })
}

fn normalize_fixture_artifact_name(kind: &str) -> String {
    kind.replace(':', "-")
}

fn parse_tree_inventory(inventory: &str) -> Result<Vec<TreeEntry>, Box<dyn Error>> {
    let mut paths = BTreeSet::new();
    inventory
        .split('\0')
        .filter(|record| !record.is_empty())
        .map(|record| {
            let (header, path_text) = record
                .split_once('\t')
                .ok_or("Git tree entry omitted its path separator")?;
            let mut fields = header.split(' ');
            let mode = fields.next().ok_or("Git tree entry omitted its mode")?;
            let kind = fields.next().ok_or("Git tree entry omitted its kind")?;
            let oid = fields
                .next()
                .ok_or("Git tree entry omitted its object ID")?;
            if fields.next().is_some() {
                return Err("Git tree entry has unexpected header fields".into());
            }
            if mode == "120000" && kind == "blob" {
                return Err(
                    "Git symlinks are outside the raw source materialization contract".into(),
                );
            }
            if !matches!(mode, "100644" | "100755") || kind != "blob" {
                if mode == "160000" && kind == "commit" {
                    return Err(
                        "Git submodules are outside the raw source materialization contract".into(),
                    );
                }
                return Err(
                    format!("unsupported Git tree entry mode and kind: {mode} {kind}").into(),
                );
            }
            if !matches!(oid.len(), 40 | 64)
                || !oid
                    .bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            {
                return Err(format!("invalid Git tree object ID: {oid:?}").into());
            }
            let path = PathBuf::from(path_text);
            if path_text.is_empty()
                || !path
                    .components()
                    .all(|component| matches!(component, Component::Normal(_)))
            {
                return Err(format!("Git tree contains a noncanonical path: {path_text:?}").into());
            }
            if !paths.insert(path.clone()) {
                return Err(format!("Git tree contains duplicate path: {path_text:?}").into());
            }
            Ok(TreeEntry {
                mode: mode.to_owned(),
                oid: oid.to_owned(),
                path,
                path_text: path_text.to_owned(),
            })
        })
        .collect()
}

fn read_bound_entry(root: &Path, entry: &TreeEntry) -> Result<Vec<u8>, Box<dyn Error>> {
    let path = root.join(&entry.path);
    let parent = path
        .parent()
        .ok_or_else(|| format!("tracked path has no parent: {}", path.display()))?;
    if fs::canonicalize(parent)? != parent {
        return Err(format!(
            "tracked path traverses a filesystem alias or symlink: {}",
            path.display()
        )
        .into());
    }
    let metadata = fs::symlink_metadata(&path)?;
    if !metadata.file_type().is_file() || fs::canonicalize(&path)? != path {
        return Err(format!(
            "tracked regular file traverses a filesystem alias or changed type: {}",
            path.display()
        )
        .into());
    }
    #[cfg(unix)]
    {
        let executable = metadata.permissions().mode() & 0o100 != 0;
        if executable != (entry.mode == "100755") {
            return Err(format!(
                "tracked executable mode differs from Git tree: {}",
                path.display()
            )
            .into());
        }
    }
    #[cfg(not(unix))]
    if entry.mode == "100755" {
        return Err("tracked executable modes require Unix permission support".into());
    }
    Ok(fs::read(path)?)
}

fn git_blob_oid(format: GitObjectFormat, content: &[u8]) -> String {
    let header = format!("blob {}\0", content.len());
    match format {
        GitObjectFormat::Sha1 => {
            let mut digest = Sha1::new();
            digest.update(header.as_bytes());
            digest.update(content);
            format!("{:x}", digest.finalize())
        }
        GitObjectFormat::Sha256 => {
            let mut digest = Sha256::new();
            digest.update(header.as_bytes());
            digest.update(content);
            format!("{:x}", digest.finalize())
        }
    }
}

fn digest_frame(digest: &mut Sha256, bytes: &[u8]) {
    digest.update(u64::try_from(bytes.len()).unwrap_or(u64::MAX).to_be_bytes());
    digest.update(bytes);
}
