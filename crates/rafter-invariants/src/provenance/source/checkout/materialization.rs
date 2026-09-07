//! Raw tracked-worktree materialization observed against the recorded Git tree.

use std::{
    collections::BTreeSet,
    error::Error,
    fs,
    path::{Component, Path, PathBuf},
};

use sha2::{Digest, Sha256};

#[cfg(test)]
use super::CommandOutput;
use super::{
    command_output_at, command_output_raw_at, CheckoutCommandRunner, GeneratedOutputPolicy,
};

mod tree;

use tree::{digest_frame, git_blob_oid, parse_tree_inventory, read_bound_entry};

const MATERIALIZATION_CONTRACT: &str = "git-head-worktree-raw-v1";
const MAX_CAPTURED_SOURCE_BYTES: u64 = 512 * 1024 * 1024;

#[cfg(test)]
#[path = "materialization_tests.rs"]
mod tests;

#[derive(Debug)]
pub(super) struct MaterializedSource {
    pub(super) commit: String,
    pub(super) tree: String,
    pub(super) receipt: MaterializationObservation,
    pub(super) files: Vec<CapturedSourceFile>,
}

#[derive(Debug)]
pub(crate) struct CapturedSourceFile {
    pub(crate) path: PathBuf,
    pub(crate) executable: bool,
    pub(crate) bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaterializationObservation {
    pub(crate) contract: String,
    pub(crate) sha256: String,
    pub(crate) tracked_entries: u64,
    pub(crate) submodules: u64,
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
    runner: &impl CheckoutCommandRunner,
    generated_outputs: &impl GeneratedOutputPolicy,
) -> Result<MaterializedSource, Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let commit = git(runner, &root, &["rev-parse", "--verify", "HEAD^{commit}"])?;
    let object_format = match git(runner, &root, &["rev-parse", "--show-object-format"])?.as_str() {
        "sha1" => GitObjectFormat::Sha1,
        "sha256" => GitObjectFormat::Sha256,
        format => return Err(format!("unsupported Git object format: {format:?}").into()),
    };
    validate_ignored_paths(&root, runner, generated_outputs)?;
    let tree_expression = format!("{commit}^{{tree}}");
    let tree = git(runner, &root, &["rev-parse", "--verify", &tree_expression])?;
    let inventory = git_raw(
        runner,
        &root,
        &["ls-tree", "-r", "-z", "--full-tree", &tree],
    )?;
    let entries = parse_tree_inventory(&inventory)?;
    if entries.is_empty() {
        return Err("recorded Git tree has no tracked entries".into());
    }

    let mut digest = Sha256::new();
    let mut captured_bytes = 0_u64;
    let mut files = Vec::with_capacity(entries.len());
    digest_frame(&mut digest, MATERIALIZATION_CONTRACT.as_bytes());
    for entry in &entries {
        let content = read_bound_entry(&root, entry)?;
        captured_bytes = captured_bytes
            .checked_add(u64::try_from(content.len())?)
            .ok_or("captured source size overflowed u64")?;
        if captured_bytes > MAX_CAPTURED_SOURCE_BYTES {
            return Err(format!(
                "tracked source exceeds the {MAX_CAPTURED_SOURCE_BYTES}-byte snapshot limit"
            )
            .into());
        }
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
        files.push(CapturedSourceFile {
            path: entry.path.clone(),
            executable: entry.mode == "100755",
            bytes: content,
        });
    }

    let final_commit = git(runner, &root, &["rev-parse", "--verify", "HEAD^{commit}"])?;
    if final_commit != commit {
        return Err("HEAD changed while source materialization was captured".into());
    }

    Ok(MaterializedSource {
        commit,
        tree,
        receipt: MaterializationObservation {
            contract: MATERIALIZATION_CONTRACT.to_owned(),
            sha256: format!("{:x}", digest.finalize()),
            tracked_entries: entries.len().try_into()?,
            submodules: 0,
        },
        files,
    })
}

fn git(
    runner: &impl CheckoutCommandRunner,
    root: &Path,
    arguments: &[&str],
) -> Result<String, Box<dyn Error>> {
    git_output(runner, root, arguments, false)
}

fn git_raw(
    runner: &impl CheckoutCommandRunner,
    root: &Path,
    arguments: &[&str],
) -> Result<String, Box<dyn Error>> {
    let mut bound = Vec::with_capacity(arguments.len() + 1);
    bound.push("--no-replace-objects");
    bound.extend_from_slice(arguments);
    command_output_raw_at(runner, "git", &bound, false, root)
}

fn git_output(
    runner: &impl CheckoutCommandRunner,
    root: &Path,
    arguments: &[&str],
    allow_empty: bool,
) -> Result<String, Box<dyn Error>> {
    let mut bound = Vec::with_capacity(arguments.len() + 1);
    bound.push("--no-replace-objects");
    bound.extend_from_slice(arguments);
    command_output_at(runner, "git", &bound, allow_empty, root)
}

fn validate_ignored_paths(
    root: &Path,
    runner: &impl CheckoutCommandRunner,
    generated_outputs: &impl GeneratedOutputPolicy,
) -> Result<(), Box<dyn Error>> {
    let inventory = {
        let arguments = [
            "--no-replace-objects",
            "ls-files",
            "-z",
            "--others",
            "--ignored",
            "--exclude-standard",
        ];
        command_output_raw_at(runner, "git", &arguments, true, root)?
    };
    validate_ignored_inventory(&inventory, generated_outputs)?;
    validate_ignored_path_types(root, &inventory)
}

fn validate_ignored_inventory(
    inventory: &str,
    generated_outputs: &impl GeneratedOutputPolicy,
) -> Result<(), Box<dyn Error>> {
    for value in inventory.split('\0').filter(|value| !value.is_empty()) {
        let path = Path::new(value);
        if !path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
        {
            return Err(format!("Git reported a noncanonical ignored path: {value:?}").into());
        }
        if !generated_outputs.permits(path) {
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
