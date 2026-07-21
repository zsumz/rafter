//! Confined Maelstrom scratch-state discovery and artifact-tree capture.

use std::{error::Error, path::Path, time::Instant};

use crate::{
    evidence::ArtifactRef,
    execution::filesystem::{EntryKind, HeldDirectory, OperationDeadline, TREE_LIMITS},
};

use super::super::super::artifact;

pub(in crate::producer) fn reset_state_directory(
    path: &Path,
    deadline: Instant,
) -> Result<HeldDirectory, Box<dyn Error>> {
    HeldDirectory::replace_tree(
        path,
        TREE_LIMITS,
        OperationDeadline::at(deadline, "Maelstrom scratch cleanup"),
    )
}

pub(super) fn capture_binary(
    output_dir: &Path,
    namespace: &Path,
    binary: &Path,
    kind: &str,
    artifacts: &mut Vec<ArtifactRef>,
) -> Result<(), Box<dyn Error>> {
    if !binary.is_file() {
        return Err(format!("Maelstrom run did not produce {}", binary.display()).into());
    }
    artifacts.push(artifact::capture(
        output_dir,
        &namespace.join("inputs"),
        binary,
        kind,
    )?);
    Ok(())
}

pub(in crate::producer) fn discover_store(
    state_dir: &HeldDirectory,
    deadline: Instant,
) -> Result<HeldDirectory, String> {
    OperationDeadline::at(deadline, "Maelstrom store discovery")
        .check()
        .map_err(|error| error.to_string())?;
    let root = state_dir
        .open_dir(Path::new("store/lin-kv"))
        .map_err(|error| format!("read Maelstrom store: {error}"))?;
    let stores = root
        .entries(OperationDeadline::at(deadline, "Maelstrom store discovery"))
        .map_err(|error| format!("read Maelstrom store: {error}"))?
        .into_iter()
        .filter_map(|(name, kind)| (kind == EntryKind::Directory).then_some(name))
        .collect::<Vec<_>>();
    match stores.as_slice() {
        [store] => root
            .open_dir(Path::new(store))
            .map_err(|error| format!("open Maelstrom store: {error}")),
        _ => Err(format!(
            "expected one Maelstrom retained store, found {}",
            stores.len()
        )),
    }
}

pub(in crate::producer) fn capture_tree(
    output_dir: &Path,
    namespace: &Path,
    root: &HeldDirectory,
    artifacts: &mut Vec<ArtifactRef>,
    deadline: Instant,
) -> Result<(), Box<dyn Error>> {
    for relative in root.files_below(
        TREE_LIMITS,
        OperationDeadline::at(deadline, "Maelstrom evidence traversal"),
    )? {
        let read_deadline = OperationDeadline::at(deadline, "Maelstrom evidence file read");
        read_deadline.check()?;
        let kind = if relative == Path::new("results.edn") {
            "maelstrom-results"
        } else if relative.starts_with("node-logs") {
            "maelstrom-node-log"
        } else if namespace.ends_with("durable") {
            "maelstrom-durable-file"
        } else {
            "maelstrom-store-file"
        };
        let bytes = root.read_bounded(
            &relative,
            read_deadline,
            crate::evidence::limits::MAX_ARTIFACT_BYTES,
        )?;
        artifacts.push(artifact::write(
            output_dir,
            &namespace.join(&relative),
            kind,
            &bytes,
        )?);
        read_deadline.check()?;
    }
    Ok(())
}
