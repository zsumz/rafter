//! Resolve in-tree parent paths without erasing filesystem alias checks.

use std::{
    fs,
    path::{Component, Path, PathBuf},
};

pub(super) fn resolve(workspace: &Path, path: &Path) -> Result<PathBuf, String> {
    let relative = path.strip_prefix(workspace).map_err(|_| {
        format!(
            "module source is outside the bound source tree: {}",
            path.display()
        )
    })?;
    // Preserve filesystem semantics even for components such as a trailing
    // `/.` that Path::components normalizes before the component walk.
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("inspect module source {}: {error}", path.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!(
            "module source is not a regular file: {}",
            path.display()
        ));
    }
    let mut checked = workspace.to_owned();
    let mut components = relative.components().peekable();
    while let Some(component) = components.next() {
        match component {
            Component::Normal(name) => {
                checked.push(name);
                let metadata = fs::symlink_metadata(&checked).map_err(|error| {
                    format!("inspect module source {}: {error}", checked.display())
                })?;
                if metadata.file_type().is_symlink() {
                    return Err(format!(
                        "module source is not a regular file or traverses a filesystem alias: {}",
                        checked.display()
                    ));
                }
                if components.peek().is_some() && !metadata.is_dir() {
                    return Err(format!(
                        "module source path component is not a directory: {}",
                        checked.display()
                    ));
                }
            }
            Component::ParentDir => {
                if checked == workspace || !checked.pop() {
                    return Err(format!(
                        "module source escapes the bound source tree: {}",
                        path.display()
                    ));
                }
            }
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir => {
                return Err(format!(
                    "module source has an unbound path: {}",
                    path.display()
                ));
            }
        }
    }
    let metadata = fs::symlink_metadata(&checked)
        .map_err(|error| format!("inspect module source {}: {error}", checked.display()))?;
    if !metadata.file_type().is_file() {
        return Err(format!(
            "module source is not a regular file: {}",
            checked.display()
        ));
    }
    let canonical = fs::canonicalize(&checked)
        .map_err(|error| format!("canonicalize module source {}: {error}", checked.display()))?;
    if canonical != checked {
        return Err(format!(
            "module source traverses a filesystem alias or noncanonical path: {}",
            path.display()
        ));
    }
    Ok(canonical)
}
