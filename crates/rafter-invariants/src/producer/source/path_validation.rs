use std::{
    collections::HashSet,
    error::Error,
    fs,
    path::{Component, Path, PathBuf},
};

pub(super) fn validate_tracked_source_path(
    root: &Path,
    path: &Path,
    tracked: &HashSet<PathBuf>,
    kind: &str,
) -> Result<(), Box<dyn Error>> {
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        root.join(path)
    };
    let lexical = absolute.strip_prefix(root).map_err(|_| {
        format!(
            "Cargo {kind} is outside the bound source tree: {}",
            absolute.display()
        )
    })?;
    let mut relative = PathBuf::new();
    for component in lexical.components() {
        match component {
            Component::Normal(value) => relative.push(value),
            Component::CurDir => {}
            Component::ParentDir => {
                if !relative.pop() {
                    return Err(format!(
                        "Cargo {kind} escapes the bound source tree: {}",
                        absolute.display()
                    )
                    .into());
                }
            }
            Component::Prefix(_) | Component::RootDir => {
                return Err(format!(
                    "Cargo {kind} has a non-relative source path: {}",
                    absolute.display()
                )
                .into());
            }
        }
    }
    let mut checked = root.to_owned();
    for component in relative.components() {
        checked.push(component.as_os_str());
        let metadata = fs::symlink_metadata(&checked).map_err(|error| {
            format!(
                "inspect Cargo {kind} path component {}: {error}",
                checked.display()
            )
        })?;
        if metadata.file_type().is_symlink() {
            return Err(format!(
                "Cargo {kind} uses an unbound filesystem symlink: {}",
                checked.display()
            )
            .into());
        }
    }
    let canonical = fs::canonicalize(&checked)
        .map_err(|error| format!("canonicalize Cargo {kind} {}: {error}", checked.display()))?;
    canonical.strip_prefix(root).map_err(|_| {
        format!(
            "Cargo {kind} is outside the bound source tree: {}",
            canonical.display()
        )
    })?;
    if !tracked.contains(&relative) {
        return Err(format!(
            "Cargo {kind} is not tracked by the bound source tree: {}",
            checked.display()
        )
        .into());
    }
    Ok(())
}
