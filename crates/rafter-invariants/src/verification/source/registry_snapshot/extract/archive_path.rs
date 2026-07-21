//! Canonical registry archive path and case-collision policy.

use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Component, Path, PathBuf},
};

const MAX_PATH_DEPTH: usize = 64;

pub(in crate::verification::source::registry_snapshot) fn package_relative(
    path: &Path,
    expected_root: &str,
) -> Result<PathBuf, String> {
    let mut components = path.components();
    let Some(Component::Normal(root)) = components.next() else {
        return Err(format!(
            "registry archive path is not relative: {}",
            path.display()
        ));
    };
    if root != expected_root {
        return Err(format!(
            "registry archive root {} does not match {expected_root}",
            Path::new(root).display()
        ));
    }
    let mut relative = PathBuf::new();
    for component in components {
        let Component::Normal(name) = component else {
            return Err(format!(
                "registry archive path contains traversal: {}",
                path.display()
            ));
        };
        relative.push(name);
    }
    if relative.components().count() > MAX_PATH_DEPTH {
        return Err(format!(
            "registry archive path exceeds its depth limit: {}",
            path.display()
        ));
    }
    Ok(relative)
}

pub(in crate::verification::source::registry_snapshot) fn require_unique_path(
    path: &Path,
    seen: &mut BTreeSet<PathBuf>,
    folded: &mut BTreeMap<String, PathBuf>,
) -> Result<(), String> {
    if !seen.insert(path.to_owned()) {
        return Err(format!("registry archive repeats path {}", path.display()));
    }
    let mut prefix = PathBuf::new();
    for component in path.components() {
        let Component::Normal(name) = component else {
            return Err(
                "registry archive uniqueness check received a noncanonical path".to_owned(),
            );
        };
        prefix.push(name);
        let text = prefix
            .to_str()
            .ok_or_else(|| "registry archive path is not UTF-8".to_owned())?;
        let key = text.to_lowercase();
        if let Some(existing) = folded.get(&key) {
            if existing != &prefix {
                return Err(format!(
                    "registry archive has case-colliding paths {} and {}",
                    existing.display(),
                    prefix.display()
                ));
            }
        } else {
            folded.insert(key, prefix.clone());
        }
    }
    Ok(())
}
