//! Recovery of the producer checkout root from an exact TLC working directory.

use std::path::{Component, Path, PathBuf};

use crate::verification::AggregateError;

pub(super) fn from_current_dir(current_dir: &str) -> Result<PathBuf, AggregateError> {
    let current_dir = Path::new(current_dir);
    let suffix = Path::new("specs/tla/raft");
    let clean_absolute = current_dir.is_absolute()
        && current_dir
            .components()
            .all(|component| !matches!(component, Component::CurDir | Component::ParentDir));
    let repository = current_dir
        .ancestors()
        .nth(suffix.components().count())
        .filter(|repository| {
            repository
                .components()
                .any(|part| matches!(part, Component::Normal(_)))
        });
    let Some(repository) = repository else {
        return Err(AggregateError::new(
            "TLA working directory does not identify a producer checkout".to_owned(),
        ));
    };
    if !clean_absolute || repository.join(suffix) != current_dir {
        return Err(AggregateError::new(
            "TLA working directory is not the exact repository-relative spec path".to_owned(),
        ));
    }
    Ok(repository.to_owned())
}
