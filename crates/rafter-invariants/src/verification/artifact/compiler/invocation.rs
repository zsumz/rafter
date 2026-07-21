//! Shared Cargo invocation path predicates.

use std::path::Path;

pub(crate) fn target_directory_matches(recorded: Option<&str>, expected: &Path) -> bool {
    recorded.is_some_and(|recorded| Path::new(recorded) == expected && expected.is_absolute())
}
