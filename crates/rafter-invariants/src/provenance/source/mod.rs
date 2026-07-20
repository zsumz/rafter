//! Repository source identities used by producers and independent verifiers.

mod tracked;

pub(crate) use tracked::{parse_tracked_source_paths, tracked_source_paths_at};
