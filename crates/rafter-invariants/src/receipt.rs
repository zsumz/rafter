//! Test-only compatibility facade for canonical receipt fixtures.

mod fixtures;

pub(crate) use crate::verification::process_launchers_match_runtime;
pub(crate) use fixtures::{
    launchers as fixture_launchers, process_runtime as fixture_process_runtime,
};
