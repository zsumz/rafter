//! Producer-side compilation of invariant test targets.

mod cargo_output;
mod compilation;
mod executable;
mod protected;
mod scratch;
mod target;

pub(super) use compilation::{compile, CompiledTarget};
pub(super) use scratch::prepare_target_dir;
pub(super) use target::Target;

#[cfg(test)]
use cargo_output::executable_from_messages;

#[cfg(test)]
mod tests;
