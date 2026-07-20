//! Deterministic wire renderers for already-decided invariant verdicts.

mod junit;
mod markdown;

pub(crate) use junit::render_junit;
pub(crate) use markdown::render_markdown;

#[cfg(test)]
mod tests;
