//! Registry syntax parser facade.

mod document;
mod evidence;
mod fields;
mod path;
mod scalar;
mod simulator;
mod syntax;
mod top_level;

pub(crate) use document::parse_registry_document;

#[cfg(test)]
mod fixtures;

#[cfg(test)]
mod tests;
