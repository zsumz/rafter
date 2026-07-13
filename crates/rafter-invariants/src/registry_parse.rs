mod document;
mod evidence;
mod fields;
mod simulator;
mod syntax;
mod top_level;

pub(crate) use document::parse_registry_document;

#[cfg(test)]
mod registry_parse_test_fixtures;

#[cfg(test)]
mod tests;
