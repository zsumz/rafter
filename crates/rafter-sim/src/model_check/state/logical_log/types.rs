//! Domain types retained by logical-log verification.

mod prefix;
mod view;
mod violation;

pub(crate) use prefix::LogPrefixWitness;
pub(crate) use view::LogicalLogView;
pub(crate) use violation::LogicalLogViolation;
