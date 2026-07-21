//! Stable logical-log violation records.

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct LogicalLogViolation {
    pub(crate) invariant: &'static str,
    pub(crate) message: String,
}
