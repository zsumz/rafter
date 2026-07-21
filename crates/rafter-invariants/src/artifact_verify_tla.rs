//! Compatibility facade for verification-owned TLA+ artifact acceptance.

pub(super) use crate::verification::tla::verify_authenticated;

#[cfg(test)]
pub(super) use crate::verification::tla::verify;

#[cfg(test)]
#[path = "verification/tla/tests/unit.rs"]
mod tests;

#[cfg(test)]
#[path = "verification/tla/tests/full_bundle.rs"]
mod full_bundle_tests;
