//! Independent acceptance of TLA+ receipts and proof artifacts.

mod artifact;
mod checkpoint;
mod completion;
mod detector;
mod invocation;
mod obligation;
mod observation;
mod receipt;
mod source;
mod verify;

pub(crate) use receipt::validate as validate_receipt;
pub(crate) use verify::verify_authenticated;

#[cfg(test)]
pub(crate) use checkpoint::validate_inventory;
#[cfg(test)]
pub(crate) use detector::successful_detector;
#[cfg(test)]
pub(crate) use detector::REQUIRED_MUTATION_TESTS;
#[cfg(test)]
pub(crate) use source::checksum_matches;
#[cfg(test)]
pub(crate) use verify::verify;
