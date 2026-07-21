//! Independent acceptance of Maelstrom receipts and end-to-end artifacts.

mod artifact;
mod configuration;
mod invocation;
mod lease;
mod observation;
mod receipt;
mod scenario;
mod status;
#[cfg(test)]
pub(crate) mod test_support;
mod verify;

pub(crate) use receipt::validate as validate_receipt;
pub(crate) use verify::verify_authenticated;

#[cfg(test)]
pub(crate) use verify::verify;
