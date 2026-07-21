//! Authentication and semantic reconstruction of runner artifacts.

mod compiler;
mod metrics;
mod test_execution;
mod test_runner;
mod verify;

pub(crate) use verify::verify_bundle;

#[cfg(test)]
pub(crate) mod test_support;
#[cfg(test)]
pub(crate) use verify::detector_log_verifier;

#[cfg(test)]
mod tests;
