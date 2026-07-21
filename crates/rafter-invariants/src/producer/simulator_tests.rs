//! Simulator producer scenarios grouped by the policy they protect.

#[path = "simulator_tests/classification.rs"]
mod classification;
#[path = "simulator_tests/compile_failure_metrics.rs"]
mod compile_failure_metrics;
#[path = "simulator_tests/coverage.rs"]
mod coverage;
#[cfg(unix)]
#[path = "simulator_tests/failures.rs"]
mod failures;
#[path = "simulator_tests/resources.rs"]
mod resources;
#[path = "simulator_tests/support.rs"]
mod support;
