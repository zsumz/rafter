//! Scenario: invariant tooling can only move toward its reviewed domain architecture.

#[path = "invariant_tooling_architecture_guard/analysis_scenarios.rs"]
mod analysis_scenarios;
#[path = "invariant_tooling_architecture_guard/support/mod.rs"]
mod architecture_support;
#[path = "invariant_tooling_architecture_guard/artifact_scenarios.rs"]
mod artifact_scenarios;
#[path = "invariant_tooling_architecture_guard/debt_scenarios.rs"]
mod debt_scenarios;
#[path = "invariant_tooling_architecture_guard/domain_scenarios.rs"]
mod domain_scenarios;
#[path = "support/invariant_tooling.rs"]
mod invariant_tooling;
#[path = "invariant_tooling_architecture_guard/maelstrom_scenarios.rs"]
mod maelstrom_scenarios;
#[path = "invariant_tooling_architecture_guard/process_scenarios.rs"]
mod process_scenarios;
#[path = "support/readability.rs"]
mod readability_support;
#[path = "invariant_tooling_architecture_guard/receipt_scenarios.rs"]
mod receipt_scenarios;
#[path = "invariant_tooling_architecture_guard/tla_scenarios.rs"]
mod tla_scenarios;
