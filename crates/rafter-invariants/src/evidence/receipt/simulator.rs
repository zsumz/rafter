//! Structured simulator-liveness bindings embedded in check receipts.

use serde::{Deserialize, Serialize};

use crate::contract::profile::{SimulatorExecutionContract, SimulatorLivenessContract};

/// Exact typed contract and canonical report digests for one liveness check.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SimulatorLivenessBinding {
    pub schema_version: u32,
    pub contract: SimulatorLivenessContract,
    pub contract_sha256: String,
    pub reports_sha256: String,
    pub reports: Vec<SimulatorLivenessReportBinding>,
}

/// One validated simulator run bound to its complete structured report bytes.
#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SimulatorLivenessReportBinding {
    pub check_id: String,
    pub seed: u64,
    pub execution_contract: SimulatorExecutionContract,
    pub execution_contract_sha256: String,
    pub report_sha256: String,
    pub round_limit: u64,
    pub rounds_used: u64,
}
