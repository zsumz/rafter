//! Deterministic wire binding for already-classified liveness claims.

use serde_json::Value;

use super::digest::{canonical_value_digest, serialized_digest};
use crate::{
    contract::profile::{SimulatorExecutionContract, SimulatorLivenessContract},
    evidence::{SimulatorLivenessBinding, SimulatorLivenessReportBinding},
};

pub(crate) struct LivenessBindingClaim {
    pub contract: SimulatorLivenessContract,
    pub reports: Vec<LivenessReportClaim>,
}

pub(crate) struct LivenessReportClaim {
    pub check_id: String,
    pub seed: u64,
    pub execution_contract: SimulatorExecutionContract,
    pub report: Value,
    pub round_limit: u64,
    pub rounds_used: u64,
}

pub(crate) fn bind_liveness_claims(claim: LivenessBindingClaim) -> SimulatorLivenessBinding {
    let mut reports = claim
        .reports
        .into_iter()
        .map(bind_report)
        .collect::<Vec<_>>();
    reports.sort();
    SimulatorLivenessBinding {
        schema_version: 1,
        contract_sha256: serialized_digest(&claim.contract),
        reports_sha256: serialized_digest(&reports),
        contract: claim.contract,
        reports,
    }
}

fn bind_report(claim: LivenessReportClaim) -> SimulatorLivenessReportBinding {
    SimulatorLivenessReportBinding {
        check_id: claim.check_id,
        seed: claim.seed,
        execution_contract_sha256: serialized_digest(&claim.execution_contract),
        execution_contract: claim.execution_contract,
        report_sha256: canonical_value_digest(&claim.report),
        round_limit: claim.round_limit,
        rounds_used: claim.rounds_used,
    }
}
