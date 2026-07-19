//! Assembly of producer-accepted claims into immutable evidence.

use std::collections::BTreeMap;

use serde_json::Value;

use super::{
    error::{malformed, missing, LivenessReportError},
    inventory::expected_report_contracts,
    raw::accept_run,
};
use crate::{
    contract::{
        profile::{expected_execution_contract, SimulatorLivenessContract},
        SimulatorIdentity,
    },
    evidence::{bind_liveness_claims, LivenessBindingClaim, SimulatorLivenessBinding},
};

pub(in crate::producer::simulator) fn derive_liveness_binding(
    profile: &str,
    identity: &SimulatorIdentity,
    available_contracts: &[SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<SimulatorLivenessBinding, LivenessReportError> {
    let contract = identity.liveness_report.as_ref().ok_or_else(|| {
        malformed("simulator identity does not declare a liveness report contract")
    })?;
    let mut reports = Vec::new();
    for check_id in &identity.checks {
        let execution = expected_execution_contract(profile, check_id)
            .map_err(|message| malformed(format!("liveness run `{check_id}`: {message}")))?;
        let expected_reports = expected_report_contracts(available_contracts, &execution)?;
        let runs = events.get(check_id).map(Vec::as_slice).unwrap_or_default();
        if runs.is_empty() {
            return Err(missing(format!(
                "required simulator check `{check_id}` has no liveness run"
            )));
        }
        for event in runs {
            reports.push(accept_run(
                profile,
                check_id,
                &contract.feature_id,
                &execution,
                &expected_reports,
                event,
            )?);
        }
    }
    Ok(bind_liveness_claims(LivenessBindingClaim {
        contract: contract.clone(),
        reports,
    }))
}
