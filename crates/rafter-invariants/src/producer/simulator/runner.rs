//! Aggregate simulator execution and result-bundle assembly contract.

use std::{collections::BTreeMap, error::Error, path::Path};

use crate::{
    contract::{catalog::Catalog, profile::ProfileContract},
    evidence::{ExecutionReceipt, ResultBundle, SourceReceipt, RESULT_SCHEMA_VERSION},
};

use super::{
    check_contract::liveness_contracts,
    detector::{run_detectors, DetectorRun},
    evaluation::evaluate_descriptors,
    resources::execution_resource_metrics,
};
use crate::producer::{simulator_model, source, ProducerContext};

pub(in crate::producer) fn run(
    catalog: &Catalog,
    contract: &ProfileContract,
    profile: &str,
    source: SourceReceipt,
    output_dir: &Path,
    context: &ProducerContext<'_>,
) -> Result<ResultBundle, Box<dyn Error>> {
    let runner = contract
        .runners
        .get("simulator")
        .ok_or("simulator runner missing")?;
    let descriptors = catalog
        .required_evidence(contract)
        .into_values()
        .flatten()
        .filter(|descriptor| descriptor.layer == "simulator")
        .collect::<Vec<_>>();
    let model = simulator_model::execute(profile, &source.commit, output_dir)?;
    let detectors = match run_detectors(&descriptors, profile, &source.commit, output_dir) {
        Ok(detectors) => detectors,
        Err(error) => DetectorRun {
            outcomes: BTreeMap::new(),
            artifacts: Vec::new(),
            peak_rss_kib: 0,
            duration_ms: 0,
            harness_error: Some(format!("simulator detector execution failed: {error}")),
        },
    };
    let liveness_contracts = liveness_contracts(&descriptors)?;
    let (checks, results) = evaluate_descriptors(
        &descriptors,
        profile,
        &runner.simulator_checks,
        &liveness_contracts,
        &model,
        &detectors,
    )?;
    source::verify(&source)?;
    let mut execution_artifacts = model.artifacts.clone();
    execution_artifacts.extend(detectors.artifacts.clone());
    let resources = execution_resource_metrics(&model, &detectors);
    Ok(ResultBundle {
        schema_version: RESULT_SCHEMA_VERSION,
        runner: "simulator".to_owned(),
        profile: profile.to_owned(),
        source_ref: source.commit.clone(),
        execution: ExecutionReceipt {
            plan: context.plan.clone(),
            invocation: context.invocation.clone(),
            producer: context.producer.clone(),
            source,
            checks,
            duration_ms: resources.duration_ms,
            peak_rss_kib: resources.peak_rss_kib,
            artifacts: execution_artifacts,
        },
        results,
    })
}
