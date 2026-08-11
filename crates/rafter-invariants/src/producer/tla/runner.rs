//! TLA+ producer orchestration and receipt assembly.

use std::{collections::BTreeSet, error::Error, path::Path};

use crate::{
    contract::{catalog::Catalog, profile::ProfileContract},
    evidence::{
        CheckReceipt, ExecutionReceipt, ResultBundle, SourceReceipt, RESULT_SCHEMA_VERSION,
    },
};

use super::{
    artifact,
    contract::{
        fetch_tool, parse_timeout, required_configuration, source_artifacts, validate_java,
        validate_obligation_options, validate_obligation_specs, validate_runner_options,
        validate_spec_contract,
    },
    evaluation::{evaluate, observations},
    execution::{execute, ExecutionRequest},
    process,
    result::evidence_result,
    source, ProducerContext,
};

pub(in crate::producer) fn run(
    catalog: &Catalog,
    contract: &ProfileContract,
    profile: &str,
    source: SourceReceipt,
    output_dir: &Path,
    context: &ProducerContext<'_>,
) -> Result<ResultBundle, Box<dyn Error>> {
    let runner = contract.runners.get("tla").ok_or("TLA runner missing")?;
    process::ensure_execution_deadline(profile, "tla", "TLA runner validation")?;
    validate_runner_options(&runner.configuration)?;
    validate_obligation_options(&runner.obligations)?;
    validate_java(&source, &runner.configuration)?;
    fetch_tool()?;
    process::ensure_execution_deadline(profile, "tla", "TLA tool preparation")?;
    let artifacts = source_artifacts(
        &runner.configuration,
        &runner.obligations,
        output_dir,
        profile,
        &source.commit,
    )?;
    process::ensure_execution_deadline(profile, "tla", "TLA input capture")?;
    let descriptors = catalog
        .required_evidence(contract)
        .into_values()
        .flatten()
        .filter(|descriptor| descriptor.layer == "tla")
        .collect::<Vec<_>>();
    let symbols = descriptors
        .iter()
        .map(|descriptor| descriptor.symbol.clone())
        .collect::<BTreeSet<_>>();
    let config_name = required_configuration(&runner.configuration, "config")?;
    let configured = validate_spec_contract(config_name, &symbols)?;
    validate_obligation_specs(&runner.obligations, &symbols)?;
    process::ensure_execution_deadline(profile, "tla", "TLA specification validation")?;
    let timeout = parse_timeout(required_configuration(
        &runner.configuration,
        "soft_timeout",
    )?)?;
    let execution = execute(
        ExecutionRequest {
            profile,
            source_ref: &source.commit,
            config: config_name,
            configuration: &runner.configuration,
            obligations: &runner.obligations,
            timeout,
            output_dir,
        },
        artifacts,
    )?;
    let verdict = evaluate(&execution, &symbols, &runner.configuration);
    let execution_id = artifact::stable_id("tla", &format!("{profile}/{config_name}"));
    let evidence_ids = descriptors
        .iter()
        .map(crate::contract::catalog::EvidenceDescriptor::evidence_id)
        .collect::<Vec<_>>();
    let results = descriptors
        .iter()
        .map(|descriptor| {
            evidence_result(descriptor, &execution_id, &verdict, &execution.artifacts)
        })
        .collect();
    let check = CheckReceipt {
        execution_id,
        check_id: format!("tla/{config_name}#Spec"),
        evidence_ids,
        completion: verdict.completion(),
        observations: observations(&execution, &symbols, configured.len()),
        simulator_liveness: None,
        duration_ms: execution.duration_ms,
        peak_rss_kib: execution.peak_rss_kib,
        artifacts: execution.artifacts.clone(),
    };
    source::verify(&source)?;
    process::ensure_total_deadline(profile, "tla", "TLA receipt construction", false)?;
    Ok(ResultBundle {
        schema_version: RESULT_SCHEMA_VERSION,
        runner: "tla".to_owned(),
        profile: profile.to_owned(),
        source_ref: source.commit.clone(),
        execution: ExecutionReceipt {
            plan: context.plan.clone(),
            invocation: context.invocation.clone(),
            producer: context.producer.clone(),
            source,
            checks: vec![check],
            duration_ms: execution.duration_ms,
            peak_rss_kib: execution.peak_rss_kib,
            artifacts: execution.artifacts,
        },
        results,
    })
}
