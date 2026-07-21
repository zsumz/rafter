//! Verifier-owned planning and execution of registered detector fixtures.

mod artifact;
mod assessment;
mod compiler;
mod deadlines;
mod execution;
#[cfg(unix)]
mod fixture;
mod metadata;
mod model;
mod plan;
mod process;
mod result;
mod toolchain;
mod workspace;

pub(crate) use model::{DetectorReplayPlan, ReplayEvidence, ReplayFixture, ReplayTarget};
#[cfg(test)]
pub(crate) use plan::prepare;
pub(crate) use plan::{prepare_bounded, required_evidence};

use std::error::Error;

use crate::{
    contract::profile::DetectorReplayContract, verification::source::AuthenticatedCompilationSource,
};

#[cfg(test)]
pub(in crate::verification) use artifact::canonical_report_value;
pub(in crate::verification) use artifact::ReplayArtifactGuard;
pub(in crate::verification) use artifact::{validate_report_bundle, ReplayReportExpectation};
pub(in crate::verification) use assessment::DetectorReplayAssessment;
pub(in crate::verification) use assessment::EvidenceReplayQualification;
pub(in crate::verification) use deadlines::ReplayDeadlines;

pub(in crate::verification) struct PreparationFailureRequest<'a> {
    pub(in crate::verification) inventory: Vec<model::ReplayEvidence>,
    pub(in crate::verification) replay: Option<&'a DetectorReplayPlan>,
    pub(in crate::verification) receipts: crate::verification::source::ReplaySourceReceipts,
    pub(in crate::verification) contract: &'a DetectorReplayContract,
    pub(in crate::verification) profile: &'a str,
    pub(in crate::verification) source_ref: &'a str,
    pub(in crate::verification) registry: Option<crate::verification::source::RegistryReceipt>,
    pub(in crate::verification) message: &'a str,
    pub(in crate::verification) deadlines: ReplayDeadlines,
}

pub(in crate::verification) fn execute(
    replay: &DetectorReplayPlan,
    source: &AuthenticatedCompilationSource<'_>,
    contract: &DetectorReplayContract,
    profile: &str,
    source_ref: &str,
    deadlines: ReplayDeadlines,
) -> Result<DetectorReplayAssessment, Box<dyn Error>> {
    #[cfg(unix)]
    let attempt = if cfg!(target_os = "linux") {
        match execution::compile(
            replay,
            source,
            contract,
            profile,
            source_ref,
            deadlines.work(),
        ) {
            Ok(compilation) => {
                let fixtures =
                    fixture::execute(replay, &compilation, source, contract, deadlines.work());
                result::DetectorReplayAttempt::Completed(Box::new(result::DetectorReplayRun {
                    compilation,
                    fixtures,
                }))
            }
            Err(failure) => result::DetectorReplayAttempt::CompilationFailed(failure),
        }
    } else {
        result::DetectorReplayAttempt::CompilationFailed(Box::new(
            execution::CompilationFailure::unsupported_platform(),
        ))
    };
    #[cfg(not(unix))]
    let attempt = result::DetectorReplayAttempt::CompilationFailed(Box::new(
        execution::CompilationFailure::unsupported_platform(),
    ));
    artifact::publish_attempt(
        replay,
        attempt,
        source,
        contract,
        profile,
        source_ref,
        deadlines.publication(),
    )
}

pub(in crate::verification) fn deadlines(
    contract: &DetectorReplayContract,
) -> Result<ReplayDeadlines, String> {
    ReplayDeadlines::from_contract(contract)
}

pub(in crate::verification) fn publish_preparation_failure(
    request: PreparationFailureRequest<'_>,
) -> Result<DetectorReplayAssessment, Box<dyn Error>> {
    artifact::publish_preparation_failure(request)
}

pub(in crate::verification) fn qualification_failure(
    inventory: Vec<model::ReplayEvidence>,
    message: &str,
    artifacts: Vec<crate::evidence::ArtifactRef>,
) -> Result<DetectorReplayAssessment, String> {
    DetectorReplayAssessment::harness_error(inventory, message, artifacts)
}

#[cfg(test)]
mod tests;
