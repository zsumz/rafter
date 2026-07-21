//! Scenario: milestone refactors preserve the flat public contract facade.

use std::path::Path;

use rafter_invariants::{
    publish_verifier_archive, render_registry_markdown, validate_detector_fixture_sources,
    verify_report_set, verify_verifier_archive, ArtifactRef, Catalog, CatalogError,
    CheckCompletion, CheckReceipt, ClauseDescriptor, ClausePolicy, ClauseVerdict,
    DetectorFixtureAnalysis, DetectorFixtureSourceBatch, DetectorFixtureSourceBinding,
    DetectorReplayArtifactPolicy, DetectorReplayBuild, DetectorReplayChallenge,
    DetectorReplayContract, DetectorReplayFixtureInventory, DetectorReplayPolicy,
    DetectorReplaySource, DetectorReplayTargetDirectory, EvidenceDescriptor, EvidenceLayer,
    EvidencePolicy, EvidenceResult, EvidenceStatus, EvidenceStrength, ExecutableReceipt,
    ExecutionPlanReceipt, ExecutionReceipt, FailureClassification, InvariantDescriptor,
    InvariantVerdict, InvocationReceipt, PlanInput, ProducerBindingReceipt, ProfileContract,
    ProfileManifest, RegistryClause, RegistryCounts, RegistryDocument, RegistryEvidence,
    RegistryInvariant, RegistryParseError, RequiredClauseStrength, RunnerContract,
    SimulatorCheckContract, SimulatorIdentity, SourceMaterializationReceipt, SourceReceipt,
    TestIdentity, ToolReceipt, VerdictReport, VerdictStatus, VerifierArchiveExpectation,
    VerifierContract, PLAN_SCHEMA_VERSION, REGISTRY_SCHEMA_VERSION,
};

type PublicationError = Box<dyn std::error::Error>;
type PublishVerifierArchive =
    fn(&Path, &Path, &str, &Path, &VerifierArchiveExpectation) -> Result<String, PublicationError>;
type VerifyVerifierArchive =
    fn(&Path, &str, &str, &VerifierArchiveExpectation) -> Result<(), PublicationError>;
type VerifyReportSet = fn(&Path, &str, &Catalog, &ProfileManifest) -> Result<(), PublicationError>;

#[test]
fn current_contract_types_remain_available_from_the_crate_root() {
    fn registry_parse(source: &str) -> Result<RegistryDocument, CatalogError> {
        RegistryDocument::parse(source)
    }
    fn registry_parse_strict(source: &str) -> Result<RegistryDocument, RegistryParseError> {
        RegistryDocument::parse_strict(source)
    }
    fn registry_load(path: &Path) -> Result<RegistryDocument, CatalogError> {
        RegistryDocument::load(path)
    }
    fn registry_load_strict(path: &Path) -> Result<RegistryDocument, RegistryParseError> {
        RegistryDocument::load_strict(path)
    }
    fn profile_load(path: &Path) -> Result<ProfileManifest, CatalogError> {
        ProfileManifest::load(path)
    }
    fn catalog_load(path: &Path) -> Result<Catalog, CatalogError> {
        Catalog::load(path)
    }

    let _ = registry_parse as fn(&str) -> Result<RegistryDocument, CatalogError>;
    let _ = registry_parse_strict as fn(&str) -> Result<RegistryDocument, RegistryParseError>;
    let _ = registry_load as fn(&Path) -> Result<RegistryDocument, CatalogError>;
    let _ = registry_load_strict as fn(&Path) -> Result<RegistryDocument, RegistryParseError>;
    let _ = profile_load as fn(&Path) -> Result<ProfileManifest, CatalogError>;
    let _ = catalog_load as fn(&Path) -> Result<Catalog, CatalogError>;
    let _ = render_registry_markdown as fn(&RegistryDocument) -> String;
    let _: PublishVerifierArchive = publish_verifier_archive;
    let _: VerifyVerifierArchive = verify_verifier_archive;
    let _: fn(
        &Path,
        &str,
        &ProfileManifest,
    ) -> Result<VerifierArchiveExpectation, PublicationError> = VerifierArchiveExpectation::capture;
    let _: VerifyReportSet = verify_report_set;
    let _: for<'a> fn(&DetectorFixtureSourceBinding<'a>) -> Result<(), String> =
        validate_detector_fixture_sources;
    let _: for<'a> fn(
        &mut DetectorFixtureAnalysis,
        &DetectorFixtureSourceBinding<'a>,
    ) -> Result<(), String> = DetectorFixtureAnalysis::validate;
    let _: for<'a> fn(
        &mut DetectorFixtureSourceBatch,
        &DetectorFixtureSourceBinding<'a>,
    ) -> Result<(), String> = DetectorFixtureSourceBatch::validate;
    let _: Option<ClauseDescriptor> = None;
    let _: Option<ClausePolicy> = None;
    let _: Option<DetectorReplayArtifactPolicy> = None;
    let _: Option<DetectorReplayBuild> = None;
    let _: Option<DetectorReplayChallenge> = None;
    let _: Option<DetectorReplayContract> = None;
    let _: Option<DetectorReplayFixtureInventory> = None;
    let _: Option<DetectorReplayPolicy> = None;
    let _: Option<DetectorReplaySource> = None;
    let _: Option<DetectorReplayTargetDirectory> = None;
    let _: Option<EvidenceDescriptor> = None;
    let _: Option<EvidenceLayer> = None;
    let _: Option<EvidencePolicy> = None;
    let _: Option<EvidenceStrength> = None;
    let _: Option<ProfileContract> = None;
    let _: Option<RunnerContract> = None;
    let _: Option<RequiredClauseStrength> = None;
    let _: Option<SimulatorCheckContract> = None;
    let _: Option<VerifierContract> = None;
    let _: Option<SimulatorIdentity> = None;
    let _: Option<TestIdentity> = None;
    let _: Option<InvariantDescriptor> = None;
    let _: Option<RegistryClause> = None;
    let _: Option<RegistryCounts> = None;
    let _: Option<RegistryEvidence> = None;
    let _: Option<RegistryInvariant> = None;
    let _: Option<ArtifactRef> = None;
    let _: Option<CheckCompletion> = None;
    let _: Option<CheckReceipt> = None;
    let _: Option<EvidenceResult> = None;
    let _: Option<EvidenceStatus> = None;
    let _: Option<ExecutableReceipt> = None;
    let _: Option<ExecutionPlanReceipt> = None;
    let _: Option<ExecutionReceipt> = None;
    let _: Option<FailureClassification> = None;
    let _: Option<InvocationReceipt> = None;
    let _: Option<PlanInput> = None;
    let _: Option<ProducerBindingReceipt> = None;
    let _: Option<SourceMaterializationReceipt> = None;
    let _: Option<SourceReceipt> = None;
    let _: Option<ToolReceipt> = None;
    let _: Option<ClauseVerdict> = None;
    let _: Option<InvariantVerdict> = None;
    let _: Option<VerdictReport> = None;
    let _: Option<VerdictStatus> = None;
    let _: u32 = REGISTRY_SCHEMA_VERSION;
    let _: u32 = PLAN_SCHEMA_VERSION;
}
