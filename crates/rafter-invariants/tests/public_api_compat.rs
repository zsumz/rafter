//! Scenario: milestone refactors preserve the flat public contract facade.

use std::path::Path;

use rafter_invariants::{
    render_registry_markdown, validate_detector_fixture_sources, ArtifactRef, Catalog,
    CatalogError, CheckCompletion, CheckReceipt, ClauseDescriptor, ClauseVerdict,
    DetectorFixtureSourceBinding, EvidenceDescriptor, EvidenceResult, EvidenceStatus,
    ExecutionPlanReceipt, ExecutionReceipt, FailureClassification, InvariantDescriptor,
    InvariantVerdict, InvocationReceipt, PlanInput, ProducerBindingReceipt, ProfileContract,
    ProfileManifest, RegistryClause, RegistryCounts, RegistryDocument, RegistryEvidence,
    RegistryInvariant, RegistryParseError, RunnerContract, SimulatorCheckContract,
    SimulatorIdentity, SourceMaterializationReceipt, SourceReceipt, TestIdentity, ToolReceipt,
    VerdictReport, VerdictStatus, PLAN_SCHEMA_VERSION, REGISTRY_SCHEMA_VERSION,
};

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
    let _: for<'a> fn(&DetectorFixtureSourceBinding<'a>) -> Result<(), String> =
        validate_detector_fixture_sources;
    let _: Option<ClauseDescriptor> = None;
    let _: Option<EvidenceDescriptor> = None;
    let _: Option<ProfileContract> = None;
    let _: Option<RunnerContract> = None;
    let _: Option<SimulatorCheckContract> = None;
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
