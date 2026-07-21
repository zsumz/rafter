//! End-to-end Maelstrom verifier fixtures preserved under their stable test identity.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    process::Command,
};

use crate::{
    ArtifactRef, CheckCompletion, CheckReceipt, EvidenceResult, EvidenceStatus,
    ExecutionPlanReceipt, ExecutionReceipt, FailureClassification, InvocationReceipt, PlanInput,
    ProfileContract, ResultBundle, RunnerContract, SourceMaterializationReceipt, SourceReceipt,
    ToolReceipt, PLAN_SCHEMA_VERSION,
};
use sha2::{Digest, Sha256};

const VALID: &str = r"{
  :stats {:count 9 :ok-count 6 :by-f {
    :read {:ok-count 2} :write {:ok-count 3} :cas {:ok-count 1}}}
  :workload {:valid? true :failures [] :results {
    0 {:linearizable {:valid? true}}}}
  :valid? true}";

include!("full_bundle/scenarios.inc");
include!("full_bundle/serialized_fixture.inc");
include!("full_bundle/bundle_fixture.inc");
