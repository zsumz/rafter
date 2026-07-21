//! Checkpoint layout, compatibility contract, and preparation state.

use std::{
    collections::BTreeMap,
    error::Error,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::evidence::format::tla::checkpoint::{CheckpointContract, RecoveryReport};
use crate::{evidence::ArtifactRef, execution::filesystem::HeldDirectory};

pub(super) const CONTRACT_FILE: &str = "checkpoint-contract.json";
pub(super) const INVENTORY_FILE: &str = "checkpoint-inventory.json";
pub(super) const CACHE_VALID_FILE: &str = "CACHE_VALID";
pub(super) const HASH_BUFFER_BYTES: usize = 1024 * 1024;
pub(super) const MAX_CHECKPOINT_METADATA_BYTES: u64 = 64 * 1024 * 1024;
pub(crate) const INPUT_KINDS: [&str; 10] = [
    "tla-tool",
    "tla-spec",
    "tla-trace-spec",
    "tla-detector-spec",
    "tla-runner",
    "tla-tool-asset-id",
    "tla-tool-checksums",
    "tla-config",
    "tla-trace-config",
    "tla-detector-config",
];

pub(in crate::producer) struct Preparation {
    pub(super) state_dir: PathBuf,
    pub(in crate::producer::tla) state_handle: Option<HeldDirectory>,
    pub(in crate::producer::tla) recover_handle: Option<HeldDirectory>,
    pub(in crate::producer::tla) report: RecoveryReport,
    pub(in crate::producer::tla) error: Option<String>,
    pub(super) artifacts: Vec<ArtifactRef>,
    pub(super) contract: CheckpointContract,
    pub(super) root: PathBuf,
    pub(super) namespace: PathBuf,
}

pub(super) struct CheckpointLayout {
    pub(super) root: PathBuf,
    pub(super) state_dir: PathBuf,
    pub(super) contract_path: PathBuf,
    pub(super) inventory_path: PathBuf,
    pub(super) namespace: PathBuf,
}

impl CheckpointLayout {
    pub(super) fn new(profile: &str, source_ref: &str) -> Self {
        let root = Path::new("target/rafter-invariants/tla-checkpoint").join(profile);
        let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
        Self {
            state_dir: root.join("states"),
            contract_path: root.join(CONTRACT_FILE),
            inventory_path: root.join(INVENTORY_FILE),
            namespace: Path::new(&format!("{profile}-tla/{source_prefix}/checkpoint"))
                .to_path_buf(),
            root,
        }
    }
}

pub(super) struct CandidateRecovery {
    pub(super) candidate_present: bool,
    pub(super) recover_from: Option<PathBuf>,
    pub(super) error: Option<String>,
    pub(super) artifacts: Vec<ArtifactRef>,
}

pub(in crate::producer::tla) fn enabled(configuration: &BTreeMap<String, String>) -> bool {
    configuration.contains_key("checkpoint_minutes")
}

pub(super) fn expected_contract(
    profile: &str,
    configuration: &BTreeMap<String, String>,
    artifacts: &[ArtifactRef],
) -> Result<CheckpointContract, Box<dyn Error>> {
    let mut input_sha256 = BTreeMap::new();
    for kind in INPUT_KINDS {
        let matching = artifacts
            .iter()
            .filter(|artifact| artifact.kind == kind)
            .collect::<Vec<_>>();
        let [artifact] = matching.as_slice() else {
            return Err(format!("checkpoint contract requires exactly one {kind} artifact").into());
        };
        input_sha256.insert(kind.to_owned(), artifact.sha256.clone());
    }
    let runner_contract_sha256 =
        format!("{:x}", Sha256::digest(serde_json::to_vec(configuration)?));
    Ok(CheckpointContract {
        schema_version: 1,
        profile: profile.to_owned(),
        config: configuration
            .get("config")
            .ok_or("checkpoint contract omitted config")?
            .clone(),
        runner_contract_sha256,
        input_sha256,
    })
}
