use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ArtifactRef;

use super::artifact;

pub(crate) const CONTRACT_KIND: &str = "tla-checkpoint-contract";
pub(crate) const INVENTORY_KIND: &str = "tla-checkpoint-inventory";
pub(crate) const RECOVERED_CONTRACT_KIND: &str = "tla-checkpoint-recovered-contract";
pub(crate) const RECOVERED_INVENTORY_KIND: &str = "tla-checkpoint-recovered-inventory";
pub(crate) const RECOVERY_REPORT_KIND: &str = "tla-checkpoint-recovery-report";

const CONTRACT_FILE: &str = "checkpoint-contract.json";
const INVENTORY_FILE: &str = "checkpoint-inventory.json";
const CACHE_VALID_FILE: &str = "CACHE_VALID";
const INPUT_KINDS: [&str; 10] = [
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointContract {
    pub schema_version: u32,
    pub profile: String,
    pub config: String,
    pub runner_contract_sha256: String,
    pub input_sha256: BTreeMap<String, String>,
}

impl CheckpointContract {
    pub(crate) fn sha256(&self) -> Result<String, serde_json::Error> {
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointInventory {
    pub schema_version: u32,
    pub contract_sha256: String,
    pub latest_checkpoint: Option<String>,
    pub files: Vec<CheckpointFile>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointFile {
    pub path: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryStatus {
    Fresh,
    Compatible,
    Incompatible,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryReport {
    pub schema_version: u32,
    pub status: RecoveryStatus,
    pub contract_sha256: String,
    pub candidate_present: bool,
    pub recovery_attempted: bool,
    pub recovered_checkpoint: Option<String>,
    pub error: Option<String>,
}

pub(super) struct Preparation {
    pub(super) state_dir: PathBuf,
    pub(super) recover_from: Option<PathBuf>,
    pub(super) report: RecoveryReport,
    pub(super) error: Option<String>,
    pub(super) artifacts: Vec<ArtifactRef>,
    contract: CheckpointContract,
    root: PathBuf,
    namespace: PathBuf,
}

pub(super) fn enabled(configuration: &BTreeMap<String, String>) -> bool {
    configuration.contains_key("checkpoint_minutes")
}

pub(crate) fn expected_contract(
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

pub(super) fn prepare(
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    source_artifacts: &[ArtifactRef],
    output_dir: &Path,
) -> Result<Preparation, Box<dyn Error>> {
    let contract = expected_contract(profile, configuration, source_artifacts)?;
    let contract_sha256 = contract.sha256()?;
    let root = Path::new("target/rafter-invariants/tla-checkpoint").join(profile);
    let state_dir = root.join("states");
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let namespace = Path::new(&format!("{profile}-tla/{source_prefix}/checkpoint")).to_path_buf();
    fs::create_dir_all(&root)?;
    remove_file_if_present(&root.join(CACHE_VALID_FILE))?;

    let contract_path = root.join(CONTRACT_FILE);
    let inventory_path = root.join(INVENTORY_FILE);
    let candidate_present =
        contract_path.exists() || inventory_path.exists() || directory_has_entries(&state_dir)?;
    let mut artifacts = Vec::new();
    if candidate_present {
        preserve_if_regular(
            &contract_path,
            output_dir,
            &namespace,
            RECOVERED_CONTRACT_KIND,
            &mut artifacts,
        )?;
        preserve_if_regular(
            &inventory_path,
            output_dir,
            &namespace,
            RECOVERED_INVENTORY_KIND,
            &mut artifacts,
        )?;
    }

    let compatibility = if candidate_present {
        validate_candidate(&contract_path, &inventory_path, &state_dir, &contract)
    } else {
        Ok(None)
    };
    let (status, recover_from, error) = match compatibility {
        Ok(Some(checkpoint)) => (
            RecoveryStatus::Compatible,
            Some(state_dir.join(&checkpoint)),
            None,
        ),
        Ok(None) => (RecoveryStatus::Fresh, None, None),
        Err(error) => (RecoveryStatus::Incompatible, None, Some(error)),
    };
    let report = RecoveryReport {
        schema_version: 1,
        status,
        contract_sha256: contract_sha256.clone(),
        candidate_present,
        recovery_attempted: recover_from.is_some(),
        recovered_checkpoint: recover_from
            .as_ref()
            .and_then(|path| path.file_name())
            .map(|name| name.to_string_lossy().into_owned()),
        error: error.clone(),
    };
    artifacts.push(write_json_artifact(
        output_dir,
        &namespace,
        RECOVERY_REPORT_KIND,
        &report,
    )?);

    if error.is_some() {
        sanitize_cache_root(&root)?;
        write_cache_valid_marker(&root, "clean", &contract_sha256)?;
    } else {
        fs::create_dir_all(&state_dir)?;
        write_json_atomic(&contract_path, &contract)?;
    }

    Ok(Preparation {
        state_dir,
        recover_from,
        report,
        error,
        artifacts,
        contract,
        root,
        namespace,
    })
}

impl Preparation {
    pub(super) fn finish(mut self, output_dir: &Path) -> Result<Vec<ArtifactRef>, Box<dyn Error>> {
        if self.error.is_some() {
            return Ok(self.artifacts);
        }
        let contract_sha256 = self.contract.sha256()?;
        prune_to_latest(&self.state_dir)?;
        let inventory = inventory(&self.state_dir, &contract_sha256)?;
        let contract_path = self.root.join(CONTRACT_FILE);
        let inventory_path = self.root.join(INVENTORY_FILE);
        write_json_atomic(&contract_path, &self.contract)?;
        write_json_atomic(&inventory_path, &inventory)?;
        self.artifacts.push(artifact::capture(
            output_dir,
            &self.namespace,
            &contract_path,
            CONTRACT_KIND,
        )?);
        self.artifacts.push(artifact::capture(
            output_dir,
            &self.namespace,
            &inventory_path,
            INVENTORY_KIND,
        )?);
        write_cache_valid_marker(&self.root, "checkpoint", &contract_sha256)?;
        Ok(self.artifacts)
    }
}

fn sanitize_cache_root(root: &Path) -> Result<(), Box<dyn Error>> {
    if root.exists() {
        fs::remove_dir_all(root)?;
    }
    fs::create_dir_all(root)?;
    Ok(())
}

fn remove_file_if_present(path: &Path) -> Result<(), Box<dyn Error>> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn write_cache_valid_marker(
    root: &Path,
    state: &str,
    contract_sha256: &str,
) -> Result<(), Box<dyn Error>> {
    let path = root.join(CACHE_VALID_FILE);
    let temporary = root.join(format!("{CACHE_VALID_FILE}.tmp-{}", std::process::id()));
    fs::write(
        &temporary,
        format!("schema_version=1\nstate={state}\ncontract_sha256={contract_sha256}\n"),
    )?;
    fs::rename(temporary, path)?;
    Ok(())
}

fn prune_to_latest(state_dir: &Path) -> Result<(), Box<dyn Error>> {
    if !state_dir.exists() {
        return Ok(());
    }
    let mut directories = Vec::new();
    for entry in fs::read_dir(state_dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() && directory_contains_file(&entry.path())? {
            if checkpoint_directory_is_complete(&entry.path())? {
                directories.push(entry);
            } else {
                fs::remove_dir_all(entry.path())?;
            }
        }
    }
    directories.sort_by_key(fs::DirEntry::file_name);
    if let Some(latest) = directories.pop() {
        let latest = latest.path();
        for directory in directories {
            if directory.path() != latest {
                fs::remove_dir_all(directory.path())?;
            }
        }
    }
    Ok(())
}

fn directory_contains_file(directory: &Path) -> Result<bool, Box<dyn Error>> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        if file_type.is_file() || (file_type.is_dir() && directory_contains_file(&entry.path())?) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn checkpoint_directory_is_complete(directory: &Path) -> Result<bool, Box<dyn Error>> {
    let mut has_committed_checkpoint = false;
    let mut has_temporary_checkpoint = false;
    collect_checkpoint_markers(
        directory,
        &mut has_committed_checkpoint,
        &mut has_temporary_checkpoint,
    )?;
    Ok(has_committed_checkpoint && !has_temporary_checkpoint)
}

fn collect_checkpoint_markers(
    directory: &Path,
    has_committed_checkpoint: &mut bool,
    has_temporary_checkpoint: &mut bool,
) -> Result<(), Box<dyn Error>> {
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_symlink() {
            return Err(format!("checkpoint directory rejects symlink {}", path.display()).into());
        }
        if file_type.is_dir() {
            collect_checkpoint_markers(&path, has_committed_checkpoint, has_temporary_checkpoint)?;
        } else if file_type.is_file() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if has_tlc_extension(&name, "tmp") {
                *has_temporary_checkpoint = true;
            }
            *has_committed_checkpoint |= has_tlc_extension(&name, "chkpt");
        } else {
            return Err(format!("checkpoint directory rejects {}", path.display()).into());
        }
    }
    Ok(())
}

fn has_tlc_extension(name: &str, expected: &str) -> bool {
    let path = Path::new(name);
    path.extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
        || path
            .file_stem()
            .and_then(|stem| Path::new(stem).extension())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
}

fn validate_candidate(
    contract_path: &Path,
    inventory_path: &Path,
    state_dir: &Path,
    expected: &CheckpointContract,
) -> Result<Option<String>, String> {
    let observed_contract: CheckpointContract = read_json(contract_path, "contract")?;
    if &observed_contract != expected {
        return Err("restored checkpoint contract is stale or incompatible".to_owned());
    }
    let observed_inventory: CheckpointInventory = read_json(inventory_path, "inventory")?;
    let expected_inventory = inventory(
        state_dir,
        &expected
            .sha256()
            .map_err(|error| format!("digest checkpoint contract: {error}"))?,
    )
    .map_err(|error| format!("inventory restored checkpoint: {error}"))?;
    if observed_inventory != expected_inventory {
        return Err("restored checkpoint inventory is malformed or stale".to_owned());
    }
    Ok(observed_inventory.latest_checkpoint)
}

fn inventory(
    state_dir: &Path,
    contract_sha256: &str,
) -> Result<CheckpointInventory, Box<dyn Error>> {
    let mut files = Vec::new();
    if state_dir.exists() {
        for entry in fs::read_dir(state_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir()
                && directory_contains_file(&entry.path())?
                && !checkpoint_directory_is_complete(&entry.path())?
            {
                return Err(format!(
                    "checkpoint run directory is incomplete: {}",
                    entry.path().display()
                )
                .into());
            }
        }
        collect_files(state_dir, state_dir, &mut files)?;
    }
    files.sort();
    let mut top_level = files
        .iter()
        .filter_map(|file| {
            file.path
                .split_once('/')
                .map(|(directory, _)| directory.to_owned())
        })
        .collect::<Vec<_>>();
    top_level.sort();
    top_level.dedup();
    let latest_checkpoint = top_level.last().cloned();
    Ok(CheckpointInventory {
        schema_version: 1,
        contract_sha256: contract_sha256.to_owned(),
        latest_checkpoint,
        files,
    })
}

fn collect_files(
    root: &Path,
    directory: &Path,
    files: &mut Vec<CheckpointFile>,
) -> Result<(), Box<dyn Error>> {
    let mut entries = fs::read_dir(directory)?.collect::<Result<Vec<_>, _>>()?;
    entries.sort_by_key(fs::DirEntry::file_name);
    for entry in entries {
        let file_type = entry.file_type()?;
        let path = entry.path();
        if file_type.is_symlink() {
            return Err(format!("checkpoint inventory rejects symlink {}", path.display()).into());
        }
        if file_type.is_dir() {
            collect_files(root, &path, files)?;
        } else if file_type.is_file() {
            let bytes = fs::read(&path)?;
            let relative = path
                .strip_prefix(root)?
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            if !relative.contains('/') {
                return Err("checkpoint state file is not inside a TLC run directory".into());
            }
            files.push(CheckpointFile {
                path: relative,
                sha256: format!("{:x}", Sha256::digest(&bytes)),
                size_bytes: bytes.len() as u64,
            });
        } else {
            return Err(format!("checkpoint inventory rejects {}", path.display()).into());
        }
    }
    Ok(())
}

fn directory_has_entries(path: &Path) -> Result<bool, Box<dyn Error>> {
    if !path.exists() {
        return Ok(false);
    }
    Ok(fs::read_dir(path)?.next().transpose()?.is_some())
}

fn preserve_if_regular(
    source: &Path,
    output_dir: &Path,
    namespace: &Path,
    kind: &str,
    artifacts: &mut Vec<ArtifactRef>,
) -> Result<(), Box<dyn Error>> {
    if source.is_file() {
        artifacts.push(artifact::write(
            output_dir,
            &namespace.join(kind),
            kind,
            &fs::read(source)?,
        )?);
    }
    Ok(())
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path, label: &str) -> Result<T, String> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("inspect checkpoint {label}: {error}"))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(format!("checkpoint {label} is not a regular file"));
    }
    let bytes = fs::read(path).map_err(|error| format!("read checkpoint {label}: {error}"))?;
    serde_json::from_slice(&bytes).map_err(|error| format!("parse checkpoint {label}: {error}"))
}

fn write_json_artifact<T: Serialize>(
    output_dir: &Path,
    namespace: &Path,
    kind: &str,
    value: &T,
) -> Result<ArtifactRef, Box<dyn Error>> {
    artifact::write(
        output_dir,
        &namespace.join(format!("{kind}.json")),
        kind,
        &serde_json::to_vec_pretty(value)?,
    )
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<(), Box<dyn Error>> {
    let temporary = path.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(&temporary, serde_json::to_vec_pretty(value)?)?;
    fs::rename(temporary, path)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        expected_contract, inventory, prepare, prune_to_latest, validate_candidate,
        CheckpointContract, CheckpointInventory, RecoveryStatus, CACHE_VALID_FILE, INPUT_KINDS,
        RECOVERED_CONTRACT_KIND, RECOVERED_INVENTORY_KIND, RECOVERY_REPORT_KIND,
    };
    use crate::ArtifactRef;
    use std::{collections::BTreeMap, fs, path::Path};

    #[test]
    fn inventory_detects_changed_checkpoint_bytes() {
        let root = std::env::temp_dir().join(format!(
            "rafter-checkpoint-inventory-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(root.join("26-07-12-00-00-00.000")).expect("create checkpoint");
        fs::write(root.join("26-07-12-00-00-00.000/states_0.chkpt"), b"first")
            .expect("write checkpoint");
        let first = inventory(&root, &"1".repeat(64)).expect("inventory checkpoint");
        fs::write(root.join("26-07-12-00-00-00.000/states_0.chkpt"), b"second")
            .expect("mutate checkpoint");
        let second = inventory(&root, &"1".repeat(64)).expect("inventory checkpoint");
        assert_ne!(first, second);
        assert_eq!(
            first.latest_checkpoint.as_deref(),
            Some("26-07-12-00-00-00.000")
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn incomplete_new_checkpoint_is_discarded_before_inventory() {
        let root =
            std::env::temp_dir().join(format!("rafter-checkpoint-partial-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let complete = root.join("26-07-12-00-00-00.000");
        let partial = root.join("26-07-12-01-00-00.000");
        fs::create_dir_all(&complete).expect("create complete checkpoint");
        fs::create_dir_all(&partial).expect("create partial checkpoint");
        fs::write(complete.join("queue.chkpt"), b"complete").expect("write checkpoint");
        fs::write(partial.join("queue.chkpt"), b"old").expect("write old checkpoint");
        fs::write(partial.join("queue.tmp"), b"partial").expect("write partial checkpoint");

        assert!(inventory(&root, &"1".repeat(64)).is_err());
        prune_to_latest(&root).expect("prune partial checkpoint");
        let retained = inventory(&root, &"1".repeat(64)).expect("inventory retained checkpoint");
        assert_eq!(
            retained.latest_checkpoint.as_deref(),
            Some("26-07-12-00-00-00.000")
        );
        assert!(!partial.exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn checkpoint_json_rejects_unknown_fields() {
        let source = r#"{
          "schema_version": 1,
          "contract_sha256": "digest",
          "latest_checkpoint": null,
          "files": [],
          "trusted": true
        }"#;
        assert!(serde_json::from_str::<CheckpointInventory>(source).is_err());

        let contract = CheckpointContract {
            schema_version: 1,
            profile: "weekly".to_owned(),
            config: "Raft.cfg".to_owned(),
            runner_contract_sha256: "2".repeat(64),
            input_sha256: BTreeMap::new(),
        };
        assert_eq!(contract.sha256().expect("digest").len(), 64);
    }

    #[test]
    fn stale_candidate_is_rejected_without_deleting_usable_state() {
        let root =
            std::env::temp_dir().join(format!("rafter-checkpoint-stale-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let states = root.join("states");
        let run = states.join("26-07-12-00-00-00.000");
        fs::create_dir_all(&run).expect("create checkpoint");
        let state = run.join("states_0.chkpt");
        fs::write(&state, b"usable").expect("write checkpoint state");
        let expected = CheckpointContract {
            schema_version: 1,
            profile: "weekly".to_owned(),
            config: "Raft.cfg".to_owned(),
            runner_contract_sha256: "1".repeat(64),
            input_sha256: BTreeMap::new(),
        };
        let mut stale_contract = expected.clone();
        stale_contract.runner_contract_sha256 = "2".repeat(64);
        fs::write(
            root.join("checkpoint-contract.json"),
            serde_json::to_vec_pretty(&stale_contract).expect("serialize contract"),
        )
        .expect("write contract");
        let stale_inventory =
            inventory(&states, &stale_contract.sha256().expect("digest")).expect("inventory state");
        fs::write(
            root.join("checkpoint-inventory.json"),
            serde_json::to_vec_pretty(&stale_inventory).expect("serialize inventory"),
        )
        .expect("write inventory");

        assert!(validate_candidate(
            &root.join("checkpoint-contract.json"),
            &root.join("checkpoint-inventory.json"),
            &states,
            &expected,
        )
        .is_err());
        assert_eq!(fs::read(&state).expect("state remains"), b"usable");
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn incompatible_candidate_is_sanitized_before_the_next_prepare() {
        let profile = format!("checkpoint-self-heal-{}", std::process::id());
        let root = Path::new("target/rafter-invariants/tla-checkpoint").join(&profile);
        let output_dir =
            Path::new("target/rafter-invariants/checkpoint-test-artifacts").join(&profile);
        let _ = fs::remove_dir_all(&root);
        let _ = fs::remove_dir_all(&output_dir);

        let configuration = BTreeMap::from([
            ("config".to_owned(), "Raft.cfg".to_owned()),
            ("checkpoint_minutes".to_owned(), "30".to_owned()),
        ]);
        let source_artifacts = INPUT_KINDS
            .into_iter()
            .map(|kind| ArtifactRef {
                kind: kind.to_owned(),
                path: format!("test-inputs/{kind}"),
                sha256: format!("{:0>64}", kind.len()),
                size_bytes: 1,
            })
            .collect::<Vec<_>>();
        let expected = expected_contract(&profile, &configuration, &source_artifacts)
            .expect("derive expected contract");
        let mut stale = expected.clone();
        stale.runner_contract_sha256 = "f".repeat(64);
        let states = root.join("states");
        let run = states.join("26-07-12-00-00-00.000");
        fs::create_dir_all(&run).expect("create stale checkpoint");
        fs::write(run.join("states_0.chkpt"), b"poison").expect("write stale checkpoint");
        fs::write(
            root.join("checkpoint-contract.json"),
            serde_json::to_vec_pretty(&stale).expect("serialize stale contract"),
        )
        .expect("write stale contract");
        let stale_inventory = inventory(&states, &stale.sha256().expect("digest stale contract"))
            .expect("inventory stale checkpoint");
        fs::write(
            root.join("checkpoint-inventory.json"),
            serde_json::to_vec_pretty(&stale_inventory).expect("serialize stale inventory"),
        )
        .expect("write stale inventory");
        fs::write(root.join(CACHE_VALID_FILE), b"stale marker")
            .expect("write stale validity marker");

        let first = prepare(
            &profile,
            "1c642bc4fe001234567890123456789012345678",
            &configuration,
            &source_artifacts,
            &output_dir,
        )
        .expect("reject and sanitize stale checkpoint");
        assert_eq!(first.report.status, RecoveryStatus::Incompatible);
        assert!(first.error.is_some());
        assert!(!states.exists());
        assert!(!root.join("checkpoint-contract.json").exists());
        assert!(!root.join("checkpoint-inventory.json").exists());
        assert!(root.join(CACHE_VALID_FILE).is_file());
        let diagnostic_kinds = first
            .finish(&output_dir)
            .expect("finish incompatible preparation")
            .into_iter()
            .map(|artifact| artifact.kind)
            .collect::<Vec<_>>();
        assert!(diagnostic_kinds.contains(&RECOVERED_CONTRACT_KIND.to_owned()));
        assert!(diagnostic_kinds.contains(&RECOVERED_INVENTORY_KIND.to_owned()));
        assert!(diagnostic_kinds.contains(&RECOVERY_REPORT_KIND.to_owned()));

        let second = prepare(
            &profile,
            "1c642bc4fe001234567890123456789012345678",
            &configuration,
            &source_artifacts,
            &output_dir,
        )
        .expect("prepare clean replacement checkpoint");
        assert_eq!(second.report.status, RecoveryStatus::Fresh);
        assert!(!second.report.candidate_present);
        assert!(second.error.is_none());
        second
            .finish(&output_dir)
            .expect("finish clean preparation");
        assert!(root.join(CACHE_VALID_FILE).is_file());

        let _ = fs::remove_dir_all(root);
        let _ = fs::remove_dir_all(output_dir);
    }
}
