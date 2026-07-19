use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    io::Read,
    path::{Path, PathBuf},
    time::Instant,
};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{execution::filesystem::HeldDirectory, ArtifactRef};

use super::artifact;

mod traversal;

use traversal::{
    directory_has_entries, entry_kind, path_entry_exists, remove_scanned_subtrees,
    sanitize_cache_root, scan_checkpoint_tree, CheckpointNodeKind, CheckpointTree, TraversalBudget,
    TraversalLimits, TRAVERSAL_LIMITS,
};
#[cfg(test)]
use traversal::{read_sorted_entries, sanitize_cache_root_with_limits};

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
const HASH_BUFFER_BYTES: usize = 1024 * 1024;
const MAX_CHECKPOINT_METADATA_BYTES: u64 = 64 * 1024 * 1024;

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
    pub(super) state_handle: Option<HeldDirectory>,
    pub(super) recover_handle: Option<HeldDirectory>,
    pub(super) report: RecoveryReport,
    pub(super) error: Option<String>,
    pub(super) artifacts: Vec<ArtifactRef>,
    contract: CheckpointContract,
    root: PathBuf,
    namespace: PathBuf,
}

struct CheckpointLayout {
    root: PathBuf,
    state_dir: PathBuf,
    contract_path: PathBuf,
    inventory_path: PathBuf,
    namespace: PathBuf,
}

impl CheckpointLayout {
    fn new(profile: &str, source_ref: &str) -> Self {
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

struct CandidateRecovery {
    candidate_present: bool,
    recover_from: Option<PathBuf>,
    error: Option<String>,
    artifacts: Vec<ArtifactRef>,
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
    deadline: Instant,
) -> Result<Preparation, Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint preparation")?;
    let contract = expected_contract(profile, configuration, source_artifacts)?;
    let contract_sha256 = contract.sha256()?;
    let layout = CheckpointLayout::new(profile, source_ref);
    let root_is_symlink = initialize_cache_root(&layout.root, deadline)?;
    let mut recovery =
        inspect_candidate(&layout, root_is_symlink, &contract, output_dir, deadline)?;
    let report = recovery_report(&recovery, contract_sha256.clone());
    recovery.artifacts.push(write_json_artifact(
        output_dir,
        &layout.namespace,
        RECOVERY_REPORT_KIND,
        &report,
    )?);
    ensure_deadline(deadline, "checkpoint recovery report capture")?;

    let state_handle = if recovery.error.is_some() {
        sanitize_cache_root(&layout.root, deadline)?;
        ensure_deadline(deadline, "checkpoint cache sanitization")?;
        write_cache_valid_marker(&layout.root, "clean", &contract_sha256)?;
        None
    } else {
        let state_handle = HeldDirectory::create_all(&layout.state_dir)?;
        write_json_atomic(&layout.contract_path, &contract)?;
        Some(state_handle)
    };
    let recover_handle = recovery
        .recover_from
        .as_deref()
        .map(HeldDirectory::open)
        .transpose()?;
    ensure_deadline(deadline, "checkpoint preparation finalization")?;

    Ok(Preparation {
        state_dir: layout.state_dir,
        state_handle,
        recover_handle,
        report,
        error: recovery.error,
        artifacts: recovery.artifacts,
        contract,
        root: layout.root,
        namespace: layout.namespace,
    })
}

fn inspect_candidate(
    layout: &CheckpointLayout,
    root_is_symlink: bool,
    contract: &CheckpointContract,
    output_dir: &Path,
    deadline: Instant,
) -> Result<CandidateRecovery, Box<dyn Error>> {
    let contract_present = !root_is_symlink && path_entry_exists(&layout.contract_path)?;
    let inventory_present = !root_is_symlink && path_entry_exists(&layout.inventory_path)?;
    let states_is_symlink =
        !root_is_symlink && entry_kind(&layout.state_dir)? == Some(CheckpointNodeKind::Symlink);
    let states_present = states_is_symlink
        || (!root_is_symlink && directory_has_entries(&layout.state_dir, deadline)?);
    let candidate_present =
        root_is_symlink || contract_present || inventory_present || states_present;
    let mut artifacts = Vec::new();
    if candidate_present && !root_is_symlink {
        preserve_if_regular(
            &layout.contract_path,
            output_dir,
            &layout.namespace,
            RECOVERED_CONTRACT_KIND,
            &mut artifacts,
            deadline,
        )?;
        preserve_if_regular(
            &layout.inventory_path,
            output_dir,
            &layout.namespace,
            RECOVERED_INVENTORY_KIND,
            &mut artifacts,
            deadline,
        )?;
    }

    let compatibility = if root_is_symlink {
        Err("restored checkpoint cache root is a symlink".to_owned())
    } else if states_is_symlink {
        Err("restored checkpoint states directory is a symlink".to_owned())
    } else if candidate_present {
        validate_candidate(
            &layout.contract_path,
            &layout.inventory_path,
            &layout.state_dir,
            contract,
            deadline,
        )?
    } else {
        Ok(None)
    };
    let (recover_from, error) = match compatibility {
        Ok(Some(checkpoint)) => (Some(layout.state_dir.join(checkpoint)), None),
        Ok(None) => (None, None),
        Err(error) => (None, Some(error)),
    };
    Ok(CandidateRecovery {
        candidate_present,
        recover_from,
        error,
        artifacts,
    })
}

fn recovery_report(recovery: &CandidateRecovery, contract_sha256: String) -> RecoveryReport {
    let status = if recovery.error.is_some() {
        RecoveryStatus::Incompatible
    } else if recovery.recover_from.is_some() {
        RecoveryStatus::Compatible
    } else {
        RecoveryStatus::Fresh
    };
    RecoveryReport {
        schema_version: 1,
        status,
        contract_sha256,
        candidate_present: recovery.candidate_present,
        recovery_attempted: recovery.recover_from.is_some(),
        recovered_checkpoint: recovery
            .recover_from
            .as_ref()
            .and_then(|path| path.file_name())
            .map(|name| name.to_string_lossy().into_owned()),
        error: recovery.error.clone(),
    }
}

impl Preparation {
    pub(super) fn abandon(self) -> Vec<ArtifactRef> {
        self.artifacts
    }

    pub(super) fn finish(
        mut self,
        output_dir: &Path,
        deadline: Instant,
    ) -> Result<Vec<ArtifactRef>, Box<dyn Error>> {
        ensure_deadline(deadline, "checkpoint finalization")?;
        if self.error.is_some() {
            return Ok(self.artifacts);
        }
        self.state_handle
            .as_ref()
            .ok_or("compatible checkpoint preparation omitted state handle")?
            .verify_path_binding()?;
        if let Some(recover_handle) = &self.recover_handle {
            recover_handle.verify_path_binding()?;
        }
        let contract_sha256 = self.contract.sha256()?;
        prune_to_latest(&self.state_dir, deadline)?;
        let inventory = inventory(&self.state_dir, &contract_sha256, deadline)?;
        let contract_path = self.root.join(CONTRACT_FILE);
        let inventory_path = self.root.join(INVENTORY_FILE);
        write_json_atomic(&contract_path, &self.contract)?;
        write_json_atomic(&inventory_path, &inventory)?;
        ensure_deadline(deadline, "checkpoint metadata publication")?;
        self.artifacts.push(artifact::capture(
            output_dir,
            &self.namespace,
            &contract_path,
            CONTRACT_KIND,
        )?);
        ensure_deadline(deadline, "checkpoint contract capture")?;
        self.artifacts.push(artifact::capture(
            output_dir,
            &self.namespace,
            &inventory_path,
            INVENTORY_KIND,
        )?);
        ensure_deadline(deadline, "checkpoint inventory capture")?;
        write_cache_valid_marker(&self.root, "checkpoint", &contract_sha256)?;
        ensure_deadline(deadline, "checkpoint validity publication")?;
        Ok(self.artifacts)
    }
}

#[derive(Debug)]
struct CheckpointDeadlineError {
    operation: String,
}

impl fmt::Display for CheckpointDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "TLA checkpoint deadline expired during {}",
            self.operation
        )
    }
}

impl Error for CheckpointDeadlineError {}

fn ensure_deadline(deadline: Instant, operation: &str) -> Result<(), Box<dyn Error>> {
    if Instant::now() >= deadline {
        return Err(Box::new(CheckpointDeadlineError {
            operation: operation.to_owned(),
        }));
    }
    Ok(())
}

fn initialize_cache_root(root: &Path, deadline: Instant) -> Result<bool, Box<dyn Error>> {
    let root_is_symlink = entry_kind(root)? == Some(CheckpointNodeKind::Symlink);
    if !root_is_symlink {
        let root_handle = HeldDirectory::create_all(root)?;
        root_handle.remove_file_if_exists(Path::new(CACHE_VALID_FILE))?;
    }
    ensure_deadline(deadline, "checkpoint cache initialization")?;
    Ok(root_is_symlink)
}

fn write_cache_valid_marker(
    root: &Path,
    state: &str,
    contract_sha256: &str,
) -> Result<(), Box<dyn Error>> {
    HeldDirectory::open(root)?.write_atomic(
        Path::new(CACHE_VALID_FILE),
        format!("schema_version=1\nstate={state}\ncontract_sha256={contract_sha256}\n").as_bytes(),
    )
}

fn prune_to_latest(state_dir: &Path, deadline: Instant) -> Result<(), Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint pruning")?;
    if !path_entry_exists(state_dir)? {
        return Ok(());
    }
    let mut budget = TraversalBudget::new(TRAVERSAL_LIMITS);
    let tree = scan_checkpoint_tree(
        state_dir,
        deadline,
        "checkpoint pruning",
        &mut budget,
        false,
    )?;
    let runs = checkpoint_runs(state_dir, &tree)?;
    let mut complete = runs
        .iter()
        .filter_map(|(path, markers)| markers.complete().then_some(path.clone()))
        .collect::<Vec<_>>();
    let mut remove = runs
        .iter()
        .filter_map(|(path, markers)| (!markers.complete()).then_some(path.clone()))
        .collect::<Vec<_>>();
    complete.sort();
    if let Some(latest) = complete.pop() {
        for directory in complete {
            if directory != latest {
                remove.push(directory);
            }
        }
    }
    remove.sort();
    remove.dedup();
    if !remove.is_empty() {
        remove_scanned_subtrees(&tree, &remove, deadline)?;
    }
    Ok(())
}

#[derive(Default)]
struct CheckpointMarkers {
    committed: bool,
    temporary: bool,
}

impl CheckpointMarkers {
    fn complete(&self) -> bool {
        self.committed && !self.temporary
    }
}

fn checkpoint_runs(
    state_dir: &Path,
    tree: &CheckpointTree,
) -> Result<BTreeMap<PathBuf, CheckpointMarkers>, Box<dyn Error>> {
    let mut runs = BTreeMap::<PathBuf, CheckpointMarkers>::new();
    for node in &tree.nodes {
        if node.kind != CheckpointNodeKind::File {
            continue;
        }
        let relative = node.path.strip_prefix(state_dir)?;
        let mut components = relative.components();
        let Some(run_name) = components.next() else {
            return Err("checkpoint state file has no run directory".into());
        };
        if components.next().is_none() {
            return Err("checkpoint state file is not inside a TLC run directory".into());
        }
        let markers = runs
            .entry(state_dir.join(run_name.as_os_str()))
            .or_default();
        let name = node
            .path
            .file_name()
            .ok_or("checkpoint state file has no file name")?
            .to_string_lossy();
        markers.temporary |= has_tlc_extension(&name, "tmp");
        markers.committed |= has_tlc_extension(&name, "chkpt");
    }
    Ok(runs)
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
    deadline: Instant,
) -> Result<Result<Option<String>, String>, Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint candidate validation")?;
    let observed_contract: CheckpointContract =
        match read_candidate_json(contract_path, "contract", deadline)? {
            Ok(contract) => contract,
            Err(error) => return Ok(Err(error)),
        };
    ensure_deadline(deadline, "checkpoint contract validation")?;
    if &observed_contract != expected {
        return Ok(Err(
            "restored checkpoint contract is stale or incompatible".to_owned()
        ));
    }
    let observed_inventory: CheckpointInventory =
        match read_candidate_json(inventory_path, "inventory", deadline)? {
            Ok(inventory) => inventory,
            Err(error) => return Ok(Err(error)),
        };
    ensure_deadline(deadline, "checkpoint inventory validation")?;
    let contract_sha256 = match expected.sha256() {
        Ok(sha256) => sha256,
        Err(error) => return Ok(Err(format!("digest checkpoint contract: {error}"))),
    };
    let expected_inventory = match inventory(state_dir, &contract_sha256, deadline) {
        Ok(inventory) => inventory,
        Err(error)
            if error.downcast_ref::<CheckpointDeadlineError>().is_some()
                || error.downcast_ref::<std::io::Error>().is_some() =>
        {
            return Err(error)
        }
        Err(error) => return Ok(Err(format!("inventory restored checkpoint: {error}"))),
    };
    if observed_inventory != expected_inventory {
        return Ok(Err(
            "restored checkpoint inventory is malformed or stale".to_owned()
        ));
    }
    Ok(Ok(observed_inventory.latest_checkpoint))
}

fn inventory(
    state_dir: &Path,
    contract_sha256: &str,
    deadline: Instant,
) -> Result<CheckpointInventory, Box<dyn Error>> {
    inventory_with_limits(state_dir, contract_sha256, deadline, TRAVERSAL_LIMITS)
}

fn inventory_with_limits(
    state_dir: &Path,
    contract_sha256: &str,
    deadline: Instant,
    limits: TraversalLimits,
) -> Result<CheckpointInventory, Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint inventory")?;
    let mut files = Vec::new();
    if path_entry_exists(state_dir)? {
        let mut budget = TraversalBudget::new(limits);
        let tree = scan_checkpoint_tree(
            state_dir,
            deadline,
            "checkpoint inventory",
            &mut budget,
            false,
        )?;
        for (run, markers) in checkpoint_runs(state_dir, &tree)? {
            if !markers.complete() {
                return Err(
                    format!("checkpoint run directory is incomplete: {}", run.display()).into(),
                );
            }
        }
        for node in tree
            .nodes
            .iter()
            .filter(|node| node.kind == CheckpointNodeKind::File)
        {
            ensure_deadline(deadline, "checkpoint hashing")?;
            let (sha256, size_bytes) = hash_file(&node.path, deadline)?;
            let relative = node
                .path
                .strip_prefix(state_dir)?
                .components()
                .map(|component| component.as_os_str().to_string_lossy())
                .collect::<Vec<_>>()
                .join("/");
            files.push(CheckpointFile {
                path: relative,
                sha256,
                size_bytes,
            });
        }
    }
    ensure_deadline(deadline, "checkpoint inventory sorting")?;
    files.sort();
    ensure_deadline(deadline, "checkpoint inventory sorting")?;
    let latest_checkpoint = files
        .iter()
        .filter_map(|file| file.path.split_once('/').map(|(directory, _)| directory))
        .max()
        .map(str::to_owned);
    Ok(CheckpointInventory {
        schema_version: 1,
        contract_sha256: contract_sha256.to_owned(),
        latest_checkpoint,
        files,
    })
}

fn hash_file(path: &Path, deadline: Instant) -> Result<(String, u64), Box<dyn Error>> {
    let file = HeldDirectory::workspace()?.open_file(path)?;
    hash_reader(file, || {
        ensure_deadline(deadline, "checkpoint file hashing")
    })
}

fn hash_reader<R, F>(mut reader: R, mut check_deadline: F) -> Result<(String, u64), Box<dyn Error>>
where
    R: Read,
    F: FnMut() -> Result<(), Box<dyn Error>>,
{
    let mut digest = Sha256::new();
    let mut size_bytes = 0_u64;
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        check_deadline()?;
        let read = reader.read(&mut buffer)?;
        check_deadline()?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
        size_bytes = size_bytes
            .checked_add(u64::try_from(read)?)
            .ok_or("checkpoint file size overflow")?;
    }
    Ok((format!("{:x}", digest.finalize()), size_bytes))
}

fn preserve_if_regular(
    source: &Path,
    output_dir: &Path,
    namespace: &Path,
    kind: &str,
    artifacts: &mut Vec<ArtifactRef>,
    deadline: Instant,
) -> Result<(), Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint diagnostic capture")?;
    if entry_kind(source)? != Some(CheckpointNodeKind::File) {
        return Ok(());
    }
    let metadata = HeldDirectory::workspace()?.open_file(source)?.metadata()?;
    if metadata.len() > MAX_CHECKPOINT_METADATA_BYTES {
        return Ok(());
    }
    let bytes = match read_file_with_deadline(source, deadline, "checkpoint diagnostic read") {
        Ok(bytes) => bytes,
        Err(error)
            if error.downcast_ref::<CheckpointDeadlineError>().is_some()
                || error.downcast_ref::<std::io::Error>().is_some() =>
        {
            return Err(error)
        }
        Err(_) => return Ok(()),
    };
    artifacts.push(artifact::write(
        output_dir,
        &namespace.join(kind),
        kind,
        &bytes,
    )?);
    ensure_deadline(deadline, "checkpoint diagnostic capture")?;
    Ok(())
}

fn read_candidate_json<T: for<'de> Deserialize<'de>>(
    path: &Path,
    label: &str,
    deadline: Instant,
) -> Result<Result<T, String>, Box<dyn Error>> {
    if entry_kind(path)? != Some(CheckpointNodeKind::File) {
        return Ok(Err(format!("checkpoint {label} is not a regular file")));
    }
    let metadata = HeldDirectory::workspace()?.open_file(path)?.metadata()?;
    if metadata.len() > MAX_CHECKPOINT_METADATA_BYTES {
        return Ok(Err(format!(
            "checkpoint {label} exceeds the metadata size limit"
        )));
    }
    let bytes = match read_file_with_deadline(path, deadline, "checkpoint metadata read") {
        Ok(bytes) => bytes,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(Err(format!("read checkpoint {label}: {error}")))
        }
        Err(error)
            if error.downcast_ref::<CheckpointDeadlineError>().is_some()
                || error.downcast_ref::<std::io::Error>().is_some() =>
        {
            return Err(error)
        }
        Err(error) => return Ok(Err(format!("read checkpoint {label}: {error}"))),
    };
    Ok(
        serde_json::from_slice(&bytes)
            .map_err(|error| format!("parse checkpoint {label}: {error}")),
    )
}

fn read_file_with_deadline(
    path: &Path,
    deadline: Instant,
    operation: &str,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut file = HeldDirectory::workspace()?.open_file(path)?;
    let metadata = file.metadata()?;
    if metadata.len() > MAX_CHECKPOINT_METADATA_BYTES {
        return Err(format!(
            "{} exceeds the checkpoint metadata size limit",
            path.display()
        )
        .into());
    }
    let mut bytes = Vec::new();
    let mut buffer = vec![0_u8; HASH_BUFFER_BYTES];
    loop {
        ensure_deadline(deadline, operation)?;
        let read = file.read(&mut buffer)?;
        ensure_deadline(deadline, operation)?;
        if read == 0 {
            break;
        }
        let next_len = bytes
            .len()
            .checked_add(read)
            .ok_or("checkpoint metadata size overflow")?;
        if u64::try_from(next_len)? > MAX_CHECKPOINT_METADATA_BYTES {
            return Err("checkpoint metadata exceeds the size limit".into());
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    Ok(bytes)
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
    HeldDirectory::workspace()?.write_atomic(path, &serde_json::to_vec_pretty(value)?)
}

#[cfg(test)]
#[path = "tla_checkpoint_tests.rs"]
mod tests;
