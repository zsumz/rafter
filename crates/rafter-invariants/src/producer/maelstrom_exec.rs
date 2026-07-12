use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use crate::ArtifactRef;

use super::{artifact, maelstrom_edn, maelstrom_scenario::required_configuration, process};

pub(super) use super::maelstrom_scenario::Scenario;

pub(super) struct TrialOutcome {
    pub summary: Option<maelstrom_edn::MaelstromSummary>,
    pub error: Option<String>,
    pub process_succeeded: bool,
    pub markers: ScenarioMarkers,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub artifacts: Vec<ArtifactRef>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ScenarioMarkers {
    pub membership_enter: u64,
    pub membership_leave: u64,
    pub membership_complete: u64,
    pub restarts: u64,
    pub post_restart_progress: u64,
    pub crashpoints: u64,
    pub post_crash_progress: u64,
    pub snapshots_compacted: u64,
    pub snapshots_applied: u64,
    pub post_restart_snapshots_applied: u64,
}

pub(super) fn run_trial(
    scenario: Scenario,
    trial: u64,
    profile: &str,
    source_ref: &str,
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
) -> Result<TrialOutcome, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let state_dir = reset_state_directory(
        Path::new("target/rafter-invariants/maelstrom")
            .join(source_prefix)
            .join(profile)
            .join(scenario.name())
            .join(format!("trial-{trial}")),
    )?;
    let durable = state_dir.join("durable");
    fs::create_dir_all(&durable)?;
    let script = fs::canonicalize(scenario.script())?;
    let environment = trial_environment(configuration, &durable, scenario)?;
    let output = process::timed(
        script
            .to_str()
            .ok_or("Maelstrom script path is not UTF-8")?,
        &["--test-count".into(), "1".into()],
        &environment,
        &state_dir,
    )?;
    let namespace = Path::new(&format!(
        "{profile}-maelstrom/{source_prefix}/{}/trial-{trial}",
        scenario.name()
    ))
    .to_path_buf();
    let mut artifacts = vec![artifact::write(
        output_dir,
        &namespace.join("process.json"),
        "maelstrom-process-log",
        &process::json_log(scenario.name(), &output)?,
    )?];
    let script_artifact = artifact::capture(
        output_dir,
        &namespace.join("inputs"),
        &script,
        "maelstrom-runner",
    )?;
    artifacts.push(script_artifact);
    artifacts.push(super::maelstrom_tool::capture_jar(output_dir, &namespace)?);
    capture_binary(
        output_dir,
        &namespace,
        Path::new("target/debug/rafter-maelstrom"),
        "maelstrom-binary",
        &mut artifacts,
    )?;
    if matches!(
        scenario,
        Scenario::Restart | Scenario::AppCrash | Scenario::Snapshot
    ) {
        capture_binary(
            output_dir,
            &namespace,
            Path::new("target/debug/rafter-maelstrom-leader-restart-proxy"),
            "maelstrom-proxy-binary",
            &mut artifacts,
        )?;
    }
    let run_store = discover_store(&state_dir);
    let (summary, error, markers) = match run_store {
        Ok(store) => {
            capture_tree(output_dir, &namespace.join("store"), &store, &mut artifacts)?;
            let results = fs::read_to_string(store.join("results.edn"));
            let parsed = results
                .map_err(|error| format!("read Maelstrom results.edn: {error}"))
                .and_then(|source| maelstrom_edn::parse(&source));
            let markers = read_markers(&store)?;
            match parsed {
                Ok(summary) => (Some(summary), None, markers),
                Err(error) => (None, Some(error), markers),
            }
        }
        Err(error) => (None, Some(error), ScenarioMarkers::default()),
    };
    capture_tree(
        output_dir,
        &namespace.join("durable"),
        &durable,
        &mut artifacts,
    )?;
    Ok(TrialOutcome {
        summary,
        error,
        process_succeeded: output.status.success(),
        markers,
        duration_ms: process::duration_ms(output.duration),
        peak_rss_kib: output.peak_rss_kib,
        artifacts,
    })
}

fn trial_environment(
    configuration: &BTreeMap<String, String>,
    durable: &Path,
    scenario: Scenario,
) -> Result<BTreeMap<String, String>, String> {
    let mut environment = process::base_environment();
    environment.extend([
        (
            "RAFTER_MAELSTROM_ROOT".to_owned(),
            durable.to_string_lossy().into_owned(),
        ),
        (
            "RAFTER_MAELSTROM_TIME_LIMIT".to_owned(),
            required_configuration(configuration, "duration_seconds")?.to_owned(),
        ),
        (
            "RAFTER_MAELSTROM_RATE".to_owned(),
            required_configuration(configuration, "rate")?.to_owned(),
        ),
        (
            "RAFTER_MAELSTROM_CONCURRENCY".to_owned(),
            scenario.concurrency().to_owned(),
        ),
    ]);
    Ok(environment)
}

fn reset_state_directory(path: PathBuf) -> Result<PathBuf, Box<dyn Error>> {
    if path.exists() {
        fs::remove_dir_all(&path)?;
    }
    fs::create_dir_all(&path)?;
    Ok(fs::canonicalize(path)?)
}

fn capture_binary(
    output_dir: &Path,
    namespace: &Path,
    binary: &Path,
    kind: &str,
    artifacts: &mut Vec<ArtifactRef>,
) -> Result<(), Box<dyn Error>> {
    if !binary.is_file() {
        return Err(format!("Maelstrom run did not produce {}", binary.display()).into());
    }
    artifacts.push(artifact::capture(
        output_dir,
        &namespace.join("inputs"),
        binary,
        kind,
    )?);
    Ok(())
}

fn discover_store(state_dir: &Path) -> Result<PathBuf, String> {
    let root = state_dir.join("store/lin-kv");
    let mut stores = fs::read_dir(&root)
        .map_err(|error| format!("read Maelstrom store {}: {error}", root.display()))?
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    stores.sort();
    match stores.as_slice() {
        [store] => Ok(store.clone()),
        _ => Err(format!(
            "expected one Maelstrom retained store, found {}",
            stores.len()
        )),
    }
}

fn capture_tree(
    output_dir: &Path,
    namespace: &Path,
    root: &Path,
    artifacts: &mut Vec<ArtifactRef>,
) -> Result<(), Box<dyn Error>> {
    for file in files_below(root)? {
        let relative = file.strip_prefix(root)?;
        let kind = if relative == Path::new("results.edn") {
            "maelstrom-results"
        } else if relative.starts_with("node-logs") {
            "maelstrom-node-log"
        } else if namespace.ends_with("durable") {
            "maelstrom-durable-file"
        } else {
            "maelstrom-store-file"
        };
        artifacts.push(artifact::capture_as(
            output_dir,
            &namespace.join(relative),
            &file,
            kind,
        )?);
    }
    Ok(())
}

fn files_below(root: &Path) -> Result<Vec<PathBuf>, Box<dyn Error>> {
    let mut pending = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let kind = entry.file_type()?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                files.push(entry.path());
            }
        }
    }
    files.sort();
    Ok(files)
}

fn read_markers(store: &Path) -> Result<ScenarioMarkers, Box<dyn Error>> {
    let mut markers = ScenarioMarkers::default();
    for file in files_below(&store.join("node-logs"))? {
        scan_markers(&fs::read_to_string(file)?, &mut markers);
    }
    Ok(markers)
}

fn scan_markers(source: &str, markers: &mut ScenarioMarkers) {
    let mut saw_restart = false;
    let mut saw_crash = false;
    for line in source.lines() {
        markers.membership_enter += u64::from(line.contains("action=enter-joint"));
        markers.membership_leave += u64::from(line.contains("action=leave-joint"));
        markers.membership_complete += u64::from(line.contains("complete target="));
        if line.contains("proxy restarting child") {
            markers.restarts += 1;
            saw_restart = true;
        }
        if line.contains("crashpoint=RAFTER_MAELSTROM_CRASH_AFTER_APP_PERSIST_ONCE fired") {
            markers.crashpoints += 1;
            saw_crash = true;
        }
        let progress = line.contains(" role=leader ") || line.contains("compacted snapshot");
        markers.post_restart_progress += u64::from(saw_restart && progress);
        markers.post_crash_progress += u64::from(saw_crash && progress);
        markers.snapshots_compacted += u64::from(line.contains("compacted snapshot"));
        markers.snapshots_applied += u64::from(line.contains("applied snapshot"));
        markers.post_restart_snapshots_applied +=
            u64::from(saw_restart && line.contains("applied snapshot"));
    }
}
