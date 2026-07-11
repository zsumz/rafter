use std::{
    collections::BTreeMap,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use serde_json::Value;

use crate::ArtifactRef;

use super::{artifact, process};

const EVENT_PREFIX: &str = "RAFTER_EVENT ";

pub(super) struct SimulatorExecution {
    pub events: BTreeMap<String, Vec<Value>>,
    pub artifacts: Vec<ArtifactRef>,
    pub peak_rss_kib: u64,
    pub duration_ms: u64,
    pub processes_succeeded: bool,
}

pub(super) fn execute(
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
) -> Result<SimulatorExecution, Box<dyn Error>> {
    let (binary, mut artifacts, build_peak) = build(profile, source_ref, output_dir)?;
    let binary_artifact = artifact::existing(&binary, "simulator-binary")?;
    artifacts.push(binary_artifact);
    let mut events = BTreeMap::<String, Vec<Value>>::new();
    let mut peak_rss_kib = build_peak;
    let mut duration_ms = 0_u64;
    let mut processes_succeeded = true;
    for model_profile in ["fast", "raft-soak"] {
        let output = process::timed(
            binary
                .to_str()
                .ok_or("simulator binary path is not UTF-8")?,
            &["--profile".into(), model_profile.into()],
            &process::base_environment(),
            Path::new("."),
        )?;
        peak_rss_kib = peak_rss_kib.max(output.peak_rss_kib);
        duration_ms = duration_ms.saturating_add(process::duration_ms(output.duration));
        processes_succeeded &= output.status.success();
        collect_events(&output.stdout, &mut events)?;
        let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
        artifacts.push(artifact::write(
            output_dir,
            Path::new(&format!(
                "{profile}-simulator/{source_prefix}/{model_profile}.log"
            )),
            "simulator-log",
            &process::combined_log(model_profile, &output),
        )?);
    }
    Ok(SimulatorExecution {
        events,
        artifacts,
        peak_rss_kib,
        duration_ms,
        processes_succeeded,
    })
}

fn build(
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
) -> Result<(PathBuf, Vec<ArtifactRef>, u64), Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let target_dir = Path::new("target/rafter-invariants/simulator-build")
        .join(source_prefix)
        .join(profile);
    if target_dir.exists() {
        fs::remove_dir_all(&target_dir)?;
    }
    fs::create_dir_all(&target_dir)?;
    let mut environment = process::base_environment();
    environment.insert(
        "CARGO_TARGET_DIR".to_owned(),
        target_dir.to_string_lossy().into_owned(),
    );
    let arguments = [
        "build".into(),
        "--release".into(),
        "--locked".into(),
        "-p".into(),
        "rafter-sim".into(),
        "--bin".into(),
        "rafter-model-check-fast".into(),
        "--message-format=json-render-diagnostics".into(),
    ];
    let output = process::timed("cargo", &arguments, &environment, Path::new("."))?;
    let log = artifact::write(
        output_dir,
        Path::new(&format!("{profile}-simulator/{source_prefix}/compile.log")),
        "compile-log",
        &process::combined_log("simulator compile", &output),
    )?;
    if !output.status.success() {
        return Err("simulator release build failed".into());
    }
    let binary = executable_from_messages(&output.stdout)?;
    Ok((binary, vec![log], output.peak_rss_kib))
}

fn executable_from_messages(bytes: &[u8]) -> Result<PathBuf, Box<dyn Error>> {
    let mut executables = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if message["reason"] == "compiler-artifact"
            && message["target"]["name"] == "rafter-model-check-fast"
        {
            if message["fresh"] == true {
                return Err("fresh cached simulator binary is forbidden".into());
            }
            if let Some(executable) = message["executable"].as_str() {
                executables.push(PathBuf::from(executable));
            }
        }
    }
    if executables.len() != 1 {
        return Err(format!(
            "expected one simulator executable, found {}",
            executables.len()
        )
        .into());
    }
    Ok(executables.remove(0))
}

fn collect_events(
    stdout: &[u8],
    events: &mut BTreeMap<String, Vec<Value>>,
) -> Result<(), Box<dyn Error>> {
    for source in String::from_utf8_lossy(stdout)
        .lines()
        .filter_map(|line| line.strip_prefix(EVENT_PREFIX))
    {
        let event = serde_json::from_str::<Value>(source)?;
        let check_id = event["check_id"]
            .as_str()
            .ok_or("simulator event omitted check_id")?;
        events.entry(check_id.to_owned()).or_default().push(event);
    }
    Ok(())
}
