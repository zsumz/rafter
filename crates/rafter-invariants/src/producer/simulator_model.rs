use std::{
    collections::BTreeMap,
    error::Error,
    ffi::OsString,
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

#[derive(Debug, Eq, PartialEq)]
struct ModelRun {
    label: String,
    arguments: Vec<OsString>,
}

pub(super) fn execute(
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
) -> Result<SimulatorExecution, Box<dyn Error>> {
    let (binary, mut artifacts, build_peak) = build(profile, source_ref, output_dir)?;
    let binary_artifact = artifact::capture(
        output_dir,
        Path::new(&format!("{profile}-simulator/inputs")),
        &binary,
        "simulator-binary",
    )?;
    artifacts.push(binary_artifact);
    let mut events = BTreeMap::<String, Vec<Value>>::new();
    let mut peak_rss_kib = build_peak;
    let mut duration_ms = 0_u64;
    let mut processes_succeeded = true;
    for run in execution_plan(profile, source_ref)? {
        let output = process::timed(
            binary
                .to_str()
                .ok_or("simulator binary path is not UTF-8")?,
            &run.arguments,
            &process::base_environment(),
            Path::new("."),
        )?;
        peak_rss_kib = peak_rss_kib.max(output.peak_rss_kib);
        duration_ms = duration_ms.saturating_add(process::duration_ms(output.duration));
        processes_succeeded &= output.status.success();
        collect_events(profile, &output.stdout, &mut events)?;
        let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
        artifacts.push(artifact::write(
            output_dir,
            Path::new(&format!(
                "{profile}-simulator/{source_prefix}/{}.log",
                run.label
            )),
            "simulator-log",
            &process::combined_log(&run.label, &output)?,
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

fn execution_plan(profile: &str, source_ref: &str) -> Result<Vec<ModelRun>, Box<dyn Error>> {
    let runs = match profile {
        "pr" => vec![model_run("fast", None), model_run("raft-soak", None)],
        "nightly" => vec![model_run(
            "raft-nightly",
            expected_scheduled_seeds(profile, source_ref),
        )],
        "weekly" => vec![model_run(
            "raft-weekly",
            expected_scheduled_seeds(profile, source_ref),
        )],
        _ => return Err(format!("unsupported simulator profile {profile}").into()),
    };
    Ok(runs)
}

fn model_run(profile: &str, seeds: Option<String>) -> ModelRun {
    let mut arguments = vec![OsString::from("--profile"), OsString::from(profile)];
    if let Some(seeds) = seeds {
        arguments.extend([OsString::from("--seed"), OsString::from(seeds)]);
    }
    ModelRun {
        label: profile.to_owned(),
        arguments,
    }
}

fn source_derived_seeds(profile: &str, source_ref: &str, count: usize) -> String {
    (0..count)
        .map(|index| {
            let value = artifact::deterministic_u64(
                "scheduled-simulator-seed-v1",
                &format!("{profile}\0{source_ref}\0{index}"),
            );
            format!("0x{value:x}")
        })
        .collect::<Vec<_>>()
        .join(",")
}

pub(crate) fn expected_scheduled_seeds(profile: &str, source_ref: &str) -> Option<String> {
    let count = match profile {
        "nightly" => 6,
        "weekly" => 10,
        _ => return None,
    };
    Some(source_derived_seeds(profile, source_ref, count))
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
        &process::combined_log("simulator compile", &output)?,
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
    profile: &str,
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
        events
            .entry(check_id.to_owned())
            .or_default()
            .push(event.clone());
        if let Some(canonical) = canonical_check_id(profile, check_id) {
            events.entry(canonical).or_default().push(event);
        }
    }
    Ok(())
}

pub(crate) fn canonical_check_id(profile: &str, check_id: &str) -> Option<String> {
    let suffix = match profile {
        "nightly" => "nightly",
        "weekly" => "weekly",
        _ => return None,
    };
    let scheduled_soak = format!("raft-{suffix}-soak");
    if let Some(rest) = check_id.strip_prefix(&scheduled_soak) {
        return Some(format!("raft-soak{rest}"));
    }
    check_id
        .strip_suffix(&format!("-{suffix}"))
        .map(str::to_owned)
}

#[cfg(test)]
mod tests {
    use super::{canonical_check_id, execution_plan};

    #[test]
    fn scheduled_plans_use_stable_source_derived_seed_counts() {
        let first = execution_plan("nightly", "abc123").expect("nightly plan");
        let second = execution_plan("nightly", "abc123").expect("nightly plan");
        assert_eq!(first, second);
        assert_eq!(first.len(), 1);
        let seeds = first[0].arguments[3].to_string_lossy();
        assert_eq!(seeds.split(',').count(), 6);

        let weekly = execution_plan("weekly", "abc123").expect("weekly plan");
        assert_eq!(
            weekly[0].arguments[3].to_string_lossy().split(',').count(),
            10
        );
        assert_ne!(first[0].arguments[3], weekly[0].arguments[3]);
    }

    #[test]
    fn scheduled_check_ids_bind_to_canonical_registry_checks() {
        assert_eq!(
            canonical_check_id("nightly", "raft-commit-nightly").as_deref(),
            Some("raft-commit")
        );
        assert_eq!(
            canonical_check_id("weekly", "raft-election-prevote-weekly").as_deref(),
            Some("raft-election-prevote")
        );
        assert_eq!(
            canonical_check_id("nightly", "raft-nightly-soak-membership").as_deref(),
            Some("raft-soak-membership")
        );
        assert_eq!(canonical_check_id("pr", "raft-commit"), None);
    }

    #[test]
    fn unsupported_simulator_profile_has_no_execution_plan() {
        assert!(execution_plan("adhoc", "abc123").is_err());
    }
}
