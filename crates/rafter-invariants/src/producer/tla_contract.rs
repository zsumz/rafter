use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    ffi::OsString,
    fs,
    path::Path,
    time::Duration,
};

use sha2::{Digest, Sha256};

use crate::SourceReceipt;

use super::tla_checkpoint;
use super::{artifact, process};

const SPEC: &str = "specs/tla/raft/Raft.tla";
const TRACE_SPEC: &str = "specs/tla/raft/RaftMembershipTraceSample.tla";
const TRACE_CONFIG: &str = "specs/tla/raft/RaftMembershipTraceSample.cfg";
const TRACE_SPEC_SHA256: &str = "6ed44f924f4a23dc507e76a4d8f540ecbb7c3689b319a9790cf5f210080132e8";
const TRACE_CONFIG_SHA256: &str =
    "1286edee2df96b702937d9c1340f8412c060a6e9a0df53dd46b0149d2027b96e";
const DETECTOR_SPEC: &str = "specs/tla/raft/RafterInvariantDetectorNegative.tla";
const DETECTOR_CONFIG: &str = "specs/tla/raft/RafterInvariantDetectorNegative.cfg";
const JAR: &str = "tools/cache/tla2tools.jar";
const TOOL_FETCH_TIMEOUT: Duration = Duration::from_secs(5 * 60);

use super::tla_output::{
    render_detector_config, DETECTOR_PROBES, REGISTERED_PREDICATES, REQUIRED_MODEL_TRANSITIONS,
};

pub(super) fn validate_runner_options(
    configuration: &BTreeMap<String, String>,
) -> Result<(), Box<dyn Error>> {
    for (name, expected) in [
        ("module", "Raft.tla"),
        ("fp", "0"),
        ("tool_mode", "required"),
        ("trace_sample", "required"),
        ("detector_negative", "required"),
    ] {
        if required_configuration(configuration, name)? != expected {
            return Err(format!("TLA runner requires {name}={expected}").into());
        }
    }
    if matches!(
        configuration.get("config").map(String::as_str),
        Some("RaftCi.cfg" | "RaftNightly.cfg")
    ) && required_configuration(configuration, "symmetry")?
        != "nodes-values-read-requests-product"
    {
        return Err("bounded TLA runner requires the complete model-value symmetry".into());
    }
    match (
        configuration.get("config").map(String::as_str),
        tla_checkpoint::enabled(configuration),
    ) {
        (Some("RaftCi.cfg"), false) => {
            for (name, expected) in [
                ("workers", "4"),
                ("soft_timeout", "300m"),
                ("max_heap", "8g"),
                ("fp_mem", "0.45"),
            ] {
                if required_configuration(configuration, name)? != expected {
                    return Err(format!("PR TLA runner requires {name}={expected}").into());
                }
            }
        }
        (Some("RaftNightly.cfg"), true) => {
            for (name, expected) in [
                ("workers", "auto"),
                ("soft_timeout", "295m"),
                ("checkpoint_minutes", "30"),
                ("checkpoint_gzip", "required"),
                ("max_heap", "8g"),
                ("fp_mem", "0.45"),
                ("checkpoint_recovery", "strict-compatible-if-present"),
            ] {
                if required_configuration(configuration, name)? != expected {
                    return Err(format!(
                        "checkpointed nightly TLA runner requires {name}={expected}"
                    )
                    .into());
                }
            }
        }
        _ => {}
    }
    if tla_checkpoint::enabled(configuration) {
        match required_configuration(configuration, "config")? {
            "Raft.cfg" => {
                for (name, expected) in [
                    ("workers", "auto"),
                    ("soft_timeout", "295m"),
                    ("checkpoint_minutes", "30"),
                    ("checkpoint_gzip", "required"),
                    ("max_heap", "4g"),
                    ("fp_mem", "0.45"),
                    ("checkpoint_recovery", "strict-compatible-if-present"),
                    ("unsymmetrized_exploration", "required"),
                ] {
                    if required_configuration(configuration, name)? != expected {
                        return Err(format!(
                            "checkpointed weekly TLA runner requires {name}={expected}"
                        )
                        .into());
                    }
                }
            }
            "RaftNightly.cfg" => {}
            other => {
                return Err(
                    format!("checkpointed TLA runner does not support config={other}").into(),
                )
            }
        }
    }
    Ok(())
}

pub(super) fn validate_spec_contract(
    config_name: &str,
    symbols: &BTreeSet<String>,
) -> Result<Vec<String>, Box<dyn Error>> {
    let registered = REGISTERED_PREDICATES
        .iter()
        .map(|predicate| (*predicate).to_owned())
        .collect::<BTreeSet<_>>();
    if symbols != &registered {
        return Err("TLA registry must contain exactly the eight detector predicates".into());
    }
    let config = Path::new("specs/tla/raft").join(config_name);
    let config_source = fs::read_to_string(&config)?;
    let configured = configured_invariants(&config_source);
    let configured_set = configured.iter().cloned().collect::<BTreeSet<_>>();
    let mut expected = symbols.clone();
    expected.insert("TypeOK".to_owned());
    let spec = fs::read_to_string(SPEC)?;
    let detector_spec = fs::read_to_string(DETECTOR_SPEC)?;
    let detector_config = fs::read_to_string(DETECTOR_CONFIG)?;
    validate_trace_contract(symbols)?;
    validate_safety_only_boundary(&spec, &config_source)?;
    validate_symmetry_contract(config_name, &config_source)?;
    if configured_set != expected
        || symbols.iter().any(|symbol| {
            !spec
                .lines()
                .any(|line| line.trim_start().starts_with(&format!("{symbol} ==")))
        })
        || !detector_spec
            .lines()
            .any(|line| line.trim() == "EXTENDS Raft")
        || !detector_spec
            .lines()
            .any(|line| line.trim_start().starts_with("FixtureInit =="))
        || !detector_spec
            .lines()
            .any(|line| line.trim_start().starts_with("FixtureNext =="))
        || DETECTOR_PROBES.iter().any(|probe| {
            detector_spec.lines().any(|line| {
                line.trim_start()
                    .starts_with(&format!("{} ==", probe.predicate))
            }) || render_detector_config(&detector_config, *probe).is_err()
        })
    {
        return Err(
            "TLA config/spec/detector does not contain exactly the registry predicates".into(),
        );
    }
    Ok(configured)
}

fn validate_trace_contract(symbols: &BTreeSet<String>) -> Result<(), Box<dyn Error>> {
    let trace_spec = fs::read_to_string(TRACE_SPEC)?;
    let trace_config = fs::read_to_string(TRACE_CONFIG)?;
    validate_trace_contract_sources(symbols, &trace_spec, &trace_config)
}

fn validate_trace_contract_sources(
    symbols: &BTreeSet<String>,
    trace_spec: &str,
    trace_config: &str,
) -> Result<(), Box<dyn Error>> {
    let mut expected_invariants = symbols.clone();
    expected_invariants.insert("TypeOK".to_owned());
    let configured = configured_invariants(trace_config)
        .into_iter()
        .collect::<BTreeSet<_>>();
    let specifications = trace_config
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("SPECIFICATION "))
        .collect::<Vec<_>>();
    let properties = trace_config
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("PROPERTY ") || line.starts_with("PROPERTIES "))
        .collect::<Vec<_>>();
    let required_definitions = [
        "ReaddCheckpointReady ==",
        "ReaddCheckpointReached ==",
        "TraceAction44 ==",
        "TraceSpec ==",
        "TraceComplete ==",
        "TraceCompletes ==",
    ];
    if configured != expected_invariants {
        return Err("membership trace must bind the exact registered safety invariants".into());
    }
    if specifications.as_slice() != ["SPECIFICATION TraceSpec"] {
        return Err("membership trace must configure exactly TraceSpec".into());
    }
    if properties.as_slice() != ["PROPERTY TraceCompletes"] {
        return Err("membership trace must configure exactly TraceCompletes".into());
    }
    if let Some(definition) = required_definitions.iter().find(|definition| {
        !trace_spec
            .lines()
            .any(|line| line.trim_start().starts_with(*definition))
    }) {
        return Err(format!("membership trace is missing required definition {definition}").into());
    }
    validate_trace_transition_coverage(trace_spec)?;
    for required_line in [
        "EXTENDS Raft",
        "/\\ WF_traceVars(TraceNext)",
        "TraceComplete == traceStep = 45",
        "TraceCompletes == <>TraceComplete",
        "\\/ /\\ traceStep = 45",
    ] {
        if !trace_spec.lines().any(|line| line.trim() == required_line) {
            return Err(
                format!("membership trace is missing exact contract line {required_line}").into(),
            );
        }
    }
    for required_line in ["Nodes = {n1, n2, n3}", "MaxTerm = 2", "MaxLogLen = 6"] {
        if !trace_config
            .lines()
            .any(|line| line.trim() == required_line)
        {
            return Err(
                format!("membership trace config is missing exact bound {required_line}").into(),
            );
        }
    }
    if format!("{:x}", Sha256::digest(trace_spec.as_bytes())) != TRACE_SPEC_SHA256 {
        return Err("membership trace module bytes do not match the reviewed contract".into());
    }
    if format!("{:x}", Sha256::digest(trace_config.as_bytes())) != TRACE_CONFIG_SHA256 {
        return Err("membership trace config bytes do not match the reviewed contract".into());
    }
    Ok(())
}

fn validate_trace_transition_coverage(trace_spec: &str) -> Result<(), Box<dyn Error>> {
    let lines = trace_spec.lines().collect::<Vec<_>>();
    let mut blocks = BTreeMap::new();
    for step in 0..=44 {
        let definition = format!("TraceAction{step} ==");
        let start = lines
            .iter()
            .position(|line| line.trim() == definition)
            .ok_or_else(|| format!("membership trace is missing {definition}"))?;
        let end = lines[start + 1..]
            .iter()
            .position(|line| {
                !line.chars().next().is_some_and(char::is_whitespace)
                    && line.trim_end().ends_with(" ==")
            })
            .map_or(lines.len(), |offset| start + 1 + offset);
        let body = lines[start + 1..end].join("\n");
        for required in [
            format!("/\\ traceStep = {step}"),
            format!("/\\ traceStep' = {}", step + 1),
            format!("\\/ TraceAction{step}"),
        ] {
            if required.starts_with("\\/") {
                if !lines.iter().any(|line| line.trim() == required) {
                    return Err(format!("membership trace is missing chain edge {required}").into());
                }
            } else if !body.lines().any(|line| line.trim() == required) {
                return Err(format!("membership trace action {step} is missing {required}").into());
            }
        }
        blocks.insert(step, body);
    }
    for transition in REQUIRED_MODEL_TRANSITIONS {
        let call = if transition == "InstallSnapshot" {
            "/\\ InstallSnapshot".to_owned()
        } else {
            format!("/\\ {transition}(")
        };
        if !blocks
            .values()
            .any(|body| body.lines().any(|line| line.trim().starts_with(&call)))
        {
            return Err(format!(
                "membership trace does not execute required transition {transition}"
            )
            .into());
        }
    }
    Ok(())
}

fn validate_symmetry_contract(config_name: &str, config: &str) -> Result<(), Box<dyn Error>> {
    let declarations = config
        .lines()
        .map(str::trim)
        .filter(|line| line.starts_with("SYMMETRY "))
        .collect::<Vec<_>>();
    match config_name {
        "RaftCi.cfg" | "RaftNightly.cfg"
            if declarations.as_slice() == ["SYMMETRY ModelPermutations"] =>
        {
            Ok(())
        }
        "RaftCi.cfg" | "RaftNightly.cfg" => {
            Err("bounded TLA config must use the complete model-value product symmetry".into())
        }
        "Raft.cfg" if declarations.is_empty() => Ok(()),
        "Raft.cfg" => Err("full weekly TLA config must be unsymmetrized".into()),
        _ => Ok(()),
    }
}

fn validate_safety_only_boundary(spec: &str, config: &str) -> Result<(), Box<dyn Error>> {
    let stuttering_spec = spec
        .lines()
        .any(|line| line.trim() == "Spec == Init /\\ [][Next]_vars");
    let embeds_fairness = spec.lines().any(|line| {
        let line = line.trim();
        line.contains("WF_vars(") || line.contains("SF_vars(")
    });
    let configures_property = config.lines().any(|line| {
        let line = line.trim_start();
        line == "PROPERTY"
            || line == "PROPERTIES"
            || line.starts_with("PROPERTY ")
            || line.starts_with("PROPERTIES ")
    });
    if !stuttering_spec || embeds_fairness || configures_property {
        return Err(
            "production TLA is safety-only; bounded fair-schedule liveness belongs to the simulator"
                .into(),
        );
    }
    Ok(())
}

fn configured_invariants(source: &str) -> Vec<String> {
    let mut invariants = Vec::new();
    let mut collecting = false;
    for line in source.lines() {
        let line = line.trim();
        if line == "INVARIANT" || line == "INVARIANTS" {
            collecting = true;
        } else if let Some(symbol) = line.strip_prefix("INVARIANT ") {
            invariants.push(symbol.trim().to_owned());
            collecting = false;
        } else if collecting && line.is_empty() {
            collecting = false;
        } else if collecting {
            invariants.push(line.to_owned());
        }
    }
    invariants
}

pub(super) fn source_artifacts(
    configuration: &BTreeMap<String, String>,
    output_dir: &Path,
    profile: &str,
    source_ref: &str,
) -> Result<Vec<crate::ArtifactRef>, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let namespace = Path::new(profile)
        .join("tla")
        .join(source_prefix)
        .join("inputs");
    let jar = artifact::capture(output_dir, &namespace, Path::new(JAR), "tla-tool")?;
    if jar.sha256 != required_configuration(configuration, "tool_sha256")? {
        return Err("pinned TLC jar digest does not match the profile contract".into());
    }
    if fs::read_to_string("tools/tla/ASSET_ID")?.trim()
        != required_configuration(configuration, "tool_asset_id")?
    {
        return Err("TLC asset ID does not match the profile contract".into());
    }
    Ok(vec![
        jar,
        artifact::capture(output_dir, &namespace, Path::new(SPEC), "tla-spec")?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new(TRACE_SPEC),
            "tla-trace-spec",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new(DETECTOR_SPEC),
            "tla-detector-spec",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new("scripts/tla-model-check"),
            "tla-runner",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new("tools/tla/ASSET_ID"),
            "tla-tool-asset-id",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new("tools/tla/SHA256SUMS"),
            "tla-tool-checksums",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            &Path::new("specs/tla/raft").join(required_configuration(configuration, "config")?),
            "tla-config",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new(TRACE_CONFIG),
            "tla-trace-config",
        )?,
        artifact::capture(
            output_dir,
            &namespace,
            Path::new(DETECTOR_CONFIG),
            "tla-detector-config",
        )?,
    ])
}

pub(super) fn fetch_tool() -> Result<(), Box<dyn Error>> {
    fetch_tool_with(
        "scripts/tla-model-check",
        &[OsString::from("--fetch-tool")],
        TOOL_FETCH_TIMEOUT,
    )
}

fn fetch_tool_with(
    program: &str,
    arguments: &[OsString],
    timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let environment = tool_fetch_environment();
    let output = process::timed_with_optional_layer_budget(
        process::ProcessKind::TlaExecution,
        program,
        arguments,
        &environment,
        Path::new("."),
        timeout,
    )?;
    if output.timed_out || !output.status.success() {
        return Err(format!(
            "fetch pinned TLC tool failed with {:?} (timed_out={}): stdout: {}; stderr: {}",
            output.status.code(),
            output.timed_out,
            String::from_utf8_lossy(&output.stdout).trim(),
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
}

fn tool_fetch_environment() -> BTreeMap<String, String> {
    let mut environment = process::base_environment();
    environment.insert("RAFTER_TLA_REPO_ROOT".to_owned(), ".".to_owned());
    environment
}

pub(super) fn validate_java(
    source: &SourceReceipt,
    configuration: &BTreeMap<String, String>,
) -> Result<(), Box<dyn Error>> {
    let java = source
        .tools
        .get("java")
        .ok_or("Java tool identity missing")?;
    let required = required_configuration(configuration, "java_major")?.parse::<u32>()?;
    if java_major(&java.version) != Some(required) {
        return Err(format!("Java version does not satisfy required major {required}").into());
    }
    Ok(())
}

pub(crate) fn java_major(version: &str) -> Option<u32> {
    version.split_whitespace().find_map(|part| {
        let part = part.trim_matches('"');
        let mut components = part.split('.');
        let first = components.next()?.parse::<u32>().ok()?;
        if first == 1 {
            components.next()?.parse().ok()
        } else {
            Some(first)
        }
    })
}

pub(super) fn required_configuration<'a>(
    configuration: &'a BTreeMap<String, String>,
    name: &str,
) -> Result<&'a str, Box<dyn Error>> {
    configuration
        .get(name)
        .map(String::as_str)
        .ok_or_else(|| format!("TLA runner configuration omitted {name}").into())
}

pub(super) fn parse_timeout(value: &str) -> Result<Duration, Box<dyn Error>> {
    let minutes = value
        .strip_suffix('m')
        .ok_or("TLA soft_timeout must use whole minutes")?
        .parse::<u64>()?;
    Ok(Duration::from_secs(minutes.saturating_mul(60)))
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, ffi::OsString, time::Duration};

    use super::super::tla_output::{
        render_detector_config, DetectorProbe, DEFAULT_FIXTURE_MODE, DETECTOR_PROBES,
        REGISTERED_PREDICATES,
    };
    use super::{
        configured_invariants, fetch_tool_with, java_major, tool_fetch_environment,
        validate_runner_options, validate_safety_only_boundary, validate_symmetry_contract,
        validate_trace_contract_sources, TRACE_CONFIG, TRACE_SPEC,
    };

    #[test]
    fn java_major_is_parsed_exactly() {
        assert_eq!(java_major("java 21.0.5 2024-10-15 LTS"), Some(21));
        assert_eq!(java_major("openjdk 21.0.7 2025-04-15"), Some(21));
        assert_eq!(java_major("java version \"1.8.0_402\""), Some(8));
        assert_eq!(java_major("java 210.0.1"), Some(210));
    }

    #[test]
    #[cfg(unix)]
    fn tool_fetch_is_managed_and_times_out_with_retained_diagnostics() {
        let error = fetch_tool_with(
            "sh",
            &[
                OsString::from("-c"),
                OsString::from("printf fetch-started; sleep 5"),
            ],
            Duration::from_millis(50),
        )
        .expect_err("stalled tool fetch must time out")
        .to_string();
        assert!(error.contains("timed_out=true"));
        assert!(error.contains("fetch-started"));
    }

    #[test]
    fn descriptor_bound_tool_fetch_receives_the_held_repository_root() {
        let environment = tool_fetch_environment();

        assert_eq!(environment["RAFTER_TLA_REPO_ROOT"], ".");
    }

    #[test]
    fn production_tla_contract_is_safety_only() {
        let safety_spec = "Spec == Init /\\ [][Next]_vars\n";
        let safety_config = "INVARIANT TypeOK\n";
        assert!(validate_safety_only_boundary(safety_spec, safety_config).is_ok());

        let fair_spec = "Spec == Init /\\ [][Next]_vars /\\ WF_vars(Next)\n";
        assert!(validate_safety_only_boundary(fair_spec, safety_config).is_err());
        assert!(validate_safety_only_boundary(safety_spec, "PROPERTY EventualLeader\n").is_err());
    }

    #[test]
    fn bounded_and_weekly_symmetry_contracts_are_exact() {
        assert!(validate_symmetry_contract("RaftCi.cfg", "SYMMETRY ModelPermutations\n").is_ok());
        assert!(
            validate_symmetry_contract("RaftNightly.cfg", "SYMMETRY NodePermutations\n").is_err()
        );
        assert!(validate_symmetry_contract("RaftCi.cfg", "CHECK_DEADLOCK FALSE\n").is_err());
        assert!(validate_symmetry_contract("Raft.cfg", "CHECK_DEADLOCK FALSE\n").is_ok());
        assert!(validate_symmetry_contract("Raft.cfg", "SYMMETRY ModelPermutations\n").is_err());
    }

    #[test]
    fn fixed_runner_options_cannot_drift_from_execution() {
        let mut options = BTreeMap::from([
            ("module".to_owned(), "Raft.tla".to_owned()),
            ("fp".to_owned(), "0".to_owned()),
            ("tool_mode".to_owned(), "required".to_owned()),
            ("trace_sample".to_owned(), "required".to_owned()),
            ("detector_negative".to_owned(), "required".to_owned()),
        ]);
        assert!(validate_runner_options(&options).is_ok());
        options.insert("fp".to_owned(), "1".to_owned());
        assert!(validate_runner_options(&options).is_err());
    }

    #[test]
    fn bounded_runner_symmetry_label_cannot_drift_from_execution() {
        let mut options = BTreeMap::from([
            ("module".to_owned(), "Raft.tla".to_owned()),
            ("fp".to_owned(), "0".to_owned()),
            ("tool_mode".to_owned(), "required".to_owned()),
            ("trace_sample".to_owned(), "required".to_owned()),
            ("detector_negative".to_owned(), "required".to_owned()),
            ("config".to_owned(), "RaftCi.cfg".to_owned()),
            ("workers".to_owned(), "4".to_owned()),
            ("soft_timeout".to_owned(), "300m".to_owned()),
            ("max_heap".to_owned(), "8g".to_owned()),
            ("fp_mem".to_owned(), "0.45".to_owned()),
            (
                "symmetry".to_owned(),
                "nodes-values-read-requests-product".to_owned(),
            ),
        ]);
        assert!(validate_runner_options(&options).is_ok());
        options.insert("symmetry".to_owned(), "nodes-only".to_owned());
        assert!(validate_runner_options(&options).is_err());
    }

    #[test]
    fn weekly_checkpoint_contract_is_exact() {
        let mut options = BTreeMap::from([
            ("module".to_owned(), "Raft.tla".to_owned()),
            ("fp".to_owned(), "0".to_owned()),
            ("tool_mode".to_owned(), "required".to_owned()),
            ("trace_sample".to_owned(), "required".to_owned()),
            ("detector_negative".to_owned(), "required".to_owned()),
            ("config".to_owned(), "Raft.cfg".to_owned()),
            ("workers".to_owned(), "auto".to_owned()),
            ("soft_timeout".to_owned(), "295m".to_owned()),
            ("checkpoint_minutes".to_owned(), "30".to_owned()),
            ("checkpoint_gzip".to_owned(), "required".to_owned()),
            ("max_heap".to_owned(), "4g".to_owned()),
            ("fp_mem".to_owned(), "0.45".to_owned()),
            (
                "checkpoint_recovery".to_owned(),
                "strict-compatible-if-present".to_owned(),
            ),
            (
                "unsymmetrized_exploration".to_owned(),
                "required".to_owned(),
            ),
        ]);
        assert!(validate_runner_options(&options).is_ok());
        options.insert("max_heap".to_owned(), "8g".to_owned());
        assert!(validate_runner_options(&options).is_err());
    }

    #[test]
    fn nightly_checkpoint_contract_is_exact() {
        let mut options = BTreeMap::from([
            ("module".to_owned(), "Raft.tla".to_owned()),
            ("fp".to_owned(), "0".to_owned()),
            ("tool_mode".to_owned(), "required".to_owned()),
            ("trace_sample".to_owned(), "required".to_owned()),
            ("detector_negative".to_owned(), "required".to_owned()),
            ("config".to_owned(), "RaftNightly.cfg".to_owned()),
            (
                "symmetry".to_owned(),
                "nodes-values-read-requests-product".to_owned(),
            ),
            ("workers".to_owned(), "auto".to_owned()),
            ("soft_timeout".to_owned(), "295m".to_owned()),
            ("checkpoint_minutes".to_owned(), "30".to_owned()),
            ("checkpoint_gzip".to_owned(), "required".to_owned()),
            ("max_heap".to_owned(), "8g".to_owned()),
            ("fp_mem".to_owned(), "0.45".to_owned()),
            (
                "checkpoint_recovery".to_owned(),
                "strict-compatible-if-present".to_owned(),
            ),
        ]);
        assert!(validate_runner_options(&options).is_ok());
        options.insert("workers".to_owned(), "4".to_owned());
        assert!(validate_runner_options(&options).is_err());
    }

    #[test]
    fn every_invariant_block_is_part_of_the_exact_contract() {
        let config = "INVARIANTS\n  TypeOK\n\nCHECK_DEADLOCK FALSE\n\nINVARIANT ElectionSafety\n";
        assert_eq!(
            configured_invariants(config),
            vec!["TypeOK".to_owned(), "ElectionSafety".to_owned()]
        );
    }

    #[test]
    fn detector_configs_bind_one_unique_counterexample_identity() {
        let template = "INIT FixtureInit\nCONSTANT TargetPredicate = \"ElectionSafety\"\nCONSTANT FixtureMode = \"Default\"\nINVARIANT TypeOK\nINVARIANT ElectionSafety\n";
        let rendered = DETECTOR_PROBES
            .iter()
            .map(|probe| {
                let config = render_detector_config(template, *probe).expect("valid template");
                assert!(config.contains(&format!(
                    "CONSTANT TargetPredicate = \"{}\"",
                    probe.predicate
                )));
                assert!(config.contains(&format!("CONSTANT FixtureMode = \"{}\"", probe.mode)));
                assert!(config.contains(&format!("INVARIANT {}", probe.predicate)));
                config
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(rendered.len(), DETECTOR_PROBES.len());
        let invalid = DetectorProbe {
            predicate: "ExpectedViolation",
            mode: DEFAULT_FIXTURE_MODE,
        };
        assert!(render_detector_config(template, invalid).is_err());
        assert!(render_detector_config("INIT Init\n", DETECTOR_PROBES[0]).is_err());
    }

    #[test]
    fn membership_trace_contract_rejects_any_reviewed_source_drift() {
        let symbols = REGISTERED_PREDICATES
            .iter()
            .map(|predicate| (*predicate).to_owned())
            .collect::<std::collections::BTreeSet<_>>();
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let spec =
            std::fs::read_to_string(root.join(TRACE_SPEC)).expect("read membership trace spec");
        let config =
            std::fs::read_to_string(root.join(TRACE_CONFIG)).expect("read membership trace config");
        validate_trace_contract_sources(&symbols, &spec, &config)
            .expect("checked-in trace contract is exact");

        for mutated in [
            spec.replace("/\\ WF_traceVars(TraceNext)", "/\\ TRUE"),
            spec.replace("/\\ InstallSnapshot", "/\\ TRUE"),
            spec.replace("\\/ TraceAction28", "\\/ TraceAction27"),
            spec.replace("/\\ Timeout(n1)", "/\\ TRUE"),
            spec.replace("/\\ ClientAppend(n1, v1)", "/\\ TRUE"),
            spec.replace(
                "TraceComplete == traceStep = 45",
                "TraceComplete == traceStep \\in 44..45",
            ),
        ] {
            assert!(validate_trace_contract_sources(&symbols, &mutated, &config).is_err());
        }
        assert!(validate_trace_contract_sources(
            &symbols,
            &spec,
            &config.replace("  LeaderCompleteness\n", "")
        )
        .is_err());
        assert!(validate_trace_contract_sources(
            &symbols,
            &spec,
            &config.replace("  MaxLogLen = 6", "  MaxLogLen = 5")
        )
        .is_err());
    }
}
