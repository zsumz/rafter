use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::Path,
    process::Command,
    time::Duration,
};

use crate::SourceReceipt;

use super::tla_checkpoint;
use super::{artifact, process};

const SPEC: &str = "specs/tla/raft/Raft.tla";
const TRACE_SPEC: &str = "specs/tla/raft/RaftTraceSample.tla";
const DETECTOR_SPEC: &str = "specs/tla/raft/RafterInvariantDetectorNegative.tla";
const DETECTOR_CONFIG: &str = "specs/tla/raft/RafterInvariantDetectorNegative.cfg";
const JAR: &str = "tools/cache/tla2tools.jar";

use super::tla_output::{render_detector_config, DETECTOR_PROBES, REGISTERED_PREDICATES};

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
    if tla_checkpoint::enabled(configuration) {
        for (name, expected) in [
            ("config", "Raft.cfg"),
            ("workers", "auto"),
            ("soft_timeout", "295m"),
            ("checkpoint_minutes", "30"),
            ("checkpoint_gzip", "required"),
            ("max_heap", "4g"),
            ("checkpoint_recovery", "strict-compatible-if-present"),
            ("unsymmetrized_exploration", "required"),
        ] {
            if required_configuration(configuration, name)? != expected {
                return Err(format!("checkpointed TLA runner requires {name}={expected}").into());
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
            Path::new("specs/tla/raft/RaftTraceSample.cfg"),
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
    let output = Command::new("scripts/tla-model-check")
        .arg("--fetch-tool")
        .env_clear()
        .envs(process::base_environment())
        .output()?;
    if !output.status.success() {
        return Err(format!(
            "fetch pinned TLC tool: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    Ok(())
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
    use std::collections::BTreeMap;

    use super::super::tla_output::{
        render_detector_config, DetectorProbe, DEFAULT_FIXTURE_MODE, DETECTOR_PROBES,
    };
    use super::{
        configured_invariants, java_major, validate_runner_options, validate_safety_only_boundary,
        validate_symmetry_contract,
    };

    #[test]
    fn java_major_is_parsed_exactly() {
        assert_eq!(java_major("java 21.0.5 2024-10-15 LTS"), Some(21));
        assert_eq!(java_major("openjdk 21.0.7 2025-04-15"), Some(21));
        assert_eq!(java_major("java version \"1.8.0_402\""), Some(8));
        assert_eq!(java_major("java 210.0.1"), Some(210));
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
}
