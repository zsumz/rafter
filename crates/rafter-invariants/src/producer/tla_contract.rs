use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::Path,
    process::Command,
    time::Duration,
};

use crate::SourceReceipt;

use super::{artifact, process};

const SPEC: &str = "specs/tla/raft/Raft.tla";
const TRACE_SPEC: &str = "specs/tla/raft/RaftTraceSample.tla";
const DETECTOR_SPEC: &str = "specs/tla/raft/RafterInvariantDetectorNegative.tla";
const DETECTOR_CONFIG: &str = "specs/tla/raft/RafterInvariantDetectorNegative.cfg";
const JAR: &str = "tools/cache/tla2tools.jar";

use super::tla_output::{render_detector_config, REGISTERED_PREDICATES};

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
    let configured = configured_invariants(&fs::read_to_string(&config)?);
    let configured_set = configured.iter().cloned().collect::<BTreeSet<_>>();
    let mut expected = symbols.clone();
    expected.insert("TypeOK".to_owned());
    let spec = fs::read_to_string(SPEC)?;
    let detector_spec = fs::read_to_string(DETECTOR_SPEC)?;
    let detector_config = fs::read_to_string(DETECTOR_CONFIG)?;
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
        || REGISTERED_PREDICATES.iter().any(|predicate| {
            detector_spec
                .lines()
                .any(|line| line.trim_start().starts_with(&format!("{predicate} ==")))
                || render_detector_config(&detector_config, predicate).is_err()
        })
    {
        return Err(
            "TLA config/spec/detector does not contain exactly the registry predicates".into(),
        );
    }
    Ok(configured)
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

    use super::super::tla_output::{render_detector_config, REGISTERED_PREDICATES};
    use super::{configured_invariants, java_major, validate_runner_options};

    #[test]
    fn java_major_is_parsed_exactly() {
        assert_eq!(java_major("java 21.0.5 2024-10-15 LTS"), Some(21));
        assert_eq!(java_major("openjdk 21.0.7 2025-04-15"), Some(21));
        assert_eq!(java_major("java version \"1.8.0_402\""), Some(8));
        assert_eq!(java_major("java 210.0.1"), Some(210));
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
    fn every_invariant_block_is_part_of_the_exact_contract() {
        let config = "INVARIANTS\n  TypeOK\n\nCHECK_DEADLOCK FALSE\n\nINVARIANT ElectionSafety\n";
        assert_eq!(
            configured_invariants(config),
            vec!["TypeOK".to_owned(), "ElectionSafety".to_owned()]
        );
    }

    #[test]
    fn detector_configs_bind_one_unique_counterexample_identity() {
        let template = "INIT FixtureInit\nCONSTANT TargetPredicate = \"ElectionSafety\"\nINVARIANT TypeOK\nINVARIANT ElectionSafety\n";
        let rendered = REGISTERED_PREDICATES
            .iter()
            .map(|predicate| {
                let config = render_detector_config(template, predicate).expect("valid template");
                assert!(config.contains(&format!("CONSTANT TargetPredicate = \"{predicate}\"")));
                assert!(config.contains(&format!("INVARIANT {predicate}")));
                config
            })
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(rendered.len(), REGISTERED_PREDICATES.len());
        assert!(render_detector_config(template, "ExpectedViolation").is_err());
        assert!(render_detector_config("INIT Init\n", "ElectionSafety").is_err());
    }
}
