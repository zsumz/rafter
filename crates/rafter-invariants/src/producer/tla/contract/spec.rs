//! Exact TLA+ specification, trace, detector, and symmetry contracts.

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fs,
    path::Path,
};

use sha2::{Digest, Sha256};

use super::super::tla_output::{
    render_detector_config, DETECTOR_PROBES, REGISTERED_PREDICATES, REQUIRED_MODEL_TRANSITIONS,
};

pub(in crate::producer::tla::contract) const SPEC: &str = "specs/tla/raft/Raft.tla";
pub(super) const TRACE_SPEC: &str = "specs/tla/raft/RaftMembershipTraceSample.tla";
pub(super) const TRACE_CONFIG: &str = "specs/tla/raft/RaftMembershipTraceSample.cfg";
const TRACE_SPEC_SHA256: &str = "c28b7e336153af62713ab0fa0a05b5a794ad378512d46d3fc42cacc57e2e0436";
const TRACE_CONFIG_SHA256: &str =
    "1286edee2df96b702937d9c1340f8412c060a6e9a0df53dd46b0149d2027b96e";
pub(super) const DETECTOR_SPEC: &str = "specs/tla/raft/RafterInvariantDetectorNegative.tla";
pub(super) const DETECTOR_CONFIG: &str = "specs/tla/raft/RafterInvariantDetectorNegative.cfg";

pub(in crate::producer::tla) fn validate_spec_contract(
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

pub(super) fn validate_trace_contract_sources(
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
        "TraceAction43 ==",
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
        "TraceComplete == traceStep = 44",
        "TraceCompletes == <>TraceComplete",
        "\\/ /\\ traceStep = 44",
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
    for step in 0..=43 {
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

pub(in crate::producer::tla::contract) fn validate_symmetry_contract(
    config_name: &str,
    config: &str,
) -> Result<(), Box<dyn Error>> {
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

pub(in crate::producer::tla::contract) fn validate_safety_only_boundary(
    spec: &str,
    config: &str,
) -> Result<(), Box<dyn Error>> {
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

pub(in crate::producer::tla::contract) fn configured_invariants(source: &str) -> Vec<String> {
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
