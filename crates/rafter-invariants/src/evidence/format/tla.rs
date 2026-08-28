//! Neutral decoding of framed TLC progress, terminal, and counterexample output.

pub(crate) mod checkpoint;
mod frames;
mod mutation;
mod probes;
mod summary;
mod vocabulary;

use frames::{parse_frame_prefix, parse_frames};
pub(crate) use mutation::{
    parse_mutation_transcript, MutationSummary, MUTATION_SUITE_ARTIFACT_KIND, MUTATION_SUITE_LABEL,
};
use probes::{artifact_kind, is_registered_predicate, is_registered_probe, is_valid_fixture_probe};
pub(crate) use probes::{DetectorProbe, DETECTOR_PROBES};
use summary::{parse_progress, parse_summary, summarize_frames};
pub(crate) use vocabulary::{
    DEFAULT_FIXTURE_MODE, MEMBERSHIP_TRACE_MIN_DEPTH, MEMBERSHIP_TRACE_MIN_DISTINCT_STATES,
    OBLIGATION_METRICS, REGISTERED_PREDICATES, REQUIRED_MODEL_TRANSITIONS,
};

pub(crate) fn detector_invariant(probe: DetectorProbe) -> Option<String> {
    is_valid_fixture_probe(probe).then(|| probe.predicate.to_owned())
}

pub(crate) fn detector_label(probe: DetectorProbe) -> Option<String> {
    is_registered_probe(probe).then(|| format!("detector-negative-{}", probe_slug(probe)))
}

pub(crate) fn detector_log_kind(probe: DetectorProbe) -> Option<String> {
    is_registered_probe(probe).then(|| artifact_kind("tla-detector-log", probe))
}

pub(crate) fn detector_config_kind(probe: DetectorProbe) -> Option<String> {
    is_registered_probe(probe).then(|| artifact_kind("tla-detector-config", probe))
}

pub(crate) fn detector_observation(predicate: &str) -> Option<String> {
    is_registered_predicate(predicate).then(|| format!("detector_qualified:{predicate}"))
}

/// Process label and log identity for one obligation. Obligations are named
/// rather than positional so a receipt stays readable when the reviewed set
/// changes, and so producer and verifier agree without sharing an index.
pub(crate) fn obligation_label(id: &str) -> String {
    format!("obligation-{id}")
}

pub(crate) fn obligation_log_kind(id: &str) -> String {
    format!("tla-obligation-log:{id}")
}

pub(crate) fn obligation_config_kind(id: &str) -> String {
    format!("tla-obligation-config:{id}")
}

pub(crate) fn obligation_observation(id: &str, metric: &str) -> String {
    format!("obligation_{metric}:{id}")
}

/// Independent acceptance predicate for one obligation summary, shared as a
/// serialized-vocabulary helper so producer and verifier cannot drift on what
/// "discharged" means. Both call it on evidence they parsed themselves.
pub(crate) fn obligation_discharged(
    summary: &TlcSummary,
    minimum_generated_states: u64,
    minimum_distinct_states: u64,
) -> bool {
    summary.completed_without_error
        && summary.process_finished
        && summary.violated_invariant.is_none()
        && summary.states_left == 0
        && summary.search_depth > 0
        && summary.generated_states >= minimum_generated_states
        && summary.distinct_states >= minimum_distinct_states
}

/// Observation frame for one executed obligation, derived only from its parsed
/// TLC summary so that the producer's frame and the verifier's rederivation are
/// the same function of the same bytes.
pub(crate) fn obligation_observations(
    id: &str,
    summary: &TlcSummary,
    discharged: bool,
) -> [(String, u64); OBLIGATION_METRICS.len()] {
    [
        (
            obligation_observation(id, "generated_states"),
            summary.generated_states,
        ),
        (
            obligation_observation(id, "distinct_states"),
            summary.distinct_states,
        ),
        (
            obligation_observation(id, "search_depth"),
            summary.search_depth,
        ),
        (
            obligation_observation(id, "frontier_exhausted"),
            u64::from(discharged),
        ),
    ]
}

pub(crate) fn probe_slug(probe: DetectorProbe) -> String {
    if probe.mode == DEFAULT_FIXTURE_MODE {
        probe.predicate.to_owned()
    } else {
        format!("{}-{}", probe.predicate, probe.mode)
    }
}

pub(crate) fn render_detector_config(
    template: &str,
    probe: DetectorProbe,
) -> Result<String, String> {
    let invariant = detector_invariant(probe).ok_or_else(|| {
        format!(
            "unregistered TLA detector probe {}:{}",
            probe.predicate, probe.mode
        )
    })?;
    let mut target_lines = 0;
    let mut mode_lines = 0;
    let mut invariant_lines = 0;
    let mut rendered = Vec::new();
    for line in template.lines() {
        let indentation = &line[..line.len() - line.trim_start().len()];
        let trimmed = line.trim();
        if trimmed.starts_with("CONSTANT TargetPredicate = ")
            || trimmed.starts_with("TargetPredicate = ")
        {
            target_lines += 1;
            let declaration = if trimmed.starts_with("CONSTANT ") {
                "CONSTANT TargetPredicate"
            } else {
                "TargetPredicate"
            };
            rendered.push(format!(
                "{indentation}{declaration} = \"{}\"",
                probe.predicate
            ));
        } else if trimmed.starts_with("CONSTANT FixtureMode = ")
            || trimmed.starts_with("FixtureMode = ")
        {
            mode_lines += 1;
            let declaration = if trimmed.starts_with("CONSTANT ") {
                "CONSTANT FixtureMode"
            } else {
                "FixtureMode"
            };
            rendered.push(format!("{indentation}{declaration} = \"{}\"", probe.mode));
        } else if trimmed.starts_with("INVARIANT ") && trimmed != "INVARIANT TypeOK" {
            invariant_lines += 1;
            rendered.push(format!("{indentation}INVARIANT {invariant}"));
        } else {
            rendered.push(line.to_owned());
        }
    }
    if target_lines != 1 || mode_lines != 1 || invariant_lines != 1 {
        return Err(
            "TLA detector config must contain one target, fixture mode, and invariant".to_owned(),
        );
    }
    let mut rendered = rendered.join("\n");
    if template.ends_with('\n') {
        rendered.push('\n');
    }
    Ok(rendered)
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct TlcSummary {
    pub generated_states: u64,
    pub distinct_states: u64,
    pub states_left: u64,
    pub search_depth: u64,
    pub completed_without_error: bool,
    pub process_finished: bool,
    pub violated_invariant: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TlcProgress {
    pub generated_states: u64,
    pub distinct_states: u64,
    pub states_left: u64,
    pub depth: u64,
}

struct Frame {
    code: u16,
    class: u8,
    body: String,
}

struct SummaryError {
    summary: TlcSummary,
    message: &'static str,
}

pub(crate) fn parse(bytes: &[u8]) -> Result<TlcSummary, String> {
    parse_summary(bytes, false)
}

pub(crate) fn parse_complete_prefix(bytes: &[u8]) -> Result<TlcSummary, String> {
    let source = String::from_utf8(bytes.to_vec())
        .map_err(|error| format!("TLC tool output is not UTF-8: {error}"))?;
    let frames = parse_frame_prefix(&source)?;
    match summarize_frames(frames) {
        Ok(summary) => Ok(summary),
        Err(error) if error.summary.violated_invariant.is_some() => Ok(error.summary),
        Err(error) => Err(error.message.to_owned()),
    }
}

pub(crate) fn parse_latest_progress(bytes: &[u8]) -> Result<Option<TlcProgress>, String> {
    let source = String::from_utf8(bytes.to_vec())
        .map_err(|error| format!("TLC tool output is not UTF-8: {error}"))?;
    let frames = parse_frames(&source, true)?;
    let mut latest = None;
    for frame in frames {
        if frame.code != 2200 || frame.class != 0 {
            continue;
        }
        latest = Some(
            parse_progress(&frame.body)
                .ok_or("TLC 2200 frame has malformed progress statistics")?,
        );
    }
    Ok(latest)
}

#[cfg(test)]
#[path = "tla_tests.rs"]
mod tests;
