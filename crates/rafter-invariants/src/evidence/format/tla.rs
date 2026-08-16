//! Neutral decoding of framed TLC progress, terminal, and counterexample output.

pub(crate) mod checkpoint;
mod mutation;

pub(crate) use mutation::{
    parse_mutation_transcript, MutationSummary, MUTATION_SUITE_ARTIFACT_KIND, MUTATION_SUITE_LABEL,
};

pub(crate) const REGISTERED_PREDICATES: [&str; 8] = [
    "ElectionSafety",
    "LogMatching",
    "LeaderCompleteness",
    "CommittedPrefixStability",
    "StateMachineSafety",
    "StaleLeaderFencing",
    "CommittedEntriesHaveQuorum",
    "ReadBarrierLinearizability",
];

pub(crate) const REQUIRED_MODEL_TRANSITIONS: [&str; 18] = [
    "Timeout",
    "SendRequestVote",
    "DeliverRequestVote",
    "BecomeLeader",
    "ClientAppend",
    "SendAppend",
    "DeliverAppend",
    "Commit",
    "Apply",
    "ApplicationStateLoss",
    "Restart",
    "CreateSnapshot",
    "TransferSnapshot",
    "InstallSnapshot",
    "EnterJoint",
    "LeaveJoint",
    "RegisterRead",
    "GrantRead",
];

// One lower than they were: folding snapshot creation and compaction into one
// atomic action removed `CompactSnapshot` from the model, and with it the trace
// step that executed it. The trace is one step shorter, not one step weaker --
// it still executes every transition in REQUIRED_MODEL_TRANSITIONS.
pub(crate) const MEMBERSHIP_TRACE_MIN_DISTINCT_STATES: u64 = 45;
pub(crate) const MEMBERSHIP_TRACE_MIN_DEPTH: u64 = 45;
pub(crate) const DEFAULT_FIXTURE_MODE: &str = "Default";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DetectorProbe {
    pub(crate) predicate: &'static str,
    pub(crate) mode: &'static str,
}

pub(crate) const DETECTOR_PROBES: [DetectorProbe; 11] = [
    DetectorProbe {
        predicate: "ElectionSafety",
        mode: DEFAULT_FIXTURE_MODE,
    },
    DetectorProbe {
        predicate: "LogMatching",
        mode: "LogMatchingRecorderOnly",
    },
    DetectorProbe {
        predicate: "LogMatching",
        mode: "SnapshotPrefixRecorderOnly",
    },
    DetectorProbe {
        predicate: "LeaderCompleteness",
        mode: "LeaderCompletenessRecorderOnly",
    },
    DetectorProbe {
        predicate: "CommittedPrefixStability",
        mode: "CommittedPrefixRecorderOnly",
    },
    DetectorProbe {
        predicate: "StateMachineSafety",
        mode: DEFAULT_FIXTURE_MODE,
    },
    DetectorProbe {
        predicate: "StateMachineSafety",
        mode: "ApplicationEpochRecorderOnly",
    },
    DetectorProbe {
        predicate: "StaleLeaderFencing",
        mode: "HigherTermRecorderOnly",
    },
    DetectorProbe {
        predicate: "StaleLeaderFencing",
        mode: "StaleAuthorityRecorderOnly",
    },
    DetectorProbe {
        predicate: "CommittedEntriesHaveQuorum",
        mode: "CommitQuorumRecorderOnly",
    },
    DetectorProbe {
        predicate: "ReadBarrierLinearizability",
        mode: "ReadBarrierRecorderOnly",
    },
];

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

/// Observation metrics every executed proof obligation contributes to the
/// layer receipt, in the order a reader wants them: what it explored, how deep
/// it went, and whether it actually drained its queue.
pub(crate) const OBLIGATION_METRICS: [&str; 4] = [
    "generated_states",
    "distinct_states",
    "search_depth",
    "frontier_exhausted",
];

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

fn is_registered_predicate(predicate: &str) -> bool {
    REGISTERED_PREDICATES.contains(&predicate)
}

fn is_registered_probe(probe: DetectorProbe) -> bool {
    DETECTOR_PROBES.contains(&probe)
}

fn is_valid_fixture_probe(probe: DetectorProbe) -> bool {
    is_registered_predicate(probe.predicate)
        && (probe.mode == DEFAULT_FIXTURE_MODE
            || matches!(
                (probe.predicate, probe.mode),
                ("ElectionSafety", "ElectionRecorderOnly")
                    | (
                        "LogMatching",
                        "LogMatchingRecorderOnly" | "SnapshotPrefixRecorderOnly"
                    )
                    | ("LeaderCompleteness", "LeaderCompletenessRecorderOnly")
                    | ("CommittedPrefixStability", "CommittedPrefixRecorderOnly")
                    | (
                        "StateMachineSafety",
                        "ApplicationRecorderOnly" | "ApplicationEpochRecorderOnly"
                    )
                    | (
                        "StaleLeaderFencing",
                        "HigherTermRecorderOnly" | "StaleAuthorityRecorderOnly"
                    )
                    | ("CommittedEntriesHaveQuorum", "CommitQuorumRecorderOnly")
                    | ("ReadBarrierLinearizability", "ReadBarrierRecorderOnly")
            ))
}

fn artifact_kind(prefix: &str, probe: DetectorProbe) -> String {
    if probe.mode == DEFAULT_FIXTURE_MODE {
        format!("{prefix}:{}", probe.predicate)
    } else {
        format!("{prefix}:{}:{}", probe.predicate, probe.mode)
    }
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

fn parse_summary(bytes: &[u8], allow_trailing_frame: bool) -> Result<TlcSummary, String> {
    let source = String::from_utf8(bytes.to_vec())
        .map_err(|error| format!("TLC tool output is not UTF-8: {error}"))?;
    let frames = parse_frames(&source, allow_trailing_frame)?;
    summarize_frames(frames).map_err(|error| error.message.to_owned())
}

fn summarize_frames(frames: Vec<Frame>) -> Result<TlcSummary, SummaryError> {
    let mut summary = TlcSummary::default();
    let mut success_frames = 0;
    let mut statistics_frames = 0;
    let mut depth_frames = 0;
    let mut finished_frames = 0;
    for frame in frames {
        if frame.class != 0 && !matches!(frame.code, 2107 | 2110) {
            continue;
        }
        match frame.code {
            2193 => {
                success_frames += 1;
                summary.completed_without_error = true;
            }
            2199 => {
                statistics_frames += 1;
                let Some((generated, distinct, left)) = parse_state_counts(&frame.body) else {
                    return Err(SummaryError {
                        summary,
                        message: "TLC 2199 frame has malformed state statistics",
                    });
                };
                summary.generated_states = generated;
                summary.distinct_states = distinct;
                summary.states_left = left;
            }
            2194 => {
                depth_frames += 1;
                let Some(search_depth) = parse_search_depth(&frame.body) else {
                    return Err(SummaryError {
                        summary,
                        message: "TLC 2194 frame has malformed search depth",
                    });
                };
                summary.search_depth = search_depth;
            }
            2186 => {
                finished_frames += 1;
                summary.process_finished = true;
            }
            2107 | 2110 => {
                let Some(invariant) = parse_violated_invariant(&frame.body) else {
                    return Err(SummaryError {
                        summary,
                        message: "TLC violation frame omitted invariant name",
                    });
                };
                if summary
                    .violated_invariant
                    .as_ref()
                    .is_some_and(|previous| previous != &invariant)
                {
                    return Err(SummaryError {
                        summary,
                        message: "TLC reported multiple distinct invariant violations",
                    });
                }
                summary.violated_invariant = Some(invariant);
            }
            _ => {}
        }
    }
    if success_frames > 1 || statistics_frames > 1 || depth_frames > 1 || finished_frames > 1 {
        return Err(SummaryError {
            summary,
            message: "TLC tool output duplicated a terminal frame",
        });
    }
    Ok(summary)
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

fn parse_frames(source: &str, allow_trailing_frame: bool) -> Result<Vec<Frame>, String> {
    let mut frames = Vec::new();
    let mut current: Option<(u16, u8, Vec<&str>)> = None;
    for line in source.lines() {
        if let Some(header) = line
            .strip_prefix("@!@!@STARTMSG ")
            .and_then(|line| line.strip_suffix(" @!@!@"))
        {
            if current.is_some() {
                return Err("nested TLC tool frame".to_owned());
            }
            let (code, class) = header
                .split_once(':')
                .ok_or("malformed TLC tool frame header")?;
            current = Some((
                code.parse().map_err(|_| "invalid TLC message code")?,
                class.parse().map_err(|_| "invalid TLC message class")?,
                Vec::new(),
            ));
            continue;
        }
        if let Some(footer) = line
            .strip_prefix("@!@!@ENDMSG ")
            .and_then(|line| line.strip_suffix(" @!@!@"))
        {
            let (code, class, body) = current
                .take()
                .ok_or("TLC tool frame ended without a start")?;
            if footer.parse::<u16>().ok() != Some(code) {
                return Err("TLC tool frame code mismatch".to_owned());
            }
            frames.push(Frame {
                code,
                class,
                body: body.join("\n"),
            });
            continue;
        }
        if let Some((_, _, body)) = current.as_mut() {
            body.push(line);
        }
    }
    if current.is_some() && !allow_trailing_frame {
        return Err("truncated TLC tool frame".to_owned());
    }
    if frames.is_empty() {
        return Err("TLC output contained no tool frames".to_owned());
    }
    Ok(frames)
}

fn parse_frame_prefix(source: &str) -> Result<Vec<Frame>, String> {
    let mut frames = Vec::new();
    let mut current: Option<(u16, u8, Vec<&str>)> = None;
    for line in source.lines() {
        if let Some(header) = line
            .strip_prefix("@!@!@STARTMSG ")
            .and_then(|line| line.strip_suffix(" @!@!@"))
        {
            if current.is_some() {
                break;
            }
            let Some((code, class)) = header.split_once(':') else {
                break;
            };
            let (Ok(code), Ok(class)) = (code.parse(), class.parse()) else {
                break;
            };
            current = Some((code, class, Vec::new()));
            continue;
        }
        if let Some(footer) = line
            .strip_prefix("@!@!@ENDMSG ")
            .and_then(|line| line.strip_suffix(" @!@!@"))
        {
            let Some((code, class, body)) = current.take() else {
                break;
            };
            if footer.parse::<u16>().ok() != Some(code) {
                break;
            }
            frames.push(Frame {
                code,
                class,
                body: body.join("\n"),
            });
            continue;
        }
        if let Some((_, _, body)) = current.as_mut() {
            body.push(line);
        }
    }
    if frames.is_empty() {
        return Err("TLC output contained no complete tool-frame prefix".to_owned());
    }
    Ok(frames)
}

fn parse_progress(body: &str) -> Option<TlcProgress> {
    let line = body
        .lines()
        .find(|line| line.trim().starts_with("Progress("))?;
    let line = line.trim();
    let depth = line
        .strip_prefix("Progress(")?
        .split_once(')')?
        .0
        .parse()
        .ok()?;
    let (_, statistics) = line.split_once(": ")?;
    let (generated, statistics) = statistics.split_once(" states generated (")?;
    let (_, statistics) = statistics.split_once("), ")?;
    let (distinct, statistics) = statistics.split_once(" distinct states found (")?;
    let (_, states_left) = statistics.split_once("), ")?;
    let states_left = states_left.strip_suffix(" states left on queue.")?;
    Some(TlcProgress {
        generated_states: parse_u64(generated)?,
        distinct_states: parse_u64(distinct)?,
        states_left: parse_u64(states_left)?,
        depth,
    })
}

fn parse_state_counts(body: &str) -> Option<(u64, u64, u64)> {
    let line = body
        .lines()
        .find(|line| line.contains(" states generated, "))?;
    let line = line.trim();
    let (generated, rest) = line.split_once(" states generated, ")?;
    let (distinct, left) = rest.split_once(" distinct states found, ")?;
    let left = left.strip_suffix(" states left on queue.")?;
    Some((
        parse_u64(generated)?,
        parse_u64(distinct)?,
        parse_u64(left)?,
    ))
}

fn parse_search_depth(body: &str) -> Option<u64> {
    body.lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("The depth of the complete state graph search is ")
        })?
        .strip_suffix('.')
        .and_then(parse_u64)
}

fn parse_violated_invariant(body: &str) -> Option<String> {
    body.lines().find_map(|line| {
        let (_, rest) = line.split_once("Invariant ")?;
        let (name, _) = rest.split_once(" is violated")?;
        (!name.trim().is_empty()).then(|| name.trim().to_owned())
    })
}

fn parse_u64(value: &str) -> Option<u64> {
    value.replace(',', "").trim().parse().ok()
}

#[cfg(test)]
#[path = "tla_tests.rs"]
mod tests;
