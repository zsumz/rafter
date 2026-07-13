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

pub(crate) const DEFAULT_FIXTURE_MODE: &str = "Default";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct DetectorProbe {
    pub(crate) predicate: &'static str,
    pub(crate) mode: &'static str,
}

pub(crate) const DETECTOR_PROBES: [DetectorProbe; 16] = [
    DetectorProbe {
        predicate: "ElectionSafety",
        mode: DEFAULT_FIXTURE_MODE,
    },
    DetectorProbe {
        predicate: "LogMatching",
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
        mode: DEFAULT_FIXTURE_MODE,
    },
    DetectorProbe {
        predicate: "LeaderCompleteness",
        mode: "LeaderCompletenessRecorderOnly",
    },
    DetectorProbe {
        predicate: "CommittedPrefixStability",
        mode: DEFAULT_FIXTURE_MODE,
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
        mode: DEFAULT_FIXTURE_MODE,
    },
    DetectorProbe {
        predicate: "CommittedEntriesHaveQuorum",
        mode: "CommitQuorumRecorderOnly",
    },
    DetectorProbe {
        predicate: "ReadBarrierLinearizability",
        mode: DEFAULT_FIXTURE_MODE,
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

struct Frame {
    code: u16,
    class: u8,
    body: String,
}

pub(crate) fn parse(bytes: &[u8]) -> Result<TlcSummary, String> {
    let source = String::from_utf8(bytes.to_vec())
        .map_err(|error| format!("TLC tool output is not UTF-8: {error}"))?;
    let frames = parse_frames(&source)?;
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
                let (generated, distinct, left) = parse_state_counts(&frame.body)
                    .ok_or("TLC 2199 frame has malformed state statistics")?;
                summary.generated_states = generated;
                summary.distinct_states = distinct;
                summary.states_left = left;
            }
            2194 => {
                depth_frames += 1;
                summary.search_depth = parse_search_depth(&frame.body)
                    .ok_or("TLC 2194 frame has malformed search depth")?;
            }
            2186 => {
                finished_frames += 1;
                summary.process_finished = true;
            }
            2107 | 2110 => {
                let invariant = parse_violated_invariant(&frame.body)
                    .ok_or("TLC violation frame omitted invariant name")?;
                if summary
                    .violated_invariant
                    .as_ref()
                    .is_some_and(|previous| previous != &invariant)
                {
                    return Err("TLC reported multiple distinct invariant violations".to_owned());
                }
                summary.violated_invariant = Some(invariant);
            }
            _ => {}
        }
    }
    if success_frames > 1 || statistics_frames > 1 || depth_frames > 1 || finished_frames > 1 {
        return Err("TLC tool output duplicated a terminal frame".to_owned());
    }
    Ok(summary)
}

fn parse_frames(source: &str) -> Result<Vec<Frame>, String> {
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
    if current.is_some() {
        return Err("truncated TLC tool frame".to_owned());
    }
    if frames.is_empty() {
        return Err("TLC output contained no tool frames".to_owned());
    }
    Ok(frames)
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
mod tests {
    use super::{parse, TlcSummary};

    #[test]
    fn parses_framed_success_with_grouped_counts() {
        let summary = parse(
            b"@!@!@STARTMSG 2193:0 @!@!@\nNo error.\n@!@!@ENDMSG 2193 @!@!@\n\
              @!@!@STARTMSG 2199:0 @!@!@\n130,123,456 states generated, 120,000,001 distinct states found, 0 states left on queue.\n@!@!@ENDMSG 2199 @!@!@\n\
              @!@!@STARTMSG 2194:0 @!@!@\nThe depth of the complete state graph search is 19.\n@!@!@ENDMSG 2194 @!@!@\n\
              @!@!@STARTMSG 2186:0 @!@!@\nFinished.\n@!@!@ENDMSG 2186 @!@!@\n",
        )
        .expect("framed output parses");
        assert_eq!(
            summary,
            TlcSummary {
                generated_states: 130_123_456,
                distinct_states: 120_000_001,
                states_left: 0,
                search_depth: 19,
                completed_without_error: true,
                process_finished: true,
                violated_invariant: None,
            }
        );
    }

    #[test]
    fn extracts_named_invariant_counterexample() {
        let summary = parse(
            b"@!@!@STARTMSG 2107:1 @!@!@\nInvariant ElectionSafety is violated.\n@!@!@ENDMSG 2107 @!@!@\n",
        )
        .expect("violation frame parses");
        assert_eq!(
            summary.violated_invariant.as_deref(),
            Some("ElectionSafety")
        );
        assert!(!summary.completed_without_error);
    }

    #[test]
    fn rejects_truncated_or_duplicate_terminal_frames() {
        assert!(parse(b"@!@!@STARTMSG 2193:0 @!@!@\n").is_err());
        assert!(parse(
            b"@!@!@STARTMSG 2193:0 @!@!@\nA\n@!@!@ENDMSG 2193 @!@!@\n\
              @!@!@STARTMSG 2193:0 @!@!@\nB\n@!@!@ENDMSG 2193 @!@!@\n"
        )
        .is_err());
    }

    #[test]
    fn rejects_unframed_success_prose() {
        assert!(parse(
            b"Model checking completed. No error has been found. 120,000,001 distinct states found."
        )
        .is_err());
    }
}
