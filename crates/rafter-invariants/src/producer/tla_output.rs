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

pub(crate) const REQUIRED_MODEL_TRANSITIONS: [&str; 19] = [
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
    "CompactSnapshot",
    "EnterJoint",
    "LeaveJoint",
    "RegisterRead",
    "GrantRead",
];

pub(crate) const MEMBERSHIP_TRACE_MIN_DISTINCT_STATES: u64 = 46;
pub(crate) const MEMBERSHIP_TRACE_MIN_DEPTH: u64 = 46;
pub(crate) const MUTATION_SUITE_ARTIFACT_KIND: &str = "tla-mutation-log";
pub(crate) const MUTATION_SUITE_LABEL: &str = "detector-mutation-suite";
pub(crate) const REQUIRED_MUTATION_TESTS: [&str; 34] = [
    "application_epoch_loss_replays_identically_without_erasing_history",
    "applied_membership_quorum_mutation_breaks_joint_regression",
    "closed_term_election_history_is_retired_after_every_node_advances",
    "closed_term_prefix_history_retires_without_erasing_conflicts",
    "corrupted_snapshot_install_breaks_lifecycle_identity",
    "corrupted_snapshot_restored_state_breaks_empty_epoch_lifecycle",
    "delayed_append_uses_frozen_sender_authority_after_self_removal",
    "every_required_detector_probe_reaches_its_named_counterexample",
    "follower_recomputation_breaks_delayed_heartbeat_regression",
    "leader_completeness_uses_commit_authority_term",
    "missing_application_epoch_recorder_cannot_qualify_state_machine_safety",
    "missing_application_recorder_cannot_qualify_state_machine_safety",
    "missing_commit_ledger_recorder_cannot_qualify_history_predicates",
    "missing_commit_witness_recorder_cannot_qualify_quorum_predicate",
    "missing_effective_recomputation_breaks_overwrite_regression",
    "missing_election_recorder_cannot_qualify_election_safety",
    "missing_higher_term_recorder_cannot_qualify_fencing",
    "missing_log_prefix_recorder_cannot_qualify_log_or_snapshot_paths",
    "missing_read_grant_recorder_cannot_qualify_read_barrier_predicate",
    "missing_self_removal_step_down_breaks_commit_regression",
    "missing_stale_authority_recorder_cannot_qualify_fencing",
    "non_violating_fixture_cannot_qualify",
    "recorder_only_fixtures_qualify_before_mutation",
    "removed_candidate_vote_requires_membership_and_freshness_guards",
    "sanitized_application_result_cannot_qualify_detector_fixture",
    "self_removing_leader_commits_final_configuration_and_steps_down",
    "shorter_authoritative_log_repairs_an_uncommitted_suffix",
    "snapshot_compaction_pending_tracks_create_and_compact_transitions",
    "snapshot_lifecycle_preserves_logical_identity_through_restart",
    "stale_messages_are_retired_when_the_target_term_advances",
    "true_mutation_of_real_predicate_cannot_qualify",
    "unfrozen_effective_membership_breaks_commit_witness_regression",
    "unvalidated_commit_certificate_cannot_qualify_quorum_predicate",
    "unvalidated_read_grant_cannot_qualify_read_barrier_predicate",
];

pub(crate) fn mutation_suite_passed(exit_code: Option<i32>, timed_out: bool, stdout: &str) -> bool {
    if exit_code != Some(0) || timed_out {
        return false;
    }
    let expected_count = REQUIRED_MUTATION_TESTS.len();
    let running = format!("running {expected_count} tests");
    let result =
        format!("test result: ok. {expected_count} passed; 0 failed; 0 ignored; 0 measured;");
    stdout.lines().any(|line| line.trim() == running)
        && stdout.lines().any(|line| line.contains(&result))
        && REQUIRED_MUTATION_TESTS.iter().all(|name| {
            let expected = format!("test producer::tla_exec::mutation_tests::{name} ... ok");
            stdout
                .lines()
                .filter(|line| line.trim() == expected)
                .count()
                == 1
        })
}

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
mod tests {
    use super::{parse, parse_complete_prefix, parse_latest_progress, TlcProgress, TlcSummary};

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
    fn complete_violation_survives_a_truncated_trailing_frame() {
        let output = b"@!@!@STARTMSG 2110:1 @!@!@\nInvariant ElectionSafety is violated.\n@!@!@ENDMSG 2110 @!@!@\n\
@!@!@STARTMSG 2200:0 @!@!@\nProgress(3) at";
        assert!(parse(output).is_err());
        assert_eq!(
            parse_complete_prefix(output)
                .expect("parse complete frame prefix")
                .violated_invariant
                .as_deref(),
            Some("ElectionSafety")
        );
    }

    #[test]
    fn complete_violation_survives_later_malformed_terminal_frames() {
        let violation = "@!@!@STARTMSG 2110:1 @!@!@\nInvariant ElectionSafety is violated.\n@!@!@ENDMSG 2110 @!@!@\n";
        let cases = [
            (
                format!(
                    "{violation}@!@!@STARTMSG 2199:0 @!@!@\nmalformed statistics\n@!@!@ENDMSG 2199 @!@!@\n"
                ),
                "TLC 2199 frame has malformed state statistics",
                0,
            ),
            (
                format!(
                    "{violation}@!@!@STARTMSG 2199:0 @!@!@\n2 states generated, 2 distinct states found, 0 states left on queue.\n@!@!@ENDMSG 2199 @!@!@\n\
                     @!@!@STARTMSG 2194:0 @!@!@\nmalformed depth\n@!@!@ENDMSG 2194 @!@!@\n"
                ),
                "TLC 2194 frame has malformed search depth",
                2,
            ),
        ];

        for (output, expected_error, expected_states) in cases {
            assert_eq!(parse(output.as_bytes()).unwrap_err(), expected_error);
            let summary =
                parse_complete_prefix(output.as_bytes()).expect("recover named violation");
            assert_eq!(
                summary.violated_invariant.as_deref(),
                Some("ElectionSafety")
            );
            assert_eq!(summary.generated_states, expected_states);
        }

        assert!(parse_complete_prefix(
            b"@!@!@STARTMSG 2199:0 @!@!@\nmalformed statistics\n@!@!@ENDMSG 2199 @!@!@\n"
        )
        .is_err());
    }

    #[test]
    fn complete_violation_survives_later_framing_and_duplicate_defects() {
        let violation = "@!@!@STARTMSG 2110:1 @!@!@\nInvariant ElectionSafety is violated.\n@!@!@ENDMSG 2110 @!@!@\n";
        let statistics = "@!@!@STARTMSG 2199:0 @!@!@\n2 states generated, 2 distinct states found, 0 states left on queue.\n@!@!@ENDMSG 2199 @!@!@\n";
        let cases = [
            format!("{violation}{statistics}{statistics}"),
            format!(
                "{violation}@!@!@STARTMSG 2194:0 @!@!@\nThe depth of the complete state graph search is 2.\n@!@!@ENDMSG 2199 @!@!@\n"
            ),
            format!(
                "{violation}@!@!@STARTMSG 2194:0 @!@!@\n@!@!@STARTMSG 2199:0 @!@!@\n"
            ),
        ];

        for output in cases {
            assert!(parse(output.as_bytes()).is_err());
            assert_eq!(
                parse_complete_prefix(output.as_bytes())
                    .expect("recover named violation before parser defect")
                    .violated_invariant
                    .as_deref(),
                Some("ElectionSafety")
            );
        }
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

    #[test]
    fn parses_the_latest_complete_progress_frame() {
        let progress = parse_latest_progress(
            b"@!@!@STARTMSG 2200:0 @!@!@\nProgress(21) at 2026-07-13 19:18:31: 23,784,130 states generated (4,670,725 s/min), 6,246,309 distinct states found (1,150,848 ds/min), 3,294,097 states left on queue.\n@!@!@ENDMSG 2200 @!@!@\n\
              @!@!@STARTMSG 2200:0 @!@!@\nProgress(23) at 2026-07-13 19:52:32: 181,490,601 states generated (4,966,137 s/min), 40,062,465 distinct states found (1,000,915 ds/min), 19,012,042 states left on queue.\n@!@!@ENDMSG 2200 @!@!@\n\
              @!@!@STARTMSG 2200:0 @!@!@\nProgress(24) at",
        )
        .expect("complete progress frames parse")
        .expect("latest progress exists");
        assert_eq!(
            progress,
            TlcProgress {
                generated_states: 181_490_601,
                distinct_states: 40_062_465,
                states_left: 19_012_042,
                depth: 23,
            }
        );
    }

    #[test]
    fn rejects_a_malformed_complete_progress_frame() {
        assert!(parse_latest_progress(
            b"@!@!@STARTMSG 2200:0 @!@!@\nProgress without statistics.\n@!@!@ENDMSG 2200 @!@!@\n"
        )
        .is_err());
    }
}
