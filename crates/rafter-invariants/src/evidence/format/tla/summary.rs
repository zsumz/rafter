//! Reduction of framed TLC output into summary and progress observations.
//!
//! Terminal frames are accepted at most once each, and a violation frame is
//! carried on the error so a counterexample run still reports the invariant it
//! broke.

use super::{parse_frames, Frame, SummaryError, TlcProgress, TlcSummary};

pub(super) fn parse_summary(
    bytes: &[u8],
    allow_trailing_frame: bool,
) -> Result<TlcSummary, String> {
    let source = String::from_utf8(bytes.to_vec())
        .map_err(|error| format!("TLC tool output is not UTF-8: {error}"))?;
    let frames = parse_frames(&source, allow_trailing_frame)?;
    summarize_frames(frames).map_err(|error| error.message.to_owned())
}

pub(super) fn summarize_frames(frames: Vec<Frame>) -> Result<TlcSummary, SummaryError> {
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

pub(super) fn parse_progress(body: &str) -> Option<TlcProgress> {
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
