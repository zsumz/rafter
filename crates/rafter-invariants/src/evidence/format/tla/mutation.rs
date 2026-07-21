//! Neutral decoding of Cargo's TLA+ mutation-suite transcript.

pub(crate) const MUTATION_SUITE_ARTIFACT_KIND: &str = "tla-mutation-log";
pub(crate) const MUTATION_SUITE_LABEL: &str = "detector-mutation-suite";
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct MutationSummary {
    pub passed: u64,
    pub failed: u64,
    pub ignored: u64,
    pub measured: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct MutationTranscript {
    pub running_counts: Vec<u64>,
    pub passed_tests: Vec<String>,
    pub summaries: Vec<MutationSummary>,
}

pub(crate) fn parse_mutation_transcript(stdout: &str) -> MutationTranscript {
    let mut transcript = MutationTranscript::default();
    for line in stdout.lines().map(str::trim) {
        if let Some(count) = line
            .strip_prefix("running ")
            .and_then(|value| value.strip_suffix(" tests"))
            .and_then(parse_canonical_decimal)
        {
            transcript.running_counts.push(count);
        }
        if let Some(test) = line
            .strip_prefix("test ")
            .and_then(|value| value.strip_suffix(" ... ok"))
        {
            transcript.passed_tests.push(test.to_owned());
        }
        if let Some(summary) = parse_summary(line) {
            transcript.summaries.push(summary);
        }
    }
    transcript
}

fn parse_summary(line: &str) -> Option<MutationSummary> {
    let fields = line.strip_prefix("test result: ok. ")?;
    let (passed, fields) = parse_count(fields, " passed; ")?;
    let (failed, fields) = parse_count(fields, " failed; ")?;
    let (ignored, fields) = parse_count(fields, " ignored; ")?;
    let (measured, _) = parse_count(fields, " measured;")?;
    Some(MutationSummary {
        passed,
        failed,
        ignored,
        measured,
    })
}

fn parse_count<'a>(fields: &'a str, separator: &str) -> Option<(u64, &'a str)> {
    let (count, remaining) = fields.split_once(separator)?;
    Some((parse_canonical_decimal(count)?, remaining))
}

fn parse_canonical_decimal(value: &str) -> Option<u64> {
    if value.is_empty()
        || (value.len() > 1 && value.starts_with('0'))
        || !value.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    value.parse().ok()
}

#[cfg(test)]
#[path = "mutation_tests.rs"]
mod tests;
