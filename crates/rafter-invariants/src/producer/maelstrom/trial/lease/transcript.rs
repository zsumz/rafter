//! Ordered lease-isolation transcript and retained history qualification.

use super::{
    super::model::{LeaseTranscriptStatus, ScenarioMarkers},
    history,
    marker::LeaseMarker,
};

pub(in crate::producer) fn finish_lease_transcript(
    markers: &mut ScenarioMarkers,
    events: &[LeaseMarker],
    parse_errors: u64,
) {
    let derived = match validate_lease_transcript(events) {
        Ok(LeaseTranscriptStatus::Complete) => {
            markers.lease_sequence_complete = 1;
            LeaseTranscriptStatus::Complete
        }
        Ok(LeaseTranscriptStatus::Missing) => LeaseTranscriptStatus::Missing,
        Ok(status) => {
            markers.lease_sequence_invalid = 1;
            status
        }
        Err(()) => {
            markers.lease_sequence_invalid = 1;
            LeaseTranscriptStatus::HarnessError
        }
    };
    if parse_errors > 0 {
        markers.lease_sequence_complete = 0;
        markers.lease_sequence_invalid =
            markers.lease_sequence_invalid.saturating_add(parse_errors);
        markers.lease_status = if matches!(
            derived,
            LeaseTranscriptStatus::Violation | LeaseTranscriptStatus::ViolationWithHarnessError
        ) {
            LeaseTranscriptStatus::ViolationWithHarnessError
        } else {
            LeaseTranscriptStatus::HarnessError
        };
    } else {
        markers.lease_status = derived;
    }
}

pub(in crate::producer) fn bind_lease_history(
    markers: &mut ScenarioMarkers,
    events: &[LeaseMarker],
    source: Option<&str>,
) {
    if markers.lease_status != LeaseTranscriptStatus::Complete {
        return;
    }
    let probe = events.iter().find(|event| event.phase == "read-buffered");
    let matches = probe
        .zip(source)
        .and_then(|(probe, source)| {
            history::probe_completion_count(source, &probe.client, probe.msg_id).ok()
        })
        .unwrap_or_default();
    if matches == 1 {
        markers.lease_history_probe_matches = 1;
    } else {
        markers.lease_history_probe_mismatches = 1;
        markers.lease_sequence_complete = 0;
        markers.lease_sequence_invalid = 1;
        markers.lease_status = LeaseTranscriptStatus::HarnessError;
    }
}

pub(in crate::producer) fn validate_lease_transcript(
    events: &[LeaseMarker],
) -> Result<LeaseTranscriptStatus, ()> {
    if events.is_empty() {
        return Ok(LeaseTranscriptStatus::Missing);
    }
    let first = &events[0];
    if first.phase != "fast-path-read-ok" || first.source_node != first.node || first.seq != 1 {
        return Err(());
    }
    let node = first.node.as_str();
    let term = first.term;
    let fast = first.request();
    let mut probe = None;
    let mut expired = false;
    let mut released = false;
    let mut handled = false;
    let mut terminal = None;
    let mut duplicate_terminal = false;
    for (index, event) in events.iter().enumerate().skip(1) {
        if event.seq != (index + 1) as u64
            || event.source_node != node
            || event.node != node
            || event.term != term
        {
            return Err(());
        }
        match event.phase.as_str() {
            "lease-expired"
                if !expired && probe.is_none() && !released && event.request() == fast =>
            {
                expired = true;
            }
            "read-buffered" if expired && probe.is_none() && !released => {
                if event.request() == fast {
                    return Err(());
                }
                probe = Some(event.request());
            }
            "post-expiry-released" if expired && probe == Some(event.request()) && !released => {
                released = true;
            }
            "post-expiry-handler" if released && probe == Some(event.request()) && !handled => {
                handled = true;
            }
            "post-expiry-unavailable"
                if handled && probe == Some(event.request()) && terminal.is_none() =>
            {
                terminal = Some(LeaseTranscriptStatus::Complete);
            }
            "post-expiry-read-served-violation"
                if handled && probe == Some(event.request()) && terminal.is_none() =>
            {
                terminal = Some(LeaseTranscriptStatus::Violation);
            }
            "post-expiry-renewed-violation"
                if expired && event.request() == probe.unwrap_or(fast) && terminal.is_none() =>
            {
                terminal = Some(LeaseTranscriptStatus::Violation);
            }
            "post-expiry-unexpected-error"
                if handled
                    && probe == Some(event.request())
                    && event.code.is_some()
                    && terminal.is_none() =>
            {
                terminal = Some(LeaseTranscriptStatus::HarnessError);
            }
            "post-expiry-duplicate-terminal"
                if released && probe == Some(event.request()) && !duplicate_terminal =>
            {
                duplicate_terminal = true;
            }
            "coverage-lost" if index + 1 == events.len() && event.reason.is_some() => {
                terminal = Some(LeaseTranscriptStatus::Incomplete);
            }
            _ => return Err(()),
        }
    }
    Ok(match (terminal, duplicate_terminal) {
        (Some(LeaseTranscriptStatus::Violation), true) => {
            LeaseTranscriptStatus::ViolationWithHarnessError
        }
        (Some(LeaseTranscriptStatus::Violation), false) => LeaseTranscriptStatus::Violation,
        (Some(LeaseTranscriptStatus::Complete), false) => LeaseTranscriptStatus::Complete,
        (Some(LeaseTranscriptStatus::Incomplete) | None, false) => {
            LeaseTranscriptStatus::Incomplete
        }
        (Some(LeaseTranscriptStatus::HarnessError), _) | (_, true) => {
            LeaseTranscriptStatus::HarnessError
        }
        (
            Some(LeaseTranscriptStatus::Missing | LeaseTranscriptStatus::ViolationWithHarnessError),
            _,
        ) => return Err(()),
    })
}
