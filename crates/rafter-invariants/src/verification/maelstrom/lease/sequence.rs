//! Ordered lease-isolation state-machine rederivation.

use super::{marker::LeaseMarker, LeaseArtifactStatus};

pub(super) fn rederive(events: &[LeaseMarker]) -> Result<LeaseArtifactStatus, ()> {
    if events.is_empty() {
        return Ok(LeaseArtifactStatus::Missing);
    }
    let initial = &events[0];
    if initial.phase != "fast-path-read-ok"
        || initial.source_node != initial.node
        || initial.sequence != 1
    {
        return Err(());
    }
    let node = initial.node.as_str();
    let term = initial.term;
    let fast = initial.identity();
    let mut buffered = None;
    let mut expired = false;
    let mut released = false;
    let mut handled = false;
    let mut terminal = None;
    let mut duplicate_terminal = false;
    for (offset, event) in events.iter().enumerate().skip(1) {
        if event.sequence != (offset + 1) as u64
            || event.source_node != node
            || event.node != node
            || event.term != term
        {
            return Err(());
        }
        match event.phase.as_str() {
            "lease-expired"
                if !expired && buffered.is_none() && !released && event.identity() == fast =>
            {
                expired = true;
            }
            "read-buffered" if expired && buffered.is_none() && !released => {
                if event.identity() == fast {
                    return Err(());
                }
                buffered = Some(event.identity());
            }
            "post-expiry-released"
                if expired && buffered == Some(event.identity()) && !released =>
            {
                released = true;
            }
            "post-expiry-handler" if released && buffered == Some(event.identity()) && !handled => {
                handled = true;
            }
            "post-expiry-unavailable"
                if handled && buffered == Some(event.identity()) && terminal.is_none() =>
            {
                terminal = Some(LeaseArtifactStatus::Complete);
            }
            "post-expiry-read-served-violation"
                if handled && buffered == Some(event.identity()) && terminal.is_none() =>
            {
                terminal = Some(LeaseArtifactStatus::Violation);
            }
            "post-expiry-renewed-violation"
                if expired
                    && event.identity() == buffered.unwrap_or(fast)
                    && terminal.is_none() =>
            {
                terminal = Some(LeaseArtifactStatus::Violation);
            }
            "post-expiry-unexpected-error"
                if handled
                    && buffered == Some(event.identity())
                    && event.code.is_some()
                    && terminal.is_none() =>
            {
                terminal = Some(LeaseArtifactStatus::HarnessError);
            }
            "post-expiry-duplicate-terminal"
                if released && buffered == Some(event.identity()) && !duplicate_terminal =>
            {
                duplicate_terminal = true;
            }
            "coverage-lost" if event.reason.is_some() && offset + 1 == events.len() => {
                terminal = Some(LeaseArtifactStatus::Incomplete);
            }
            _ => return Err(()),
        }
    }
    finalize(terminal, duplicate_terminal)
}

fn finalize(
    terminal: Option<LeaseArtifactStatus>,
    duplicate_terminal: bool,
) -> Result<LeaseArtifactStatus, ()> {
    Ok(match (terminal, duplicate_terminal) {
        (Some(LeaseArtifactStatus::Violation), true) => {
            LeaseArtifactStatus::ViolationWithHarnessError
        }
        (Some(LeaseArtifactStatus::Violation), false) => LeaseArtifactStatus::Violation,
        (Some(LeaseArtifactStatus::Complete), false) => LeaseArtifactStatus::Complete,
        (Some(LeaseArtifactStatus::Incomplete) | None, false) => LeaseArtifactStatus::Incomplete,
        (Some(LeaseArtifactStatus::HarnessError), _) | (_, true) => {
            LeaseArtifactStatus::HarnessError
        }
        (
            Some(LeaseArtifactStatus::Missing | LeaseArtifactStatus::ViolationWithHarnessError),
            _,
        ) => return Err(()),
    })
}
