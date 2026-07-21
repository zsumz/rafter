//! Lease-isolation marker collection and transcript qualification facade.

mod history;
mod marker;
mod transcript;

use std::{error::Error, path::Path, time::Instant};

use crate::execution::filesystem::{HeldDirectory, OperationDeadline, TREE_LIMITS};

use super::model::ScenarioMarkers;

use marker::{bump_lease_count, LeaseMarker};
use transcript::{bind_lease_history, finish_lease_transcript};

#[cfg(test)]
pub(in crate::producer) use history::{probe_completion_count, MAX_LINE_BYTES};
#[cfg(test)]
pub(in crate::producer) use marker::LeaseMarker as TestLeaseMarker;
#[cfg(test)]
pub(in crate::producer) use transcript::{
    bind_lease_history as bind_history_for_test,
    finish_lease_transcript as finish_transcript_for_test,
    validate_lease_transcript as validate_transcript_for_test,
};

pub(super) fn read_markers(
    store: &HeldDirectory,
    deadline: Instant,
) -> Result<ScenarioMarkers, Box<dyn Error>> {
    let mut markers = ScenarioMarkers::default();
    let mut lease_events = Vec::new();
    let mut lease_parse_errors = 0;
    let node_logs = store.open_dir(Path::new("node-logs"))?;
    for file in node_logs.files_below(
        TREE_LIMITS,
        OperationDeadline::at(deadline, "Maelstrom marker traversal"),
    )? {
        let source_node = file
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or("Maelstrom node log has no UTF-8 file stem")?;
        scan_markers(
            &node_logs.read_to_string_with_deadline(
                &file,
                OperationDeadline::at(deadline, "Maelstrom marker log read"),
            )?,
            source_node,
            &mut markers,
            &mut lease_events,
            &mut lease_parse_errors,
        );
    }
    finish_lease_transcript(&mut markers, &lease_events, lease_parse_errors);
    let history = store
        .read_to_string_with_deadline(
            Path::new("history.edn"),
            OperationDeadline::at(deadline, "Maelstrom history read"),
        )
        .ok();
    bind_lease_history(&mut markers, &lease_events, history.as_deref());
    Ok(markers)
}

fn scan_markers(
    source: &str,
    source_node: &str,
    markers: &mut ScenarioMarkers,
    lease_events: &mut Vec<LeaseMarker>,
    lease_parse_errors: &mut u64,
) {
    let mut saw_restart = false;
    let mut saw_crash = false;
    for line in source.lines() {
        markers.membership_enter += u64::from(line.contains("action=enter-joint"));
        markers.membership_leave += u64::from(line.contains("action=leave-joint"));
        markers.membership_complete += u64::from(line.contains("complete target="));
        if line.contains("proxy restarting child") {
            markers.restarts += 1;
            saw_restart = true;
        }
        if line.contains("crashpoint=RAFTER_MAELSTROM_CRASH_AFTER_APP_PERSIST_ONCE fired") {
            markers.crashpoints += 1;
            saw_crash = true;
        }
        let progress = line.contains(" role=leader ") || line.contains("compacted snapshot");
        markers.post_restart_progress += u64::from(saw_restart && progress);
        markers.post_crash_progress += u64::from(saw_crash && progress);
        markers.snapshots_compacted += u64::from(line.contains("compacted snapshot"));
        markers.snapshots_applied += u64::from(line.contains("applied snapshot"));
        markers.post_restart_snapshots_applied +=
            u64::from(saw_restart && line.contains("applied snapshot"));
        if line.starts_with("rafter-maelstrom lease-isolation ") {
            match LeaseMarker::parse(line, source_node) {
                Ok(event) => {
                    bump_lease_count(markers, &event.phase);
                    lease_events.push(event);
                }
                Err(()) => *lease_parse_errors += 1,
            }
        }
    }
}
