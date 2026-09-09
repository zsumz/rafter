//! Stable observation keys, complete state capture, and a compact reading transcript.

use std::fmt::Write;

use super::support::*;
use crate::node::state::{LeaderState, PersistentState, VolatileState};

fn state(node: &Node, text: &mut String) {
    // Exhaustive destructuring makes an added state field a conscious capture
    // decision. Schema keys name facts rather than Rust ownership groups. No
    // sorting, hashing, cursor substitution, or output normalization is used.
    let Node {
        config,
        persistent,
        volatile,
        election,
        leader,
        derived,
    } = node;
    let PersistentState {
        current_term,
        voted_for,
        committed_configuration,
        snapshot,
        log,
    } = persistent;
    let VolatileState {
        role,
        commit_index,
        dispatched_index: dispatch,
        local_proposals,
        incoming_snapshot,
        leader_hint,
        snapshot_chunk_rejections,
    } = volatile;
    let LeaderState {
        progress,
        ticks,
        lease,
        pending_transfer,
        deposition_authorized,
        heartbeat_sequence,
        heartbeat_elapsed,
        quorum_acks,
        quorum_check_elapsed,
        pending_reads,
    } = leader;
    for (key, value) in [
        ("configuration", format!("{config:?}")),
        ("term", format!("{current_term:?}")),
        ("vote", format!("{voted_for:?}")),
        (
            "committed-configuration",
            format!("{committed_configuration:?}"),
        ),
        ("snapshot", format!("{snapshot:?}")),
        ("log", format!("{log:?}")),
        ("role", format!("{role:?}")),
        ("commit", format!("{commit_index:?}")),
        ("dispatch", format!("{dispatch:?}")),
        ("local-proposals", format!("{local_proposals:?}")),
        ("incoming-snapshot", format!("{incoming_snapshot:?}")),
        ("leader-hint", format!("{leader_hint:?}")),
        (
            "snapshot-rejections",
            format!("{snapshot_chunk_rejections:?}"),
        ),
        ("election", format!("{election:?}")),
        ("replication-progress", format!("{progress:?}")),
        ("leader-ticks", format!("{ticks:?}")),
        ("lease", format!("{lease:?}")),
        ("transfer", format!("{pending_transfer:?}")),
        (
            "deposition-authorized",
            format!("{deposition_authorized:?}"),
        ),
        ("heartbeat-sequence", format!("{heartbeat_sequence:?}")),
        ("heartbeat-elapsed", format!("{heartbeat_elapsed:?}")),
        ("quorum-acks", format!("{quorum_acks:?}")),
        ("quorum-elapsed", format!("{quorum_check_elapsed:?}")),
        ("pending-reads", format!("{pending_reads:?}")),
        ("derived-indexes", format!("{derived:?}")),
    ] {
        writeln!(text, "{key}={value}").unwrap();
    }
}

pub(super) fn replay(scenarios: Vec<Scenario>) -> (String, String) {
    let mut observations = String::from("rafter-protocol-observations-v1\nbase=2a58d6e3\n");
    let mut document = super::document::start();
    for scenario in scenarios {
        let single = run(&scenario, false, &mut observations);
        let batch = run(&scenario, true, &mut observations);
        super::document::scenario(&mut document, &scenario, &single, &batch);
    }
    document.truncate(document.trim_end().len());
    document.push('\n');
    (observations, document)
}

pub(super) fn run(
    scenario: &Scenario,
    batch: bool,
    observations: &mut String,
) -> Vec<super::document::ObservedStep> {
    use super::document::{ObservedStep, View};

    let mut node = scenario.node.clone();
    let mut sent = Vec::new();
    let mut story = Vec::new();
    writeln!(observations, "scenario={}; batch={batch}", scenario.name).unwrap();
    state(&node, observations);
    for step in &scenario.steps {
        writeln!(observations, "checkpoint={}", step.name).unwrap();
        let mut recorded = ObservedStep {
            name: step.name,
            why: step.why,
            before: View::capture(&node),
            after: View::capture(&node),
            inputs: Vec::new(),
            outputs: Vec::new(),
        };
        let frame_size = if batch { step.inputs.len() } else { 1 };
        for frame in step.inputs.chunks(frame_size) {
            let inputs: Vec<_> = frame.iter().map(|input| input.resolve(&sent)).collect();
            writeln!(observations, "inputs={inputs:?}").unwrap();
            recorded.inputs.extend(inputs.iter().cloned());
            let outputs = if batch {
                node.step_batch(inputs)
            } else {
                node.step(inputs.into_iter().next().unwrap())
            };
            writeln!(observations, "outputs={outputs:?}").unwrap();
            assert_eq!(node.applied_index(), node.dispatched_index());
            state(&node, observations);
            sent.extend(outputs.iter().cloned());
            recorded.outputs.extend(outputs);
        }
        recorded.after = View::capture(&node);
        story.push(recorded);
    }
    assert_eq!(
        (
            node.current_term(),
            node.role(),
            node.commit_index(),
            node.last_log_index(),
            node.pending_read_count()
        ),
        scenario.expected,
        "{} batch={batch}",
        scenario.name
    );
    story
}
