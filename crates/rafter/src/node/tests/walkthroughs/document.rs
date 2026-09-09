//! Observed state changes and one primary story followed by actual batch differences.

use std::fmt::Write;

use super::{formatting, support::*};

#[derive(Eq, PartialEq)]
pub(super) struct View {
    pub before: String,
    pub fields: Vec<(&'static str, String)>,
}

impl View {
    pub fn capture(node: &Node) -> Self {
        // The compatibility accessor also exists on the pinned baseline core.
        let dispatch = node.applied_index().0;
        let progress = node
            .leader_replication_progress()
            .iter()
            .map(|peer| format!("node {}: {}", peer.follower_id.0, peer.match_index.0))
            .collect::<Vec<_>>()
            .join("; ");
        Self {
            before: format!("Node {} is a {} in term {}, voted for {}. Commit {}, dispatch {}, last index {} / term {}; {} pending reads.",
                node.id().0, formatting::role(node.role()), node.current_term().0, formatting::vote(node.voted_for()),
                node.commit_index().0, dispatch, node.last_log_index().0, node.last_log_term().0, node.pending_read_count()),
            fields: vec![
                ("Term", node.current_term().0.to_string()),
                ("Role", formatting::role(node.role()).into()),
                ("Voted for", formatting::vote(node.voted_for())),
                ("Commit index", node.commit_index().0.to_string()),
                ("Dispatch index", dispatch.to_string()),
                ("Last log index", node.last_log_index().0.to_string()),
                ("Last log term", node.last_log_term().0.to_string()),
                ("Pending reads", node.pending_read_count().to_string()),
                ("Acknowledged follower prefixes", if progress.is_empty() { "none".into() } else { progress }),
            ],
        }
    }
}

pub(super) struct ObservedStep {
    pub name: &'static str,
    pub why: &'static str,
    pub before: View,
    pub after: View,
    pub inputs: Vec<Input>,
    pub outputs: Vec<Output>,
}

pub(super) fn start() -> String {
    String::from(
        "# Production-core walkthroughs\n\n\
These stories run the actual `Node::step` and `Node::step_batch` with default\n\
pre-vote and check-quorum enabled and lease reads disabled. Each starts from the\n\
state shown below. Read the single-input story once, then its observed batch differences.\n\n\
**Raw-core boundary:** returning an output neither persists Raft state nor executes\n\
application code. The embedding persists dependent state before releasing sends,\n\
read grants, or application effects in their returned order. Dispatch includes\n\
no-ops, configurations, and the recovery floor; it does not establish execution\n\
or durable application progress.\n\n\
This is a focused node replay. Ordinary append successes echo the term, sequence,\n\
and confirmed end of an actual emitted request. A separate real-follower test\n\
checks the write exchange. Synthetic guard probes and scripted peer outcomes are\n\
labeled. The human view selects state changes and combines only consecutive\n\
identical sends, retaining recipient order. The [full regression artifact](\n\
src/node/tests/walkthroughs/observations.txt) separately records every resolved\n\
input, ordered output, and internal state field after every input or batch.\n\n",
    )
}

fn list(document: &mut String, title: &str, lines: &[String]) {
    writeln!(document, "**{title}**\n").unwrap();
    if lines.is_empty() {
        writeln!(document, "None.\n").unwrap();
    }
    for (index, line) in lines.iter().enumerate() {
        writeln!(document, "{}. {line}", index + 1).unwrap();
    }
    if !lines.is_empty() {
        writeln!(document).unwrap();
    }
}

fn changes(before: &View, after: &View) -> Vec<String> {
    before
        .fields
        .iter()
        .zip(&after.fields)
        .filter_map(|((name, before), (_, after))| {
            (before != after).then(|| format!("{name}: {before} → {after}."))
        })
        .collect()
}

pub(super) fn scenario(
    document: &mut String,
    scenario: &Scenario,
    single: &[ObservedStep],
    batch: &[ObservedStep],
) {
    writeln!(
        document,
        "## {}\n\n{}\n\nMembership: {}.\n",
        scenario.name,
        scenario.explanation,
        formatting::membership(&scenario.node.effective_membership())
    )
    .unwrap();
    for step in single {
        writeln!(
            document,
            "### {}\n\n**Before:** {}\n",
            step.name, step.before.before
        )
        .unwrap();
        list(document, "Input", &formatting::inputs(&step.inputs));
        let changed = changes(&step.before, &step.after);
        if changed.is_empty() {
            writeln!(document, "**Changes:** the displayed state is unchanged.\n").unwrap();
        } else {
            list(document, "Changes", &changed);
        }
        list(
            document,
            "Ordered outputs",
            &formatting::outputs(&step.outputs),
        );
        writeln!(document, "**Why:** {}\n", step.why).unwrap();
    }
    writeln!(document, "### Batched inputs: what changes\n\nEach checkpoint becomes one `step_batch` call; the primary story calls `step`\nonce per input. The full artifact keeps both sets of boundaries.\n").unwrap();
    let mut differences = 0;
    for (single, batch) in single.iter().zip(batch) {
        if single.inputs == batch.inputs
            && single.outputs == batch.outputs
            && single.after == batch.after
        {
            continue;
        }
        differences += 1;
        writeln!(document, "**{}**\n", single.name).unwrap();
        if single.inputs != batch.inputs {
            list(
                document,
                "Single-input exchange",
                &formatting::inputs(&single.inputs),
            );
            list(
                document,
                "Batched exchange",
                &formatting::inputs(&batch.inputs),
            );
        }
        if single.after != batch.after {
            let changed: Vec<_> = single.after.fields.iter().zip(&batch.after.fields)
                .filter_map(|((name, single), (_, batch))| (single != batch).then(|| format!("{name} at this checkpoint: {single} after single calls; {batch} after batching."))).collect();
            list(document, "Different end state", &changed);
        }
        if single.outputs != batch.outputs {
            list(
                document,
                "Single-call ordered outputs",
                &formatting::outputs(&single.outputs),
            );
            list(
                document,
                "Batch ordered outputs",
                &formatting::outputs(&batch.outputs),
            );
        }
    }
    if differences == 0 {
        writeln!(
            document,
            "The displayed checkpoint states and ordered outputs match in both modes.\n"
        )
        .unwrap();
    }
}
