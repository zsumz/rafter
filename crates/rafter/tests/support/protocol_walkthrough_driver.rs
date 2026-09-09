//! Test adapter for recording production-core observations and rendering documentation.
//! Mounted as a unit-test module for private state access; I/O stays outside src/.

#[path = "../../src/node/tests/walkthroughs/append_membership.rs"]
mod append_membership;
#[path = "../../src/node/tests/walkthroughs/document.rs"]
mod document;
#[path = "../../src/node/tests/walkthroughs/election_write.rs"]
mod election_write;
#[path = "../../src/node/tests/walkthroughs/formatting.rs"]
mod formatting;
#[path = "../../src/node/tests/walkthroughs/observation.rs"]
mod observation;
#[path = "../../src/node/tests/walkthroughs/read_barrier.rs"]
mod read_barrier;
#[path = "../../src/node/tests/walkthroughs/replies_test.rs"]
mod replies_test;
#[path = "../../src/node/tests/walkthroughs/steps.rs"]
mod steps;
#[path = "../../src/node/tests/walkthroughs/support.rs"]
mod support;

use support::Scenario;

fn scenarios() -> Vec<Scenario> {
    vec![
        election_write::election_and_write(),
        election_write::higher_term_rejection(),
        read_barrier::scenario(),
        append_membership::append_conflict(),
        append_membership::joint_quorum(),
    ]
}

#[test]
fn production_transitions_match_pre_refactor_observations() {
    let (observations, _) = observation::replay(scenarios());
    let expected = include_str!("../../src/node/tests/walkthroughs/observations.txt");
    // Report the first changed field without dumping a whole transcript.
    assert_eq!(observations.lines().count(), expected.lines().count());
    for (line, (actual, expected)) in observations.lines().zip(expected.lines()).enumerate() {
        assert_eq!(actual, expected, "transition observation line {}", line + 1);
    }
}

#[test]
fn walkthrough_document_matches_production_observations() {
    let (_, document) = observation::replay(scenarios());
    assert_eq!(
        document,
        include_str!("../../PROTOCOL_WALKTHROUGHS.md"),
        "run the explicitly ignored render_protocol_walkthroughs test after reviewing changes"
    );
}

#[test]
#[ignore = "explicitly render walkthrough documentation; never rewrites the behavioral baseline"]
fn render_protocol_walkthroughs() {
    use std::io::Write;

    let (_, document) = observation::replay(scenarios());
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(b"RAFTER_WALKTHROUGH_BEGIN\n").unwrap();
    stdout.write_all(document.as_bytes()).unwrap();
    stdout.write_all(b"RAFTER_WALKTHROUGH_END\n").unwrap();
}
