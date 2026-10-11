//! The durable adapter applies a sparse command without gap-sized allocation.

use super::*;
use rafter_runtime::application::ApplicationEvent;

#[test]
fn a_command_after_many_protocol_entries_recovers_its_bytes_and_floor() {
    let root =
        std::env::temp_dir().join(format!("rafter-application-range-{}", std::process::id()));
    assert!(
        !root.exists(),
        "the scenario requires a fresh durable directory"
    );
    let (state, mut worker) = open(&root, NodeId(1));
    let index = LogIndex(1_000_000);
    worker
        .try_submit(vec![AppliedCommand {
            first_index: LogIndex(1),
            index,
            payload: Some(super::super::codec::encode_set("range", "covered")),
            proposal_id: Some(LocalProposalId(1)),
        }])
        .unwrap();
    let ApplicationEvent::Applied(completion) = worker.complete().unwrap() else {
        panic!("the application transaction must complete");
    };
    assert_eq!(completion.entries().len(), 1);
    assert_eq!(
        completion.outcomes(),
        &[AppliedOutcome {
            proposal_id: Some(LocalProposalId(1))
        }]
    );
    assert_eq!(snapshot(&state).applied, index);
    worker.shutdown().unwrap();
    let (recovered, mut reopened) = open(&root, NodeId(1));
    assert_eq!(snapshot(&recovered).applied, index);
    assert_eq!(
        snapshot(&recovered).kv.get("range"),
        Some(&String::from("covered"))
    );
    assert_eq!(reopened.durable_through(), index);
    reopened.shutdown().unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
