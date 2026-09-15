//! The fast durable-service reference is executed, not merely compiled.

#[allow(dead_code)]
#[path = "../examples/pipelined_durable_service.rs"]
mod pipelined_durable_service;

use rafter::NodeId;

#[test]
fn pipelined_durable_service_covers_fast_public_composition() {
    let root = std::env::temp_dir().join(format!(
        "rafter-pipelined-durable-service-test-{}",
        std::process::id()
    ));

    let report = pipelined_durable_service::run_service(&root, false);

    assert_eq!(report.initial_leader, NodeId(1));
    assert_eq!(report.final_values.get("alpha"), Some(&"1".to_owned()));
    assert_eq!(report.final_values.get("gamma"), Some(&"3".to_owned()));
    assert_eq!(report.final_values.get("delta"), Some(&"4".to_owned()));
    assert!(report.pipelined_operations > 0);
    assert_eq!(report.synchronous_fallbacks, 0);
    assert!(report.retirement_submissions > 0);
    assert_eq!(report.retirement_fallbacks, 0);
    assert!(report.max_peer_queue_depth <= report.peer_queue_capacity);
    assert!(report.snapshot_index >= report.restarted_applied_floor);
}
