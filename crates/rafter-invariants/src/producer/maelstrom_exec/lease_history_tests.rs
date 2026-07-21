//! Lease-history binding and resource-bound scenarios.

use super::*;

const COMPLETION: &str = "{:index 2 :type :fail :process 0 :f :read :value nil :error [:temporarily-unavailable \"x [rafter-lease-probe client=c1 msg_id=11 code=11]\"]}";

#[test]
fn lease_probe_completion_is_bound_to_exact_client_and_message() {
    let history = concat!(
        "{:index 1 :type :invoke :process 0 :f :read :value [0 nil]}\n",
        "{:index 2 :type :fail :process 0 :f :read :value [0 nil] ",
        ":error [:temporarily-unavailable \"LeadershipLost [rafter-lease-probe client=c1 msg_id=11 code=11]\"]}\n",
    );
    assert_eq!(probe_completion_count(history, "c1", 11), Ok(1));
    assert_eq!(probe_completion_count(history, "c2", 11), Ok(0));
    assert_eq!(probe_completion_count(history, "c1", 12), Ok(0));
    assert_eq!(probe_completion_count("", "c1", 11), Ok(0));
}

#[test]
fn lease_probe_history_rejects_incomplete_or_mismatched_operations() {
    assert!(probe_completion_count(COMPLETION, "c1", 11).is_err());
    assert!(probe_completion_count(
        "{:index 1 :type :invoke :process 0 :f :read :value nil}",
        "c1",
        11
    )
    .is_err());
    let swapped = format!(
        "{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{{:index 2 :type :invoke :process 1 :f :write :value 1}}\n{}",
        COMPLETION
            .replace(":index 2", ":index 3")
            .replace(":process 0", ":process 1")
    );
    assert!(probe_completion_count(&swapped, "c1", 11).is_err());
    let intervening = format!(
        "{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{{:index 2 :type :fail :process 0 :f :read :value nil :error :net-timeout}}\n{}",
        COMPLETION.replace(":index 2", ":index 3")
    );
    assert!(probe_completion_count(&intervening, "c1", 11).is_err());
}

#[test]
fn lease_probe_history_rejects_identity_drift_and_resource_overflow() {
    let exact_pair = format!(
        "{{:index 1 :type :invoke :process 0 :f :read :value [0 nil]}}\n{}",
        COMPLETION.replace(":value nil", ":value [1 nil]")
    );
    assert!(probe_completion_count(&exact_pair, "c1", 11).is_err());
    let missing_value = format!(
        "{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{}",
        COMPLETION.replace(" :value nil", "")
    );
    assert!(probe_completion_count(&missing_value, "c1", 11).is_err());
    let oversized = "x".repeat(MAX_LINE_BYTES + 1);
    assert!(probe_completion_count(&oversized, "c1", 11)
        .expect_err("oversized line must fail before EDN parsing")
        .contains("exceeds"));
}
