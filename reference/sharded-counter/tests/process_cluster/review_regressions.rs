#[test]
#[ignore = "shared admission candidates under deterministic queue saturation"]
fn rejected_shared_barrier_candidates_do_not_claim_the_client_slot() {
    let mut cluster = ProcessCluster::start("shared-barrier-no-speculative-claim");
    let host = cluster.leader();
    let group = cluster.leader_group_on(host);
    let _blockers = occupy_all_workers(&mut cluster, host, group);
    fill_group_queue(&mut cluster, host, group);
    assert_eq!(cluster.request_on(host, "PAUSE_PEERS"), "OK PEERS paused");

    let requests = [1, 2]
        .map(|epoch| format!("OPEN {group} 1 56 {epoch}"))
        .map(|request| {
            let address = cluster.node_addr(host);
            thread::spawn(move || {
                let mut connection = LineConnection::connect(address, CONNECTION_TIMEOUTS)
                    .expect("leader connection opens");
                connection.request(&request)
            })
        });
    cluster.wait_status_at_least(host, "admission_candidates", 2);
    assert_eq!(cluster.request_on(host, "RESUME_PEERS"), "OK PEERS resumed");

    for request in requests {
        let response = request
            .join()
            .expect("session request thread does not panic")
            .expect("saturated admission returns a response");
        assert!(
            response.starts_with("ERR BACKPRESSURE GroupQueueFull"),
            "a rejected peer candidate cannot invent ownership: {response}"
        );
    }
    let status = cluster.request_on(host, "STATUS");
    assert_eq!(number_field(&status, "durable_outstanding"), 0, "{status}");
    assert_eq!(number_field(&status, "pending_proposals"), 0, "{status}");
}

#[test]
#[ignore = "committed application publication failures must replay without quarantine"]
fn application_commit_publication_failures_recover_without_stale_poison() {
    const FAILPOINTS: [&str; 7] = [
        "application_commit_temp_create_failure",
        "application_commit_temp_write_failure",
        "application_commit_temp_sync_failure",
        "before_application_commit_state_rcap_rename",
        "after_application_commit_state_rcap_rename",
        "before_application_commit_state_rcap_parent_sync",
        "application_commit_state_rcap_parent_sync_failure",
    ];

    let mut cluster = ProcessCluster::start("application-commit-publication");
    let group = cluster.leader_group_on(1);
    let client = 57;
    assert!(cluster
        .request_on(1, &format!("OPEN {group} 1 {client} 1"))
        .starts_with("OK SESSION "));

    for (offset, failpoint) in FAILPOINTS.into_iter().enumerate() {
        let sequence = u32::try_from(offset + 1).expect("seven sequences fit u32");
        let request = format!("ADD {group} 1 {client} 1 {sequence} 1");
        let host = cluster.leader_for_group(group);
        cluster.arm_failpoint(host, failpoint);
        cluster.trigger_failpoint(host, &request, failpoint);

        cluster.restart(host);
        cluster.wait_ready();
        let expected = i64::from(sequence);
        let response = cluster.request_leader(&request);
        assert!(
            response == format!("OK ADDED value={expected}")
                || response == format!("OK REPLAY ADDED value={expected}"),
            "retry after {failpoint} did not resolve the committed add: {response}"
        );
        cluster.wait_all_values(&std::collections::BTreeMap::from([((group, 1), expected)]));
        let status = cluster.request_on(host, "STATUS");
        assert_eq!(
            number_field(&status, "poisoned"),
            0,
            "{failpoint} recovered stale quarantine state: {status}"
        );
    }
}

#[test]
#[ignore = "a failed authoritative barrier cannot prove an absent local retry uncommitted"]
fn committed_retry_barrier_failure_remains_unknown_after_local_cleanup() {
    let mut cluster = ProcessCluster::start("committed-retry-barrier-unknown");
    let host = cluster.leader();
    let group = cluster.leader_group_on(host);
    let client = 60;
    assert_eq!(
        cluster.request_on(host, &format!("OPEN {group} 1 {client} 1")),
        "OK SESSION opened"
    );
    let request = format!("ADD {group} 1 {client} 1 1 5");
    assert_eq!(cluster.request_on(host, &request), "OK ADDED value=5");
    let status = cluster.request_on(host, "STATUS");
    assert_eq!(
        number_field(&status, "durable_outstanding"),
        0,
        "the successful application commit must establish the locally absent case: {status}"
    );

    assert_eq!(cluster.request_on(host, "PAUSE_PEERS"), "OK PEERS paused");
    let response = cluster.request_on(host, &request);
    assert!(
        response.starts_with("ERR UNKNOWN "),
        "an unresolved exact retry must remain unknown: {response}"
    );
    assert!(
        !response.contains("NOT_COMMITTED"),
        "barrier failure is not non-acceptance proof: {response}"
    );
    assert_eq!(
        cluster.request_on(host, "RESUME_PEERS"),
        "OK PEERS resumed"
    );
    let leader = cluster.leader_for_group(group);
    cluster.wait_response(leader, &request, "OK REPLAY ADDED value=5");
}

#[test]
#[ignore = "authoritative rejection must retire an exact durable obligation"]
fn authoritative_rejection_reconciles_exact_outstanding_before_reply() {
    let mut cluster = ProcessCluster::start("authoritative-rejection-cleanup");
    let old_leader = 1;
    let group = cluster.leader_group_on(old_leader);
    let client = 63;
    assert_eq!(
        cluster.request_on(old_leader, &format!("OPEN {group} 1 {client} 1")),
        "OK SESSION opened"
    );
    let stale_request = format!("ADD {group} 1 {client} 1 1 5");
    cluster.arm_failpoint(
        old_leader,
        "after_reservation_publication_before_managed_admission",
    );
    cluster.trigger_failpoint(
        old_leader,
        &stale_request,
        "after_reservation_publication_before_managed_admission",
    );

    let leader = cluster.leader_for_group_excluding(group, Some(old_leader));
    assert_eq!(
        cluster.request_on(leader, &format!("OPEN {group} 1 {client} 2")),
        "OK SESSION replaced"
    );
    cluster.restart(old_leader);
    assert_eq!(
        cluster.request_on(old_leader, "PAUSE_RECOVERY"),
        "OK RECOVERY paused"
    );
    cluster.wait_ready();
    cluster.wait_status_at_least(old_leader, "recovery_deferred", 1);
    let before = cluster.request_on(old_leader, "STATUS");
    assert_eq!(
        number_field(&before, "durable_outstanding"),
        1,
        "the stale operation must remain a real durable obligation before its authoritative decision: {before}"
    );
    assert_eq!(
        number_field(&before, "pending_proposals"),
        0,
        "the test must reach the authoritative admission path without attaching to recovery: {before}"
    );

    let leader = cluster.leader_for_group_excluding(group, Some(old_leader));
    assert_eq!(
        cluster.request_on(leader, &format!("TRANSFER {group} 1 {old_leader}")),
        format!("OK TRANSFER target={old_leader}")
    );
    cluster.wait_for_group_leader(old_leader, group);
    cluster.wait_response(
        old_leader,
        &format!("ADD {group} 1 59 1 1 1"),
        "ERR SESSION_NOT_OPEN",
    );
    assert_eq!(
        cluster.request_on(old_leader, &stale_request),
        "ERR STALE_SESSION current=2"
    );
    let after = cluster.request_on(old_leader, "STATUS");
    assert_eq!(
        number_field(&after, "durable_outstanding"),
        0,
        "the terminal rejection must follow exact durable cleanup: {after}"
    );
}
