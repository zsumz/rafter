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
