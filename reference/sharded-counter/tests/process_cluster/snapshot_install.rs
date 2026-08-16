// Snapshot catch-up, and the evidence that the application installed one.
//
// Split out of `process_cluster.rs` along the seam that file already uses for
// `review_regressions` and `transport_tls`, because the assertions below grew
// past what a shared binary should carry inline.
//
// The subject is the one case a local snapshot round trip cannot reach: a
// replica that fell behind a compaction and is caught up by a snapshot the
// leader sent. The `writer_id` on the promoted envelope is what separates that
// from a snapshot the replica wrote for itself, which is the distinction the
// directory-is-non-empty check this replaced could not make.

#[test]
#[ignore = "snapshot replay must reconcile exact durable outstanding work"]
fn snapshot_replay_clears_exact_outstanding_before_the_next_admission() {
    let mut cluster = ProcessCluster::start("snapshot-replay-ledger-cleanup");
    let old_leader = 1;
    let group = cluster.leader_group_on(old_leader);
    let client = 58;
    assert_eq!(
        cluster.request_on(old_leader, &format!("OPEN {group} 1 {client} 1")),
        "OK SESSION opened"
    );
    let request = format!("ADD {group} 1 {client} 1 1 5");
    cluster.arm_failpoint(
        old_leader,
        "after_reservation_publication_before_managed_admission",
    );
    cluster.trigger_failpoint(
        old_leader,
        &request,
        "after_reservation_publication_before_managed_admission",
    );

    let leader = cluster.leader_for_group_excluding(group, Some(old_leader));
    cluster.wait_response(leader, &request, "OK ADDED value=5");
    let snapshot = cluster.request_on(leader, &format!("SNAPSHOT {group} 1"));
    assert!(snapshot.starts_with("OK SNAPSHOT applied="), "{snapshot}");
    // The boundary the leader compacted through, kept so the assertions below
    // can name the transfer rather than accept any snapshot at all.
    let compacted_through = number_field(&snapshot, "applied");

    cluster.restart(old_leader);
    cluster.wait_ready();
    cluster.wait_value(old_leader, group, 1, 5);
    // Re-read once the wait has settled, because the assertions below are about
    // the applied floor this replica reached rather than about the value alone.
    let value = cluster.request_on(old_leader, &format!("VALUE {group} 1"));

    assert_promoted_snapshot_landed(
        &cluster.scratch_path().join(format!(
            "host-{old_leader}/groups/{group}/raft/snapshots"
        )),
        NodeId(leader),
        compacted_through,
        &value,
    );

    let before = cluster.request_on(old_leader, "STATUS");
    assert_eq!(
        number_field(&before, "durable_outstanding"),
        1,
        "snapshot installation deliberately leaves the consumer ledger for authoritative reconciliation: {before}"
    );

    cluster.wait_status_at_least(old_leader, "recovery_deferred", 1);
    let deferred = cluster.request_on(old_leader, "STATUS");
    assert_eq!(
        number_field(&deferred, "pending_proposals"),
        0,
        "leadership transfer starts from a deferred, not locally queued, recovery: {deferred}"
    );
    assert_eq!(
        cluster.request_on(old_leader, "PAUSE_RECOVERY"),
        "OK RECOVERY paused"
    );
    assert_eq!(
        cluster.request_on(leader, &format!("TRANSFER {group} 1 {old_leader}"),),
        format!("OK TRANSFER target={old_leader}")
    );
    cluster.wait_for_group_leader(old_leader, group);
    cluster.wait_response(
        old_leader,
        &format!("ADD {group} 1 59 1 1 1"),
        "ERR SESSION_NOT_OPEN",
    );
    let _blockers = occupy_all_workers_for(&mut cluster, old_leader, group, 15_000);
    fill_group_queue(&mut cluster, old_leader, group);
    let refused = number_field(
        &cluster.request_on(old_leader, "STATUS"),
        "recovery_refused",
    );
    assert_eq!(
        cluster.request_on(old_leader, "RESUME_RECOVERY"),
        "OK RECOVERY resumed"
    );
    cluster.wait_status_above(old_leader, "recovery_refused", refused);
    assert_eq!(
        cluster.request_on(old_leader, "PAUSE_RECOVERY"),
        "OK RECOVERY paused"
    );
    let before_replay = cluster.request_on(old_leader, "STATUS");
    assert_eq!(
        number_field(&before_replay, "pending_proposals"),
        0,
        "queue-blocked recovery must leave no local proposal: {before_replay}"
    );
    let replay = cluster.request_on(old_leader, &request);
    assert_eq!(
        replay, "OK REPLAY ADDED value=5",
        "exact retry did not reach authoritative replay; status={before_replay}"
    );
    let reconciled = cluster.request_on(old_leader, "STATUS");
    assert_eq!(
        number_field(&reconciled, "durable_outstanding"),
        0,
        "the replay response must follow durable ledger cleanup: {reconciled}"
    );
    let next = cluster.request_on(old_leader, &format!("ADD {group} 1 {client} 1 2 1"));
    assert!(
        next.starts_with("ERR BACKPRESSURE ") || next == "OK ADDED value=6",
        "the next sequence must reach the real queue decision, not stale conflict: {next}"
    );
}

// Asserts that the snapshot now current in `snapshot_dir` is the one `sender`
// transferred, and that the application installed it.
//
// A non-empty snapshots directory was the whole of this evidence once, and it
// proves nothing: a replica writes its own snapshots into the same directory,
// so the check passed whether or not a transfer ever arrived — and passed
// identically if the application had refused the install and poisoned the
// group. Each assertion below closes one of those holes.
fn assert_promoted_snapshot_landed(
    snapshot_dir: &std::path::Path,
    sender: NodeId,
    compacted_through: u64,
    value_line: &str,
) {
    let store = FileRaftSnapshotStore::open(snapshot_dir)
        .unwrap_or_else(|error| panic!("could not open {}: {error:?}", snapshot_dir.display()));
    let current = store
        .current_snapshot()
        .unwrap_or_else(|| panic!("{} holds no current snapshot", snapshot_dir.display()));

    // Promoted-inbound rather than locally written. Promotion keeps the sending
    // replica's descriptor verbatim, so `writer_id` is the node that authored
    // the snapshot; one this replica built for itself would carry its own id,
    // which is exactly the case the old assertion could not tell apart.
    assert_eq!(
        current.metadata.writer_id, sender,
        "the current snapshot must be the one the sender transferred, not one this replica wrote"
    );
    assert_eq!(current.metadata.last_included_index.0, compacted_through);

    // The content landed, and it landed intact. Reading it back through the
    // store's own chunk source is the same path the application's install takes,
    // so a payload the application could not have read fails here too.
    assert!(
        current.application_payload_len > 0,
        "an empty promoted payload would install an empty counter over live state"
    );
    let payload = store
        .snapshot_chunk(SnapshotChunkRequest {
            transfer_id: current.transfer_id(),
            metadata: &current.metadata,
            total_payload_len: current.application_payload_len,
            application_payload_crc32: current.application_payload_crc32,
            offset: 0,
            len: u32::try_from(current.application_payload_len).expect("a bounded test payload"),
        })
        .expect("the promoted payload reads back through the store's own chunk source");
    assert_eq!(
        crc32(&payload),
        current.application_payload_crc32,
        "the promoted bytes on disk are the bytes the descriptor names"
    );

    // And the application observed the install. The group verifies the state
    // machine's applied index reaches the snapshot's boundary immediately after
    // `install_snapshot` returns and poisons itself otherwise, so a replica
    // still serving reads at or beyond that index applied this snapshot rather
    // than refusing it.
    assert!(
        number_field(value_line, "applied") >= compacted_through,
        "the application must be applied through the installed snapshot's boundary \
         {compacted_through}: {value_line}"
    );
}
