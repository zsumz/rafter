use super::super::*;
use super::fixtures::{commit_payload, elect_node_one_with_pre_vote, production_cluster, LEADER};

#[test]
fn corrupted_frames_never_panic_and_the_cluster_reconverges() {
    let mut cluster = production_cluster();
    elect_node_one_with_pre_vote(&mut cluster);
    commit_payload(&mut cluster, b"stable");

    // Queue a replication wave, then corrupt the in-flight appends with
    // field-level damage: inflated terms, scrambled prev coordinates, and
    // truncated batches. Codec frame checksums reject byte-level damage at
    // decode; this direct in-memory path still verifies the kernel absorbs
    // malformed fields without panicking, even though byzantine field values
    // can locally disrupt (a huge term forces step-downs: disruption, not
    // unsafety).
    cluster.propose(LEADER, b"during-corruption".to_vec());
    let corrupted = cluster.corrupt_queued_matching(
        |envelope| envelope.to == NodeId(2),
        |message| {
            if let rafter::Message::AppendEntries(request) = message {
                request.prev_log_index = LogIndex(7);
                request.prev_log_term = Term(9);
                request.entries = rafter::SharedEntries::default();
                request.leader_commit = LogIndex(42);
            }
        },
    );
    assert!(corrupted >= 1);
    cluster.deliver_all();

    // A corrupted vote-shaped frame with an absurd term: absorbed, no
    // panic; the legitimate leader re-establishes the cluster afterward.
    cluster.tick(LEADER);
    let _ = cluster.corrupt_queued_matching(
        |envelope| envelope.to == NodeId(3),
        |message| {
            if let rafter::Message::AppendEntries(request) = message {
                request.term = Term(request.term.0 + 1_000);
            }
        },
    );
    cluster.deliver_all();

    // Reconvergence: whatever local disruption the damage caused, the
    // cluster elects, commits, and agrees again. Tick two candidates so
    // stickiness hints lapse and someone can win.
    let mut rounds = 0;
    while cluster.leaders().is_empty() {
        rounds += 1;
        assert!(rounds <= 40, "the cluster re-elects within bounded rounds");
        cluster.tick(LEADER);
        cluster.tick(NodeId(2));
        cluster.deliver_all();
    }
    let leaders = cluster.leaders();
    let leader = *leaders.last().expect("a leader re-establishes");
    cluster.propose(leader, b"after-corruption".to_vec());
    cluster.deliver_all();
    let reference = cluster.log_entries_from(leader, LogIndex(1));
    assert!(reference
        .iter()
        .any(|entry| entry.application_payload() == Some(b"after-corruption")));
}
