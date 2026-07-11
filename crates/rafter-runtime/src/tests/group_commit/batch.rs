use super::*;

#[test]
fn a_batched_step_appends_the_whole_suffix_in_one_durable_flush() {
    let (segment, appends, truncates) = CountingLogSegment::new();
    let mut runtime = elected_leader_with_log_segment(segment);
    let appends_after_election = appends.get();

    let outputs = runtime
        .step_batch(proposals(4))
        .expect("batched proposals persist");

    assert_eq!(
        appends.get() - appends_after_election,
        1,
        "four batched proposals land in one suffix append"
    );
    assert_eq!(truncates.get(), 0);
    assert_eq!(runtime.last_log_index(), rafter::LogIndex(5));
    let sent_appends = outputs
        .iter()
        .filter(|output| {
            matches!(
                output,
                RaftOutput::Send {
                    message: Message::AppendEntries(_),
                    ..
                }
            )
        })
        .count();
    assert!(
        sent_appends > 0,
        "the batch's replication traffic is released after the flush"
    );
}

#[test]
fn unbatched_steps_pay_one_flush_per_proposal() {
    let (segment, appends, _truncates) = CountingLogSegment::new();
    let mut runtime = elected_leader_with_log_segment(segment);
    let appends_after_election = appends.get();

    for input in proposals(4) {
        runtime.step(input).expect("proposal persists");
    }

    assert_eq!(
        appends.get() - appends_after_election,
        4,
        "one suffix append per unbatched proposal"
    );
}

#[test]
fn an_empty_batch_is_a_durable_no_op() {
    let (segment, appends, truncates) = CountingLogSegment::new();
    let mut runtime = elected_leader_with_log_segment(segment);
    let appends_after_election = appends.get();

    let outputs = runtime.step_batch(Vec::new()).expect("empty batch is fine");

    assert!(outputs.is_empty());
    assert_eq!(appends.get() - appends_after_election, 0);
    assert_eq!(truncates.get(), 0);
}
