#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

#[test]
fn in_memory_driver_rejects_duplicate_primary_node_ids() {
    let error = KvDriver::new(NodeId(1), vec![group(1, &[], 3), group(1, &[], 3)])
        .expect_err("duplicate primary ID is rejected");

    assert!(
        matches!(
            error,
            ManagedDriverError::DuplicateNode { node_id: NodeId(1) }
        ),
        "got {error:?}"
    );
}

#[test]
fn in_memory_driver_rejects_duplicate_non_primary_node_ids() {
    let error = KvDriver::new(
        NodeId(1),
        vec![group(1, &[2], 3), group(2, &[1], 9), group(2, &[1], 9)],
    )
    .expect_err("duplicate non-primary ID is rejected");

    assert!(
        matches!(
            error,
            ManagedDriverError::DuplicateNode { node_id: NodeId(2) }
        ),
        "got {error:?}"
    );
}

#[test]
fn in_memory_driver_rejects_poisoned_primary_on_adoption() {
    let error = KvDriver::new(NodeId(1), vec![poisoned_group(1)])
        .expect_err("poisoned primary group is rejected");

    assert!(matches!(
        error,
        ManagedDriverError::PoisonedGroup {
            node_id: NodeId(1),
            reason,
        } if reason == "ApplyBatch failed"
    ));
}

#[test]
fn in_memory_driver_rejects_poisoned_non_primary_on_adoption() {
    let error = KvDriver::new(NodeId(1), vec![group(1, &[], 3), poisoned_group(2)])
        .expect_err("poisoned non-primary group is rejected");

    assert!(matches!(
        error,
        ManagedDriverError::PoisonedGroup {
            node_id: NodeId(2),
            reason,
        } if reason == "ApplyBatch failed"
    ));
}

#[test]
fn in_memory_driver_seeds_proposal_ids_above_adopted_group_watermark() {
    let mut adopted = group(1, &[], 3);
    let begin = adopted
        .begin_proposal_outcome(Proposal {
            local_proposal_id: LocalProposalId(10),
            client_request_id: None,
            command: ("manual".to_owned(), "one".to_owned()),
        })
        .expect("manual proposal reaches a terminal result");
    assert!(matches!(begin, ProposalBegin::Rejected { .. }));
    assert_eq!(
        adopted.local_proposal_id_watermark(),
        Some(LocalProposalId(10))
    );
    assert_eq!(adopted.metrics().pending_proposals, 0);
    assert_eq!(adopted.metrics().reserved_reads, 0);

    let driver = KvDriver::new_elected(NodeId(1), vec![adopted])
        .expect("quiescent manually driven group is adoptable");
    let receipt = block_on(
        driver
            .handle()
            .write(("managed".to_owned(), "two".to_owned())),
    )
    .expect("managed write uses a proposal ID above the adopted watermark");

    assert_eq!(receipt.result, None);
}

#[test]
fn in_memory_driver_seeds_read_ids_above_adopted_group_watermark() {
    let mut adopted = scripted_read_group(ScriptedReadMode::Reject);
    let manual = adopted
        .begin_read_barrier_outcome(ReadBarrierRequest {
            group_id: (),
            read_id: ReadId(10),
            min_applied_index: None,
            context: Vec::new(),
        })
        .expect("manual read reaches a terminal result");
    assert!(matches!(manual, ReadProofOutcome::Rejected { .. }));
    assert_eq!(adopted.read_id_watermark(), Some(ReadId(10)));
    assert_eq!(adopted.metrics().reserved_reads, 0);

    let driver = ScriptedReadDriver::new(NodeId(1), vec![adopted])
        .expect("quiescent manually driven group is adoptable");
    let handle = driver.handle();

    let error = block_on(handle.read("manual".to_owned(), ReadConsistency::Linearizable))
        .expect_err("the scripted group refuses the barrier");

    assert!(
        matches!(
            error,
            ReadError::Rejected {
                read_id: Some(ReadId(11)),
                reason: ReadIndexRejection::NotLeader {
                    role: Role::Follower,
                    term: Term(1),
                },
                leader_hint: Some(NodeId(1)),
            }
        ),
        "got {error:?}"
    );
}

#[test]
fn in_memory_driver_rejects_adopted_group_with_pending_proposal() {
    let mut group = scripted_write_group(ScriptedWriteMode::AppendThenIdle);
    let begin = group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: LocalProposalId(1),
            client_request_id: None,
            command: ("manual".to_owned(), "one".to_owned()),
        })
        .expect("scripted proposal appends and remains pending");
    assert!(matches!(begin, ProposalBegin::Appended { .. }));
    assert_eq!(group.metrics().pending_proposals, 1);

    let error = ScriptedWriteDriver::new(NodeId(1), vec![group])
        .expect_err("non-quiescent group is rejected");

    assert!(
        matches!(
            error,
            ManagedDriverError::NonQuiescentGroup {
                node_id: NodeId(1),
                pending_proposals: 1,
                reserved_reads: 0,
            }
        ),
        "got {error:?}"
    );
}

#[test]
fn in_memory_driver_rejects_adopted_group_with_reserved_read_state() {
    let read_id = ReadId(11);
    let mut group = scripted_read_group(ScriptedReadMode::Pending);
    let read = group
        .read(ReadRequest::Linearizable {
            group_id: (),
            read_id,
            query: "manual".to_owned(),
            min_applied_index: None,
            context: Vec::new(),
        })
        .expect("scripted read starts and remains pending");
    assert!(matches!(read.outcome, ReadOutcome::Pending { .. }));
    assert_eq!(group.metrics().reserved_reads, 1);

    let error = ScriptedReadDriver::new(NodeId(1), vec![group])
        .expect_err("reserved read state is rejected");

    assert!(
        matches!(
            error,
            ManagedDriverError::NonQuiescentGroup {
                node_id: NodeId(1),
                pending_proposals: 0,
                reserved_reads: 1,
            }
        ),
        "got {error:?}"
    );
}

#[test]
fn in_memory_driver_rejects_adopted_group_with_exhausted_proposal_watermark() {
    let mut group = group(1, &[], 3);
    group
        .begin_proposal_outcome(Proposal {
            local_proposal_id: LocalProposalId(u64::MAX),
            client_request_id: None,
            command: ("manual".to_owned(), "one".to_owned()),
        })
        .expect("manual proposal consumes the maximum local proposal id");

    let error =
        KvDriver::new(NodeId(1), vec![group]).expect_err("exhausted adopted watermark is rejected");

    assert!(
        matches!(
            error,
            ManagedDriverError::LocalProposalIdExhausted {
                node_id: NodeId(1),
                last_seen_local_proposal_id: LocalProposalId(u64::MAX),
            }
        ),
        "got {error:?}"
    );
}

#[test]
fn in_memory_driver_rejects_adopted_group_with_exhausted_read_watermark() {
    let mut group = scripted_read_group(ScriptedReadMode::Reject);
    group
        .begin_read_barrier_outcome(ReadBarrierRequest {
            group_id: (),
            read_id: ReadId(u64::MAX),
            min_applied_index: None,
            context: Vec::new(),
        })
        .expect("manual read consumes the maximum read id");

    let error = ScriptedReadDriver::new(NodeId(1), vec![group])
        .expect_err("exhausted adopted read watermark is rejected");

    assert!(
        matches!(
            error,
            ManagedDriverError::ReadIdExhausted {
                node_id: NodeId(1),
                last_seen_read_id: ReadId(u64::MAX),
            }
        ),
        "got {error:?}"
    );
}
