#![allow(clippy::wildcard_imports)]

//! Which groups a driver may take, on both shipped drivers.
//!
//! The two answer the same question with the same error. `InMemoryRaftDriver`
//! takes a set of replicas at construction and refuses a set that mixes group
//! IDs; `TransportRaftDriver` takes one replica at a time and refuses one whose
//! ID is not the ID it was built with. Both say
//! [`ManagedDriverError::MixedGroups`].

mod support;

use rafter_service::{DriverCommandSender, WriteOptions};

use support::transport::{driver_for, tick_past_election_timeout, GROUP};
use support::*;

/// One above the ID `support::transport` builds its drivers with.
const FOREIGN: u64 = GROUP + 1;

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

// ---------------------------------------------------------------------------
// Group identity, on both drivers. Adopted from the gen-7 reproduction, which
// found `TransportRaftDriver::adopt_group` taking any group at all.
// ---------------------------------------------------------------------------

/// Adopting a group that serves a different group ID must be refused; the
/// driver's handles, metrics, and every client-facing group check keep using
/// the ID it was built with.
#[test]
fn adopting_a_foreign_group_is_refused() {
    let (driver, _transport) = driver_for(1, &[]);
    let _retired = driver.release_group().expect("the driver holds a group");

    let adopted = driver.adopt_group(numbered_group(FOREIGN, 1, &[], 3), Vec::new());

    assert!(
        matches!(adopted, Err(ManagedDriverError::MixedGroups)),
        "a driver for group {GROUP} adopted a group for {FOREIGN}: {adopted:?}"
    );
}

/// The consequence: client writes addressed to the driver's group are proposed
/// into the foreign group's log, while every other surface still refuses the
/// foreign ID.
#[test]
fn a_foreign_adoption_does_not_route_writes_into_the_wrong_group() {
    let (driver, _transport) = driver_for(1, &[]);
    let _retired = driver.release_group().expect("the driver holds a group");
    let Ok(()) = driver.adopt_group(numbered_group(FOREIGN, 1, &[], 3), Vec::new()) else {
        // If adoption is refused this probe has nothing to show, which is the
        // outcome the sibling test asks for.
        return;
    };

    tick_past_election_timeout(&driver);
    let receipt = block_on(driver.write(
        GROUP,
        ("alpha".to_owned(), "one".to_owned()),
        WriteOptions::default(),
    ));

    let served_group = driver
        .with_group(|group| *group.group_id())
        .expect("the driver holds the adopted group");
    assert!(
        receipt.is_err(),
        "a write addressed to group {GROUP} was committed into group \
         {served_group}: {receipt:?}"
    );
}

/// The rule is about identity, not about novelty: a driver re-adopts its own
/// group ID, which is what a supervisor restarting a replica does.
#[test]
fn adopting_the_drivers_own_group_id_is_accepted() {
    let (driver, _transport) = driver_for(1, &[]);
    let _retired = driver.release_group().expect("the driver holds a group");

    driver
        .adopt_group(numbered_group(GROUP, 1, &[], 3), Vec::new())
        .expect("a group with the driver's own ID is adoptable");

    let served_group = driver
        .with_group(|group| *group.group_id())
        .expect("the driver holds the adopted group");
    assert_eq!(served_group, GROUP);
}

/// OUTSIDE the identity check: the node ID. A replacement incarnation is still
/// a replica of the same group, so a new node ID under the same group ID is
/// adopted and becomes the ID the driver drives.
#[test]
fn adopting_a_new_node_id_under_the_same_group_is_accepted() {
    let (driver, _transport) = driver_for(1, &[]);
    let _retired = driver.release_group().expect("the driver holds a group");

    driver
        .adopt_group(numbered_group(GROUP, 2, &[], 3), Vec::new())
        .expect("a new node ID under the same group ID is adoptable");

    let served_node = driver
        .with_group(RaftGroup::node_id)
        .expect("the driver holds the adopted group");
    assert_eq!(served_node, NodeId(2));
}

/// The sibling rule this one is made to agree with: the in-memory driver
/// refuses a set of replicas that does not name one group.
#[test]
fn in_memory_driver_rejects_groups_that_do_not_share_one_group_id() {
    let error = NumberedDriver::new(
        NodeId(1),
        vec![
            numbered_group(GROUP, 1, &[2], 3),
            numbered_group(FOREIGN, 2, &[1], 9),
        ],
    )
    .expect_err("a mixed set of group ids is rejected");

    assert!(
        matches!(error, ManagedDriverError::MixedGroups),
        "got {error:?}"
    );
}
