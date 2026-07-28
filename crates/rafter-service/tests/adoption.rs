#![allow(clippy::wildcard_imports)]

//! Which groups a driver may take, on both shipped drivers.
//!
//! The two answer the same question with the same error. `InMemoryRaftDriver`
//! takes a set of replicas at construction and refuses a set that mixes group
//! IDs; `TransportRaftDriver` takes one replica at a time and refuses one whose
//! ID is not the ID it was built with. Both say
//! [`ManagedDriverError::MixedGroups`].

mod support;

use rafter_service::{DriverCommandSender, PeerControlPlaneCheckpoint, WriteOptions};

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
///
/// **The two incarnations are replicas of one cluster**, which this fixture used
/// to leave to chance: it adopted a single-voter group `{2}` into a driver that
/// had been the single voter of `{1}`, at the same commit index. That is two
/// different clusters sharing a group ID, and the driver absorbed it — the
/// runtime won the tie, node 1 left the committed membership, and the adoption
/// quietly spent the identity the previous incarnation had been serving under.
/// Nothing asserted otherwise, so the fixture passed while stating a sequence no
/// supervisor produces: a replacement rebuilds from the same durable storage and
/// reports the same committed membership, or a later one.
#[test]
fn adopting_a_new_node_id_under_the_same_group_is_accepted() {
    let (driver, _transport) = driver_for(1, &[2]);
    let _retired = driver.release_group().expect("the driver holds a group");

    driver
        .adopt_group(numbered_group(GROUP, 2, &[1], 3), Vec::new())
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

/// The membership every replica in the cases below bootstraps under.
///
/// **Two replicas rather than one, so a replacement incarnation names the same
/// committed membership the retired one did.** A single-voter fixture would give
/// node 1's group `{1}` and node 2's `{2}` at the same commit index, which is
/// two different answers to "what did this group commit at index 0" — and a
/// driver reading the pair concludes node 1 was removed. That is the right
/// conclusion about a fixture no cluster produces, and it is not what these
/// cases are about.
const CLUSTER: [u64; 2] = [1, 2];

/// The peers of `node_id` within [`CLUSTER`].
fn peers_of(node_id: u64) -> Vec<u64> {
    CLUSTER.into_iter().filter(|id| *id != node_id).collect()
}

/// A replica of [`CLUSTER`] whose state machine refuses every apply.
///
/// The one way an adoption can fail *after* the group is installed: the
/// watermark and identity checks all run before it, so the recovery outputs are
/// what separates a refusal from a partial adoption.
fn refusing_group(node_id: u64) -> NumberedGroup {
    numbered_group_with_app(
        GROUP,
        node_id,
        &peers_of(node_id),
        3,
        KvStateMachine {
            fail_apply: true,
            ..KvStateMachine::default()
        },
    )
}

/// An ordinary replica of [`CLUSTER`].
fn cluster_group(node_id: u64) -> NumberedGroup {
    numbered_group(GROUP, node_id, &peers_of(node_id), 3)
}

/// One recovery output the state machine above will refuse.
fn a_refused_apply() -> Vec<RaftOutput> {
    vec![RaftOutput::Apply {
        index: LogIndex(1),
        term: Term(1),
        payload: SharedPayload::from(&b"key\nvalue"[..]),
        local_proposal_id: None,
    }]
}

/// A failed adoption still installs the group, and says so.
///
/// **`Result<(), _>` cannot distinguish a refusal from a partial adoption, so
/// the distinction is pinned here.** Everything the method checks before it
/// installs anything leaves the driver holding no group; only the recovery
/// outputs fail afterwards, and that adoption is not rolled back. Rolling it
/// back would *drop* the group, and a caller holding a unit result has no other
/// way to reach it — so the group stays installed and `release_group` is how a
/// supervisor gets it back.
#[test]
fn a_failed_adoption_installs_the_group_and_leaves_it_reachable() {
    let (driver, _transport) = driver_for(1, &peers_of(1));
    let _retired = driver.release_group().expect("the driver holds a group");

    let failed = driver.adopt_group(refusing_group(2), a_refused_apply());

    assert!(failed.is_err(), "the state machine refused the apply");
    assert_eq!(
        driver
            .with_group(RaftGroup::node_id)
            .expect("the group is installed despite the error"),
        NodeId(2),
        "the identity moved with it, so this driver is the replica it adopted"
    );
    assert!(
        matches!(
            driver.adopt_group(cluster_group(1), Vec::new()),
            Err(ManagedDriverError::GroupAlreadyAdopted)
        ),
        "a second adoption is refused until the first is released"
    );
    driver
        .release_group()
        .expect("release is how a supervisor recovers the group");
}

/// The link layer hears about the installed group even when the outputs failed.
///
/// The one statement a later call cannot repair on its own. A peer set left
/// describing the retired incarnation is not stale, it is wrong about who may
/// speak — and nothing re-derives it until the cluster's next configuration
/// change, which may never come. So the publication runs after the recovery
/// outputs whatever they did, and the `?` that used to sit on that line is the
/// whole of the defect this pins.
#[test]
fn a_failed_adoption_still_publishes_what_the_installed_group_requires() {
    let (driver, transport) = driver_for(1, &peers_of(1));
    let _retired = driver.release_group().expect("the driver holds a group");
    let published_before = transport.peer_sets().len();

    let failed = driver.adopt_group(refusing_group(2), a_refused_apply());

    assert!(failed.is_err());
    assert!(
        transport.peer_sets().len() > published_before,
        "the transport was told what the adopted group requires: {:?}",
        transport.peer_sets()
    );
}

/// A refusal before the installation leaves the driver holding no group.
///
/// The other half of the contract, and the reason the two are separate
/// paragraphs at the method: a supervisor reading an `Err` needs to know whether
/// its next call is `release_group` or another `adopt_group`.
#[test]
fn a_refusal_before_the_installation_leaves_no_group_behind() {
    let (driver, _transport) = driver_for(1, &peers_of(1));
    let _retired = driver.release_group().expect("the driver holds a group");

    let refused = driver.adopt_group(numbered_group(FOREIGN, 2, &[1], 3), Vec::new());

    assert!(
        matches!(refused, Err(ManagedDriverError::MixedGroups)),
        "got {refused:?}"
    );
    assert!(
        matches!(
            driver.with_group(RaftGroup::node_id),
            Err(ManagedDriverError::NoGroup)
        ),
        "a refusal installs nothing, so the next call is another adoption"
    );
    driver
        .adopt_group(cluster_group(2), Vec::new())
        .expect("and that adoption is an ordinary first attempt");
}

/// A retry after a failed adoption is legal, and the checkpoint it merged stays
/// merged.
///
/// The join is monotone and idempotent, so offering the same record again adds
/// nothing — which is what makes "release, rebuild, adopt again" the whole
/// recovery procedure rather than one that has to reason about what the failed
/// attempt absorbed.
#[test]
fn a_retry_after_a_failed_adoption_reaches_the_same_record() {
    let (driver, _transport) = driver_for(1, &peers_of(1));
    let _retired = driver.release_group().expect("the driver holds a group");
    let checkpoint = driver.control_plane_checkpoint();

    assert!(driver
        .adopt_group_with_checkpoint(refusing_group(2), a_refused_apply(), checkpoint.clone())
        .is_err());
    let after_failure = driver.control_plane_checkpoint();
    let _half_adopted = driver.release_group().expect("the group is reachable");

    driver
        .adopt_group_with_checkpoint(cluster_group(2), Vec::new(), checkpoint)
        .expect("the same record is offered again and adds nothing");

    assert_eq!(
        driver.control_plane_checkpoint().committed_id_high_water,
        after_failure.committed_id_high_water,
        "the retry re-derived the same retirement record"
    );
    assert_eq!(
        driver.control_plane_checkpoint().current_committed,
        after_failure.current_committed,
        "and stands exactly where it stood"
    );
}

/// The control: an empty checkpoint through the same path still adopts.
#[test]
fn an_adoption_with_a_checkpoint_and_no_recovery_outputs_succeeds() {
    let (driver, _transport) = driver_for(1, &peers_of(1));
    let _retired = driver.release_group().expect("the driver holds a group");

    driver
        .adopt_group_with_checkpoint(
            cluster_group(2),
            Vec::new(),
            PeerControlPlaneCheckpoint::empty(GROUP),
        )
        .expect("an ordinary adoption");

    assert_eq!(
        driver
            .with_group(RaftGroup::node_id)
            .expect("the driver holds the adopted group"),
        NodeId(2)
    );
}
