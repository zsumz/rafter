//! A step can commit more than one configuration, and every one of them counts.
//!
//! Everything here runs against a **real kernel** — `DurableRaftNode` over the
//! in-memory hard-state store, driven through `deliver` with frames a leader
//! would actually send. That is the point of the file rather than a detail of
//! it: the defect these tests pin was invisible to every fixture that scripted a
//! runtime's memberships, because a script says what the membership *is* before
//! and after a step and the loss lives entirely in between.
//!
//! The shape is a lagging replica catching up. A follower one round behind
//! receives several configuration entries in one `AppendEntries` whose leader
//! commit covers all of them, so the commit index crosses each in a single
//! advance. If an intermediate configuration admitted a replica that a later one
//! removed, the committed membership is *identical* before and after — and a
//! driver that sampled it once per step recorded nothing at all while the
//! cluster spent an identity. The removed replica was never fenced, its ID never
//! rose above the high-water mark, and a later contract-violating readmission of
//! it was accepted as ordinary.

#![allow(clippy::wildcard_imports)]

mod support;

use rafter::{
    AppendEntries, ConfigurationEntry, ConfigurationId, LogEntry, MembershipSet, SharedEntries,
};
use rafter_service::{AuthenticatedPeerEnvelope, InboundEnvelopeError};
use support::transport::*;
use support::*;

fn voters(node_ids: &[u64]) -> MembershipSet {
    MembershipSet::new(node_ids.iter().copied().map(NodeId).collect(), Vec::new())
        .expect("test membership is valid")
}

fn stable(config_id: u64, node_ids: &[u64]) -> ConfigurationEntry {
    ConfigurationEntry::stable(ConfigurationId(config_id), voters(node_ids))
}

fn principals(node_ids: &[u64]) -> Vec<Principal> {
    node_ids
        .iter()
        .map(|node_id| Principal::for_node(NodeId(*node_id)))
        .collect()
}

/// One `AppendEntries` from the leader, carrying `entries` and committing all of
/// them.
fn append_committing_all(
    from: NodeId,
    to: NodeId,
    entries: Vec<LogEntry>,
) -> AuthenticatedPeerEnvelope<u64, Principal> {
    let leader_commit = LogIndex(entries.len() as u64);
    AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(from),
        raft_from: from,
        raft_to: to,
        message: Message::AppendEntries(AppendEntries {
            term: Term(1),
            leader_id: from,
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term(0),
            sequence: 1,
            entries: SharedEntries::from(entries),
            leader_commit,
        }),
    }
}

fn a_vote(from: NodeId) -> AuthenticatedPeerEnvelope<u64, Principal> {
    AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(from),
        raft_from: from,
        raft_to: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: from,
            last_log_index: LogIndex(2),
            last_log_term: Term(1),
        }),
    }
}

/// The reviewer's case, end to end and with nothing scripted.
///
/// Committed `{1,2,3}`; the cluster commits `+5` and then `−5`; this replica is
/// one round behind and catches up in a single append whose commit floor covers
/// both. The committed membership is `{1,2,3}` before the step and `{1,2,3}`
/// after it, so a difference of endpoints is empty — and node 5 was admitted and
/// retired in between.
#[test]
fn one_append_crossing_two_configurations_spends_the_intermediate_identity() {
    // Bootstrapped over `{1,2,3}`; the directory can name and authenticate node
    // 5 the whole time, which is what a deployment that is about to admit it
    // looks like.
    let (driver, transport) = driver_over_bootstrap(1, &[2, 3], &[2, 3, 5]);

    assert_eq!(
        driver.control_plane_checkpoint().committed_id_high_water,
        Some(NodeId(3)),
        "adoption observed the bootstrap configuration and nothing above it"
    );

    driver
        .deliver(append_committing_all(
            NodeId(2),
            NodeId(1),
            vec![
                LogEntry::configuration(Term(1), stable(1, &[1, 2, 3, 5])),
                LogEntry::configuration(Term(1), stable(2, &[1, 2, 3])),
            ],
        ))
        .expect("a leader's append is accepted");

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(
        checkpoint.committed_id_high_water,
        Some(NodeId(5)),
        "the intermediate configuration raised the mark, so node 5 is allocated"
    );
    assert_eq!(
        checkpoint.live_committed_members,
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect(),
        "and the configuration that removed it took it back out of the live set"
    );
    assert!(
        !checkpoint.live_committed_members.contains(&NodeId(5)),
        "node 5 is spent: at or below the mark and not live"
    );

    assert!(
        transport.is_fenced(NodeId(5)),
        "the removal that this step also crossed licenses the fence, and the \
         link layer took it"
    );
    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2, 3]),
        "and the published peer set does not name the identity the cluster spent"
    );

    // Refused by the validator rather than by the membership, because the fence
    // took: this deployment's directory consults its own link layer, so a
    // principal it has permanently fenced never reaches the driver. The
    // membership half of the same refusal is pinned by
    // `a_crossed_removal_leaves_its_fence_owed_when_the_link_refuses_it`, where
    // the fence did not take and the driver is the only layer left.
    assert!(
        matches!(
            driver.deliver(a_vote(NodeId(5))),
            Err(InboundEnvelopeError::Rejected { .. })
        ),
        "a frame from the spent identity is refused"
    );
}

/// The fence obligation survives a link layer that refuses it.
///
/// The same crossing, with a transport that will not take the fence. The
/// obligation is recorded rather than discharged, which is what a restart has to
/// carry: the removal is already behind the committed membership, so nothing
/// re-derives it.
#[test]
fn a_crossed_removal_leaves_its_fence_owed_when_the_link_refuses_it() {
    let (driver, transport) = driver_over_bootstrap(1, &[2, 3], &[2, 3, 5]);
    transport.refuse_next_fences(NodeId(5), 8);

    driver
        .deliver(append_committing_all(
            NodeId(2),
            NodeId(1),
            vec![
                LogEntry::configuration(Term(1), stable(1, &[1, 2, 3, 5])),
                LogEntry::configuration(Term(1), stable(2, &[1, 2, 3])),
            ],
        ))
        .expect("a leader's append is accepted");

    assert_eq!(
        driver.pending_peer_fences(),
        1,
        "the refused fence stays owed"
    );
    assert_eq!(
        driver.control_plane_checkpoint().pending_fences,
        [NodeId(5)].into_iter().collect(),
        "and it is in the checkpoint, which is the only thing a restart can read"
    );

    // The window this fence exists to close, seen from inside: the link layer
    // still authorizes node 5, and the driver's own membership is what refuses
    // it. Nothing about the identity being spent depended on the fence landing.
    assert!(
        matches!(
            driver.deliver(a_vote(NodeId(5))),
            Err(InboundEnvelopeError::NotInMembership { .. })
        ),
        "the driver fails closed on the identity the crossing spent"
    );
    assert_eq!(driver.refused_non_member_frames(), 1);
}

/// Three configurations in one step, reported in index order.
///
/// The ordering is what lets a consumer replay the history: each event names the
/// entry that carried it, and the spent test is computed against the state the
/// previous one left. Out of order, the `+5` that follows a `−5` would look like
/// a readmission of a spent identity rather than the allocation it is.
#[test]
fn a_multi_configuration_step_is_reported_in_index_order() {
    let (driver, transport) = driver_over_bootstrap(1, &[2, 3], &[2, 3, 4, 5]);

    driver
        .deliver(append_committing_all(
            NodeId(2),
            NodeId(1),
            vec![
                LogEntry::configuration(Term(1), stable(1, &[1, 2, 3, 4])),
                LogEntry::configuration(Term(1), stable(2, &[1, 2, 3, 4, 5])),
                LogEntry::configuration(Term(1), stable(3, &[1, 2, 3, 5])),
            ],
        ))
        .expect("a leader's append is accepted");

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(
        checkpoint.committed_id_high_water,
        Some(NodeId(5)),
        "the mark is the highest identity any crossed configuration named"
    );
    assert_eq!(
        checkpoint.live_committed_members,
        [NodeId(1), NodeId(2), NodeId(3), NodeId(5)]
            .into_iter()
            .collect(),
        "node 4 joined in the first configuration and left in the third; node 5 \
         joined in the second and stayed"
    );
    assert!(
        transport.is_fenced(NodeId(4)),
        "the removal in the middle of the step is fenced"
    );
    assert!(
        !transport.is_fenced(NodeId(5)),
        "and the addition in the middle of it is not"
    );
    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2, 3, 5]),
    );
}

/// The control: a step that crosses one configuration behaves exactly as before.
///
/// Worth stating, because the per-crossing stream replaced a per-step
/// comparison and the single-crossing case is the one every existing consumer
/// exercises. One configuration in, one event out, and the identity it removed
/// is spent.
#[test]
fn a_single_crossed_configuration_still_retires_exactly_what_left() {
    let (driver, transport) = driver_for(1, &[2, 3]);

    driver
        .deliver(append_committing_all(
            NodeId(2),
            NodeId(1),
            vec![LogEntry::configuration(Term(1), stable(1, &[1, 2]))],
        ))
        .expect("a leader's append is accepted");

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(checkpoint.committed_id_high_water, Some(NodeId(3)));
    assert_eq!(
        checkpoint.live_committed_members,
        [NodeId(1), NodeId(2)].into_iter().collect()
    );
    assert!(transport.is_fenced(NodeId(3)));
}

/// A configuration that commits nothing new retires nothing.
///
/// The other control, and the one that would catch an over-eager fix: replaying
/// the same membership as a fresh crossing must not make its own members look
/// like arrivals or departures.
#[test]
fn recommitting_the_same_membership_retires_nothing() {
    let (driver, transport) = driver_for(1, &[2, 3]);

    driver
        .deliver(append_committing_all(
            NodeId(2),
            NodeId(1),
            vec![
                LogEntry::configuration(Term(1), stable(1, &[1, 2, 3])),
                LogEntry::configuration(Term(1), stable(2, &[1, 2, 3])),
            ],
        ))
        .expect("a leader's append is accepted");

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(checkpoint.committed_id_high_water, Some(NodeId(3)));
    assert_eq!(
        checkpoint.live_committed_members,
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect()
    );
    assert_eq!(driver.pending_peer_fences(), 0);
    assert!(!transport.is_fenced(NodeId(2)));
    assert!(!transport.is_fenced(NodeId(3)));
}

/// An entry the state machine cannot decode does not hide the configurations
/// the same commit crossed behind it.
///
/// The failure this file exists for, reached through the one producer its other
/// cases cannot script: a step that fails *inside* the output scan rather than
/// after it. The app layer walks the vector once and decodes each `Apply` as it
/// goes, so a malformed payload at index 1 abandons the walk with the
/// configurations at 2 and 3 unvisited — and because the commit both admits node
/// 5 and removes it again, the endpoint the driver would otherwise fall back on
/// is the membership it started from. Nothing to compare, nothing owed, and an
/// identity the cluster spent left allocatable with its principal unfenced.
///
/// The fence is refused here so the obligation is *observable* rather than
/// discharged on the way past. That is also the state that matters: a driver
/// which recorded the retirement but dropped the fence is the one whose next
/// restart forgets it.
#[test]
fn an_undecodable_entry_does_not_hide_the_configurations_committed_behind_it() {
    let (driver, transport) = driver_over_bootstrap(1, &[2, 3], &[2, 3, 5]);
    transport.refuse_next_fences(NodeId(5), 8);

    let refused = driver.deliver(append_committing_all(
        NodeId(2),
        NodeId(1),
        vec![
            // No newline, which is what this state machine's decoder requires.
            LogEntry::application(Term(1), b"malformed".to_vec()),
            LogEntry::configuration(Term(1), stable(1, &[1, 2, 3, 5])),
            LogEntry::configuration(Term(1), stable(2, &[1, 2, 3])),
        ],
    ));
    assert!(
        matches!(refused, Err(InboundEnvelopeError::Driver { .. })),
        "the step fails on the payload it cannot decode: {refused:?}"
    );

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(
        checkpoint.committed_id_high_water,
        Some(NodeId(5)),
        "the admission raised the mark even though the step failed"
    );
    assert!(
        !checkpoint.live_committed_members.contains(&NodeId(5)),
        "and the removal behind it spent the identity: {:?}",
        checkpoint.live_committed_members
    );
    assert_eq!(
        checkpoint.pending_fences,
        [NodeId(5)].into_iter().collect(),
        "the fence the link refused is owed rather than forgotten"
    );
    assert_eq!(driver.pending_peer_fences(), 1);
    assert_eq!(
        driver.control_plane_checkpoint().live_committed_members,
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect()
    );
}
