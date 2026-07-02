use std::fmt;

use super::helpers::{
    config, deliver_append_entries, deliver_append_entries_response, direct_election_config,
    elect_node_one, request_vote,
};
use super::*;
use crate::model_check::MessageKind;
use rafter::NodeConfig;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    BootstrapState, ConfigurationEntry, ConfigurationId, JointMembership, MembershipConfig,
    MembershipSet, RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId,
};

#[test]
fn add_voter_transition_preserves_committed_prefix() {
    let mut cluster = learner_four_node_cluster(SimSeed(0xadd0));
    elect_node_one(&mut cluster);

    cluster.propose(NodeId(1), b"before-add".to_vec());
    cluster.deliver_all();
    commit_add_voter_transition(&mut cluster, ConfigurationId(20));
    cluster.propose(NodeId(1), b"after-add".to_vec());
    cluster.deliver_all();
    flush_commit_notifications(&mut cluster, NodeId(1));

    for node_id in [NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
        assert_eq!(
            applied_payloads(&cluster, node_id),
            vec![b"before-add".to_vec(), b"after-add".to_vec()],
            "{node_id} should preserve the committed prefix through add-voter"
        );
        assert!(cluster
            .effective_membership(node_id)
            .contains_voter(NodeId(4)));
    }
}

#[test]
fn remove_voter_transition_preserves_prefix_and_steps_down_removed_leader() {
    let mut cluster = direct_election_stable_four_voter_cluster(SimSeed(0xfeed), NodeId(2));
    elect_node_two(&mut cluster);

    cluster.propose(NodeId(2), b"before-remove".to_vec());
    cluster.deliver_all();
    commit_remove_node_two_transition(&mut cluster, ConfigurationId(30));

    assert_eq!(cluster.role(NodeId(2)), Role::Follower);
    for retained in [NodeId(1), NodeId(3), NodeId(4)] {
        assert!(!cluster
            .effective_membership(retained)
            .contains_voter(NodeId(2)));
        assert_eq!(
            applied_payloads(&cluster, retained),
            vec![b"before-remove".to_vec()],
            "{retained} should keep the prefix committed before removal"
        );
    }

    elect_node_one_after_removal(&mut cluster);
    cluster.propose(NodeId(1), b"after-remove".to_vec());
    cluster.deliver_all();
    flush_commit_notifications(&mut cluster, NodeId(1));

    for retained in [NodeId(1), NodeId(3), NodeId(4)] {
        assert_eq!(
            applied_payloads(&cluster, retained),
            vec![b"before-remove".to_vec(), b"after-remove".to_vec()],
            "{retained} should commit after removal"
        );
    }
    assert_eq!(
        applied_payloads(&cluster, NodeId(2)),
        vec![b"before-remove".to_vec()],
        "removed voter should not receive post-removal proposals"
    );
}

#[test]
fn partitioned_joint_configuration_does_not_create_conflicting_leaders() {
    let mut cluster = direct_election_learner_four_node_cluster(SimSeed(0x91));
    elect_node_one(&mut cluster);
    let barrier = cluster
        .promotion_barrier(NodeId(1), NodeId(4))
        .expect("learner has caught up during election heartbeats");

    cluster.dangerous_raw_configuration_proposal(
        NodeId(1),
        add_voter_joint_entry(ConfigurationId(40)),
        vec![barrier],
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(4))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries_response(NodeId(4), NodeId(1))),
        1
    );
    assert!(
        cluster.drop_matching(|envelope| envelope.from == NodeId(1) || envelope.to == NodeId(1))
            >= 2
    );

    for _ in 0..9 {
        cluster.tick(NodeId(4));
    }
    assert!(cluster.deliver_one_matching(request_vote(NodeId(4), NodeId(1))));
    assert!(cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(1)
            && envelope.to == NodeId(4)
            && matches!(envelope.message, rafter::Message::RequestVoteResponse(_))
    }));
    assert!(cluster.deliver_one_matching(request_vote(NodeId(4), NodeId(3))));
    assert!(cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(3)
            && envelope.to == NodeId(4)
            && matches!(
                envelope.message,
                rafter::Message::RequestVoteResponse(rafter::RequestVoteResponse {
                    vote_granted: false,
                    ..
                })
            )
    }));
    // Node 4 carries the joint configuration entry, but node 3 has not
    // learned that configuration yet, so active voter fencing makes node 3
    // reject the vote instead of binding itself to a candidate it does not
    // currently recognize as a voter.
    assert_eq!(cluster.role(NodeId(4)), Role::Candidate);

    // Node 2 can still win the same term under node 3's old configuration:
    // the rejected vote for node 4 did not consume node 3's one real vote.
    for _ in 0..9 {
        cluster.tick(NodeId(2));
    }
    assert!(cluster.deliver_one_matching(request_vote(NodeId(2), NodeId(3))));
    assert!(cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(3)
            && envelope.to == NodeId(2)
            && matches!(envelope.message, rafter::Message::RequestVoteResponse(_))
    }));
    assert_eq!(cluster.role(NodeId(2)), Role::Leader);

    let max_term = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
        .into_iter()
        .map(|node_id| cluster.current_term(node_id).0)
        .max()
        .expect("cluster has nodes");
    for term in 1..=max_term {
        assert!(
            cluster.leaders_in_term(Term(term)).len() <= 1,
            "term {term} should not have conflicting leaders"
        );
    }
}

#[test]
fn randomized_membership_trace_is_replayable() {
    let (trace, expected) = record_randomized_add_voter_trace(SimSeed(0x4459_4e41));
    let replayed = replay_membership_trace(&trace);

    assert_eq!(replayed, expected);
    assert!(trace
        .iter()
        .any(|action| matches!(action, MembershipTraceAction::Deliver { .. })));
    assert!(trace
        .iter()
        .any(|action| matches!(action, MembershipTraceAction::ProposeAddVoterJoint)));
    assert!(trace
        .iter()
        .any(|action| action.to_string().starts_with("deliver ")));
}

#[test]
fn randomized_reconfiguration_survives_snapshot_compaction_and_restart() {
    let mut cluster = learner_four_node_cluster(SimSeed(0x5a7c_0a91));
    elect_node_one(&mut cluster);
    let mut trace = Vec::new();

    cluster.propose(NodeId(1), b"before-compact".to_vec());
    drain_random_ready(&mut cluster, &mut trace);
    propose_add_voter_joint(&mut cluster, ConfigurationId(60));
    drain_random_ready(&mut cluster, &mut trace);
    cluster.dangerous_raw_configuration_proposal(
        NodeId(1),
        add_voter_final_entry(ConfigurationId(61)),
        Vec::new(),
    );
    drain_random_ready(&mut cluster, &mut trace);
    cluster.propose(NodeId(1), b"after-reconfig".to_vec());
    drain_random_ready(&mut cluster, &mut trace);

    for node_id in [NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
        assert!(
            cluster
                .effective_membership(node_id)
                .contains_voter(NodeId(4)),
            "{node_id} should recover the new voter before compaction"
        );
    }
    restart_all_nodes_from_compacted_snapshots(&mut cluster);

    elect_node_one(&mut cluster);
    cluster.propose(NodeId(1), b"after-compaction-restart".to_vec());
    drain_random_ready(&mut cluster, &mut trace);
    flush_commit_notifications(&mut cluster, NodeId(1));

    for node_id in [NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
        assert!(
            cluster
                .effective_membership(node_id)
                .contains_voter(NodeId(4)),
            "{node_id} should retain the dynamic voter after snapshot restart"
        );
        assert!(
            applied_payloads(&cluster, node_id)
                .iter()
                .any(|payload| payload.as_ref() == b"after-compaction-restart"),
            "{node_id} should commit after compacted dynamic-membership restart"
        );
    }
    assert!(
        trace
            .iter()
            .any(|action| matches!(action, MembershipTraceAction::Deliver { .. })),
        "the scenario should exercise randomized delivery"
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum MembershipTraceAction {
    Tick(NodeId),
    ProposeAddVoterJoint,
    ProposeAddVoterFinal,
    Deliver {
        from: NodeId,
        to: NodeId,
        message: MessageKind,
    },
}

impl fmt::Display for MembershipTraceAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tick(node_id) => write!(formatter, "tick {node_id}"),
            Self::ProposeAddVoterJoint => formatter.write_str("propose add-voter joint"),
            Self::ProposeAddVoterFinal => formatter.write_str("propose add-voter final"),
            Self::Deliver { from, to, message } => {
                write!(formatter, "deliver {message} {from}->{to}")
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct MembershipTraceSummary {
    leaders: Vec<NodeId>,
    commit_indexes: Vec<(NodeId, LogIndex)>,
    last_log_indexes: Vec<(NodeId, LogIndex)>,
    node_four_is_voter: bool,
}

fn record_randomized_add_voter_trace(
    seed: SimSeed,
) -> (Vec<MembershipTraceAction>, MembershipTraceSummary) {
    let mut cluster = learner_four_node_cluster(seed);
    let mut trace = Vec::new();

    for _ in 0..3 {
        cluster.tick(NodeId(1));
        trace.push(MembershipTraceAction::Tick(NodeId(1)));
    }
    drain_random_ready(&mut cluster, &mut trace);

    propose_add_voter_joint(&mut cluster, ConfigurationId(50));
    trace.push(MembershipTraceAction::ProposeAddVoterJoint);
    drain_random_ready(&mut cluster, &mut trace);

    cluster.dangerous_raw_configuration_proposal(
        NodeId(1),
        add_voter_final_entry(ConfigurationId(51)),
        Vec::new(),
    );
    trace.push(MembershipTraceAction::ProposeAddVoterFinal);
    drain_random_ready(&mut cluster, &mut trace);

    let summary = membership_trace_summary(&cluster);
    (trace, summary)
}

fn replay_membership_trace(trace: &[MembershipTraceAction]) -> MembershipTraceSummary {
    let mut cluster = learner_four_node_cluster(SimSeed(0));

    for action in trace {
        match action {
            MembershipTraceAction::Tick(node_id) => cluster.tick(*node_id),
            MembershipTraceAction::ProposeAddVoterJoint => {
                propose_add_voter_joint(&mut cluster, ConfigurationId(50));
            }
            MembershipTraceAction::ProposeAddVoterFinal => {
                cluster.dangerous_raw_configuration_proposal(
                    NodeId(1),
                    add_voter_final_entry(ConfigurationId(51)),
                    Vec::new(),
                );
            }
            MembershipTraceAction::Deliver { from, to, message } => {
                assert!(
                    cluster.deliver_one_matching(|envelope| envelope.from == *from
                        && envelope.to == *to
                        && MessageKind::from(&envelope.message) == *message),
                    "replay could not deliver {message} {from}->{to}"
                );
            }
        }
    }

    membership_trace_summary(&cluster)
}

fn drain_random_ready(cluster: &mut Cluster, trace: &mut Vec<MembershipTraceAction>) {
    while let Some(envelope) = cluster.deliver_random_ready() {
        trace.push(MembershipTraceAction::Deliver {
            from: envelope.from,
            to: envelope.to,
            message: MessageKind::from(&envelope.message),
        });
    }
}

fn membership_trace_summary(cluster: &Cluster) -> MembershipTraceSummary {
    let node_ids = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)];
    MembershipTraceSummary {
        leaders: cluster.leaders(),
        commit_indexes: node_ids
            .into_iter()
            .map(|node_id| (node_id, cluster.commit_index(node_id)))
            .collect(),
        last_log_indexes: node_ids
            .into_iter()
            .map(|node_id| (node_id, cluster.last_log_index(node_id)))
            .collect(),
        node_four_is_voter: cluster
            .effective_membership(NodeId(1))
            .contains_voter(NodeId(4)),
    }
}

fn commit_add_voter_transition(cluster: &mut Cluster, config_id: ConfigurationId) {
    propose_add_voter_joint(cluster, config_id);
    cluster.deliver_all();
    cluster.dangerous_raw_configuration_proposal(
        NodeId(1),
        add_voter_final_entry(config_id.next()),
        Vec::new(),
    );
    cluster.deliver_all();
}

fn propose_add_voter_joint(cluster: &mut Cluster, config_id: ConfigurationId) {
    let barrier = cluster
        .promotion_barrier(NodeId(1), NodeId(4))
        .expect("learner should have a promotion barrier");
    cluster.dangerous_raw_configuration_proposal(
        NodeId(1),
        add_voter_joint_entry(config_id),
        vec![barrier],
    );
}

fn commit_remove_node_two_transition(cluster: &mut Cluster, config_id: ConfigurationId) {
    cluster.dangerous_raw_configuration_proposal(
        NodeId(2),
        remove_node_two_joint_entry(config_id),
        Vec::new(),
    );
    cluster.deliver_all();
    cluster.dangerous_raw_configuration_proposal(
        NodeId(2),
        remove_node_two_final_entry(config_id.next()),
        Vec::new(),
    );
    cluster.deliver_all();
}

fn elect_node_two(cluster: &mut Cluster) {
    for _ in 0..3 {
        cluster.tick(NodeId(2));
    }
    cluster.deliver_all();
    assert_eq!(cluster.role(NodeId(2)), Role::Leader);
}

fn elect_node_one_after_removal(cluster: &mut Cluster) {
    for _ in 0..9 {
        cluster.tick(NodeId(1));
    }
    cluster.deliver_all();
    assert_eq!(cluster.role(NodeId(1)), Role::Leader);
}

fn flush_commit_notifications(cluster: &mut Cluster, leader_id: NodeId) {
    cluster.tick(leader_id);
    cluster.deliver_all();
}

fn learner_four_node_cluster(seed: SimSeed) -> Cluster {
    cluster_with_membership(seed, &initial_learner_membership(), NodeId(1), config)
}

/// The learner cluster in the minimal-protocol posture, for the partition
/// scenario whose successive candidacies pre-vote stickiness would deny.
fn direct_election_learner_four_node_cluster(seed: SimSeed) -> Cluster {
    cluster_with_membership(
        seed,
        &initial_learner_membership(),
        NodeId(1),
        direct_election_config,
    )
}

/// A four-voter cluster in the minimal-protocol posture: the removal test
/// elects a replacement leader whose peers still hold the removed leader's
/// hint, a direct election by construction.
fn direct_election_stable_four_voter_cluster(seed: SimSeed, leader: NodeId) -> Cluster {
    cluster_with_membership(
        seed,
        &stable_four_voter_membership(),
        leader,
        direct_election_config,
    )
}

fn cluster_with_membership(
    seed: SimSeed,
    membership: &MembershipConfig,
    fast_node: NodeId,
    node_config: fn(u64, &[u64], u64) -> NodeConfig,
) -> Cluster {
    let mut cluster = Cluster::new_with_seed(
        [NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
            .into_iter()
            .map(|node_id| {
                let timeout = if node_id == fast_node { 3 } else { 9 };
                let peers = [1, 2, 3, 4]
                    .into_iter()
                    .filter(|id| *id != node_id.0)
                    .collect::<Vec<_>>();
                node_config(node_id.0, &peers, timeout)
            })
            .collect(),
        seed,
    );

    for node_id in [NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
        let snapshot = RaftSnapshot::from_payload(snapshot_metadata(membership.clone()), b"");
        cluster.seed_snapshot_payload(node_id, &snapshot, Vec::new());
        cluster
            .restart_node_from_bootstrap(
                node_id,
                BootstrapState {
                    current_term: Term(1),
                    voted_for: None,
                    commit_index: LogIndex::ZERO,
                    committed_configuration: None,
                    snapshot: Some(snapshot),
                    log: Vec::new(),
                },
            )
            .expect("membership bootstrap is valid");
    }

    cluster
}

fn snapshot_metadata(membership: MembershipConfig) -> RaftSnapshotMetadata {
    snapshot_metadata_at(NodeId(1), LogIndex(1), Term(1), Term(1), membership)
}

fn snapshot_metadata_at(
    writer_id: NodeId,
    last_included_index: LogIndex,
    last_included_term: Term,
    hard_state_term: Term,
    membership: MembershipConfig,
) -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("sim-membership").expect("valid snapshot group id"),
        writer_id,
        last_included_index,
        last_included_term,
        hard_state_term,
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("membership").expect("valid snapshot kind"),
            ApplicationSnapshotVersion::new(1).expect("valid snapshot version"),
        ),
    )
    .expect("valid snapshot metadata")
    .with_committed_membership(membership)
}

fn restart_all_nodes_from_compacted_snapshots(cluster: &mut Cluster) {
    for node_id in [NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
        let live = cluster.bootstrap_state(node_id);
        let snapshot_index = live.commit_index;
        let snapshot_term = term_at(&live, snapshot_index);
        let snapshot_membership = cluster.effective_membership(node_id);
        let payload =
            format!("compacted dynamic membership for {node_id} through {snapshot_index}")
                .into_bytes();
        let snapshot = RaftSnapshot::from_payload(
            snapshot_metadata_at(
                node_id,
                snapshot_index,
                snapshot_term,
                live.current_term,
                snapshot_membership,
            ),
            &payload,
        );
        cluster.seed_snapshot_payload(node_id, &snapshot, payload);
        cluster
            .restart_node_from_bootstrap(
                node_id,
                BootstrapState {
                    current_term: live.current_term,
                    voted_for: live.voted_for,
                    commit_index: live.commit_index,
                    committed_configuration: live.committed_configuration,
                    snapshot: Some(snapshot),
                    log: live
                        .log
                        .into_iter()
                        .filter(|entry| entry.index > snapshot_index)
                        .collect(),
                },
            )
            .expect("compacted dynamic-membership bootstrap is valid");
    }
}

fn term_at(state: &BootstrapState, index: LogIndex) -> Term {
    if let Some(snapshot) = state.snapshot.as_ref() {
        if snapshot.metadata.last_included_index == index {
            return snapshot.metadata.last_included_term;
        }
    }
    state
        .log
        .iter()
        .find_map(|entry| (entry.index == index).then_some(entry.term))
        .expect("snapshot boundary must be present in the retained bootstrap state")
}

fn add_voter_joint_entry(config_id: ConfigurationId) -> ConfigurationEntry {
    ConfigurationEntry::joint(
        config_id,
        JointMembership::new(initial_learner_set(), stable_four_voter_set()),
    )
}

fn add_voter_final_entry(config_id: ConfigurationId) -> ConfigurationEntry {
    ConfigurationEntry::stable(config_id, stable_four_voter_set())
}

fn remove_node_two_joint_entry(config_id: ConfigurationId) -> ConfigurationEntry {
    ConfigurationEntry::joint(
        config_id,
        JointMembership::new(stable_four_voter_set(), remove_node_two_set()),
    )
}

fn remove_node_two_final_entry(config_id: ConfigurationId) -> ConfigurationEntry {
    ConfigurationEntry::stable(config_id, remove_node_two_set())
}

fn initial_learner_membership() -> MembershipConfig {
    MembershipConfig::stable(initial_learner_set())
}

fn stable_four_voter_membership() -> MembershipConfig {
    MembershipConfig::stable(stable_four_voter_set())
}

fn initial_learner_set() -> MembershipSet {
    membership_set(&[1, 2, 3], &[4])
}

fn stable_four_voter_set() -> MembershipSet {
    membership_set(&[1, 2, 3, 4], &[])
}

fn remove_node_two_set() -> MembershipSet {
    membership_set(&[1, 3, 4], &[])
}

fn membership_set(voters: &[u64], learners: &[u64]) -> MembershipSet {
    MembershipSet::new(
        voters.iter().copied().map(NodeId).collect(),
        learners.iter().copied().map(NodeId).collect(),
    )
    .expect("membership is valid")
}

fn applied_payloads(cluster: &Cluster, node_id: NodeId) -> Vec<rafter::SharedPayload> {
    cluster
        .applied()
        .iter()
        .filter_map(|applied| (applied.node_id == node_id).then_some(applied.payload.clone()))
        .collect()
}
