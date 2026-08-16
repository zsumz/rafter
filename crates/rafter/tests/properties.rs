//! Property suites for the protocol algebra: membership quorum
//! algebra, leader log batching, and bootstrap validation.
//!
//! # Seed reproduction
//!
//! A failing property prints the shrunken counterexample together with its
//! seed and persists the seed to `proptest-regressions/properties.txt` under
//! this crate's root (the file and directory are created on first failure).
//! The next `cargo test -p rafter --test properties` run replays every
//! persisted seed before generating fresh cases, so a red property reproduces
//! deterministically; committing the regression file pins the case forever.
//!
//! # Runtime budget
//!
//! Case counts are pinned explicitly so the whole file finishes in seconds in
//! the default test profile: 256 cases for the pure-algebra suites and 128
//! for the node-driving batching suite.
//!
//! # Max-index coverage
//!
//! Bootstrap generators include `u64::MAX - 1` and `u64::MAX` boundary cases.
//! Snapshot metadata must reject a maximum last-included index with a typed
//! error, and bootstrap must reject any contiguous log that reaches
//! `u64::MAX` with `BootstrapValidationError::LogIndexAtMaximum`.

use std::collections::BTreeSet;
use std::ops::RangeInclusive;

use proptest::prelude::*;
use proptest::test_runner::{FileFailurePersistence, TestCaseError};
use rafter::{
    AppendEntries, AppendEntriesResponse, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, BootstrapLogEntry, BootstrapState, BootstrapValidationError,
    ConfigurationEntry, ConfigurationId, Input, JointMembership, LogEntry, LogIndex,
    MembershipConfig, MembershipSet, Message, Node, NodeConfig, NodeId, Output, RaftSnapshot,
    RaftSnapshotMetadata, RequestVoteResponse, Role, SnapshotGroupId, SnapshotMetadataError, Term,
};
use rafter_invariant_test::{oracle_prop_assert, oracle_prop_assert_eq};

fn suite_config(cases: u32) -> ProptestConfig {
    ProptestConfig {
        cases,
        failure_persistence: Some(Box::new(FileFailurePersistence::Direct(
            "proptest-regressions/properties.txt",
        ))),
        ..ProptestConfig::default()
    }
}

// ---------------------------------------------------------------------------
// Shared membership strategies
// ---------------------------------------------------------------------------

/// Ids stay small so generated member sets collide and overlap often.
const MAX_NODE_ID: u64 = 15;

fn arb_id_set(sizes: RangeInclusive<usize>) -> impl Strategy<Value = BTreeSet<u64>> {
    proptest::collection::btree_set(0..=MAX_NODE_ID, sizes)
}

/// Valid stable membership sets: candidate voter/learner id sets are fed
/// through the validating constructor and anything it rejects is filtered
/// out, so the strategy can never invent a state the API refuses to build.
fn arb_membership_set() -> impl Strategy<Value = MembershipSet> {
    (arb_id_set(1..=7), arb_id_set(0..=7)).prop_filter_map(
        "MembershipSet::new rejected the candidate member sets",
        |(voters, learners)| {
            let learners = learners.difference(&voters).copied().map(NodeId).collect();
            MembershipSet::new(voters.into_iter().map(NodeId).collect(), learners).ok()
        },
    )
}

fn arb_membership_config() -> impl Strategy<Value = MembershipConfig> {
    prop_oneof![
        arb_membership_set().prop_map(MembershipConfig::stable),
        (arb_membership_set(), arb_membership_set())
            .prop_map(|(old, new)| MembershipConfig::joint(old, new)),
    ]
}

/// Acknowledgement lists with duplicates and non-members left in on purpose.
fn arb_acks() -> impl Strategy<Value = Vec<NodeId>> {
    proptest::collection::vec((0..=MAX_NODE_ID).prop_map(NodeId), 0..=20)
}

/// An independent majority recount: strictly more than half of the voter set
/// acknowledged, duplicates collapsed first.
fn majority_recount(set: &MembershipSet, acks: &[NodeId]) -> bool {
    let distinct: BTreeSet<NodeId> = acks.iter().copied().collect();
    let granted = set
        .voters()
        .iter()
        .filter(|voter| distinct.contains(voter))
        .count();
    granted * 2 > set.voters().len()
}

fn strictly_ascending(ids: &[NodeId]) -> bool {
    ids.windows(2).all(|pair| pair[0] < pair[1])
}

fn id_set(ids: &[NodeId]) -> BTreeSet<NodeId> {
    ids.iter().copied().collect()
}

// ---------------------------------------------------------------------------
// Suite 1: membership quorum algebra
// ---------------------------------------------------------------------------

proptest! {
    #![proptest_config(suite_config(256))]

    #[test]
    fn membership_stable_quorum_matches_a_direct_majority_recount(
        set in arb_membership_set(),
        acks in arb_acks(),
    ) {
        let expected = majority_recount(&set, &acks);
        oracle_prop_assert_eq!(set.has_quorum(acks.iter().copied()), expected);
        oracle_prop_assert_eq!(
            MembershipConfig::stable(set.clone()).has_quorum(acks.iter().copied()),
            expected
        );
    }

    #[test]
    fn membership_joint_quorum_holds_iff_both_half_majorities_hold(
        old in arb_membership_set(),
        new in arb_membership_set(),
        acks in arb_acks(),
    ) {
        let joint = JointMembership::new(old.clone(), new.clone());
        let config = MembershipConfig::joint(old.clone(), new.clone());
        let both_halves = majority_recount(&old, &acks) && majority_recount(&new, &acks);

        oracle_prop_assert_eq!(joint.has_quorum(acks.iter().copied()), both_halves);
        oracle_prop_assert_eq!(config.has_quorum(acks.iter().copied()), both_halves);

        // A joint configuration never grants a quorum either half alone
        // would reject.
        if config.has_quorum(acks.iter().copied()) {
            oracle_prop_assert!(old.has_quorum(acks.iter().copied()));
            oracle_prop_assert!(new.has_quorum(acks.iter().copied()));
        }
    }

    #[test]
    fn membership_quorum_is_monotone_under_acknowledgement_supersets(
        config in arb_membership_config(),
        acks in arb_acks(),
        extra in arb_acks(),
    ) {
        let superset: Vec<NodeId> = acks.iter().copied().chain(extra).collect();
        if config.has_quorum(acks.iter().copied()) {
            prop_assert!(
                config.has_quorum(superset.iter().copied()),
                "superset {:?} lost a quorum its subset {:?} held",
                superset,
                acks
            );
        }
    }

    #[test]
    fn membership_duplicate_acknowledgements_never_change_the_decision(
        config in arb_membership_config(),
        acks in arb_acks(),
    ) {
        let doubled: Vec<NodeId> = acks.iter().chain(acks.iter()).copied().collect();
        prop_assert_eq!(
            config.has_quorum(doubled),
            config.has_quorum(acks.iter().copied())
        );
    }

    #[test]
    fn membership_stable_id_accessors_agree_with_the_member_sets(set in arb_membership_set()) {
        let config = MembershipConfig::stable(set.clone());
        let voters = id_set(set.voters());
        let learners = id_set(set.learners());

        // The constructor enforced voter/learner disjointness within the set.
        prop_assert!(voters.is_disjoint(&learners));

        let replicas = set.replica_ids();
        prop_assert!(strictly_ascending(&replicas));
        prop_assert_eq!(
            id_set(&replicas),
            voters.union(&learners).copied().collect::<BTreeSet<_>>()
        );

        prop_assert_eq!(config.voter_ids(), set.voters().to_vec());
        prop_assert_eq!(config.replica_ids(), replicas);
        for id in (0..=MAX_NODE_ID).map(NodeId) {
            prop_assert_eq!(config.contains_voter(id), voters.contains(&id));
            prop_assert_eq!(config.contains_learner(id), learners.contains(&id));
        }
        prop_assert_eq!(set.quorum_size(), set.voters().len() / 2 + 1);
    }

    #[test]
    fn membership_joint_id_accessors_are_the_half_set_unions(
        old in arb_membership_set(),
        new in arb_membership_set(),
    ) {
        let config = MembershipConfig::joint(old.clone(), new.clone());
        let voter_union: BTreeSet<NodeId> =
            old.voters().iter().chain(new.voters()).copied().collect();
        let learner_union: BTreeSet<NodeId> =
            old.learners().iter().chain(new.learners()).copied().collect();

        let voter_ids = config.voter_ids();
        prop_assert!(strictly_ascending(&voter_ids));
        prop_assert_eq!(&id_set(&voter_ids), &voter_union);

        let replica_ids = config.replica_ids();
        prop_assert!(strictly_ascending(&replica_ids));
        prop_assert_eq!(
            id_set(&replica_ids),
            voter_union.union(&learner_union).copied().collect::<BTreeSet<_>>()
        );

        // Pinned constructor behavior: the halves are validated
        // independently, so one id may be a voter in one half and a learner
        // in the other — both accessors answer from the half-set unions.
        for id in (0..=MAX_NODE_ID).map(NodeId) {
            prop_assert_eq!(config.contains_voter(id), voter_union.contains(&id));
            prop_assert_eq!(config.contains_learner(id), learner_union.contains(&id));
        }
    }

    #[test]
    fn membership_constructor_validation_matches_the_documented_invariant(
        voters in proptest::collection::vec(0..=MAX_NODE_ID, 0..=7),
        learners in proptest::collection::vec(0..=MAX_NODE_ID, 0..=7),
    ) {
        let result = MembershipSet::new(
            voters.iter().copied().map(NodeId).collect(),
            learners.iter().copied().map(NodeId).collect(),
        );

        let unique_voters: BTreeSet<u64> = voters.iter().copied().collect();
        let unique_learners: BTreeSet<u64> = learners.iter().copied().collect();
        let valid = !voters.is_empty()
            && unique_voters.len() == voters.len()
            && unique_learners.len() == learners.len()
            && unique_voters.is_disjoint(&unique_learners);

        oracle_prop_assert_eq!(
            result.is_ok(),
            valid,
            "validation disagrees for voters {:?} and learners {:?}: {:?}",
            voters,
            learners,
            result
        );
        if let Ok(set) = result {
            // Validation and accessors agree: sorted protocol order over
            // exactly the input id sets.
            oracle_prop_assert!(strictly_ascending(set.voters()));
            oracle_prop_assert!(strictly_ascending(set.learners()));
            oracle_prop_assert_eq!(
                set.voters().iter().map(|id| id.0).collect::<BTreeSet<_>>(),
                unique_voters
            );
            oracle_prop_assert_eq!(
                set.learners().iter().map(|id| id.0).collect::<BTreeSet<_>>(),
                unique_learners
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Suite 2: leader log batching through the public window-fill path
// ---------------------------------------------------------------------------

const FOLLOWER: NodeId = NodeId(2);

#[derive(Clone, Debug)]
enum EntrySpec {
    Application { payload_len: usize },
    Configuration(ConfigurationEntry),
}

impl EntrySpec {
    fn log_entry(&self) -> LogEntry {
        match self {
            Self::Application { payload_len } => {
                LogEntry::application(Term(1), vec![0xAB; *payload_len])
            }
            Self::Configuration(entry) => LogEntry::configuration(Term(1), entry.clone()),
        }
    }

    fn bootstrap_entry(&self, index: u64) -> BootstrapLogEntry {
        match self {
            Self::Application { payload_len } => {
                BootstrapLogEntry::application(LogIndex(index), Term(1), vec![0xAB; *payload_len])
            }
            Self::Configuration(entry) => {
                BootstrapLogEntry::configuration(LogIndex(index), Term(1), entry.clone())
            }
        }
    }
}

/// Configuration entries whose voters stay exactly the harness voters
/// {1, 2, 3}: a bootstrapped configuration entry is the EFFECTIVE membership
/// even while uncommitted, so arbitrary voter sets would change who can
/// campaign and what a quorum is. Learner sets (and the joint halves) stay
/// arbitrary — they are what varies the entry's replication size.
fn arb_inert_configuration_entry() -> impl Strategy<Value = ConfigurationEntry> {
    let harness_membership = |learners: BTreeSet<u64>| {
        MembershipSet::new(
            vec![NodeId(1), NodeId(2), NodeId(3)],
            learners
                .into_iter()
                .filter(|id| !STATIC_VOTERS.contains(id))
                .map(NodeId)
                .collect(),
        )
        .expect("harness voters with disjoint learners are valid")
    };
    prop_oneof![
        (0..=99u64, arb_id_set(0..=7)).prop_map(move |(id, learners)| {
            ConfigurationEntry::stable(ConfigurationId(id), harness_membership(learners))
        }),
        (0..=99u64, arb_id_set(0..=7), arb_id_set(0..=7)).prop_map(
            move |(id, old_learners, new_learners)| {
                ConfigurationEntry::joint(
                    ConfigurationId(id),
                    JointMembership::new(
                        harness_membership(old_learners),
                        harness_membership(new_learners),
                    ),
                )
            }
        ),
    ]
}

/// Logs of 1..=20 application entries with payloads from empty to
/// budget-dwarfing, plus at most one configuration entry spliced in at an
/// arbitrary position — bootstrap validation admits at most one uncommitted
/// configuration entry, and every bootstrapped entry starts uncommitted.
fn arb_entry_specs() -> impl Strategy<Value = Vec<EntrySpec>> {
    (
        proptest::collection::vec(0..=2048usize, 1..=20),
        proptest::option::of((
            any::<proptest::sample::Index>(),
            arb_inert_configuration_entry(),
        )),
    )
        .prop_map(|(payload_lens, configuration)| {
            let mut specs: Vec<EntrySpec> = payload_lens
                .into_iter()
                .map(|payload_len| EntrySpec::Application { payload_len })
                .collect();
            if let Some((position, entry)) = configuration {
                let position = position.index(specs.len() + 1);
                specs.insert(position, EntrySpec::Configuration(entry));
            }
            specs
        })
}

fn follower_append_response(node: &Node, success: bool) -> Input {
    Input::Message {
        from: FOLLOWER,
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: node.current_term(),
            follower_id: FOLLOWER,
            success,
            match_index: LogIndex::ZERO,
            sequence: 0,
        }),
    }
}

/// Bootstraps a term-1 log, elects the node directly (minimal-protocol
/// posture), and includes the term-2 leadership no-op that election appends.
/// It then walks follower 2's probe cursor back to index one with rejections
/// (the kernel walks one index per rejection round trip) before confirming
/// Replicate mode with a success acknowledgement at match zero. That single
/// acknowledgement step fills the in-flight window; the window bounds are
/// opened wide, so the returned frames are exactly the kernel's batching of
/// the full pending suffix under `budget`.
fn window_fill_frames(specs: &[EntrySpec], budget: usize) -> Vec<AppendEntries> {
    let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
        .expect("static test config is valid")
        .with_pre_vote(false)
        .with_check_quorum(false)
        .with_max_append_entries_bytes(budget)
        .with_max_inflight_appends(usize::MAX)
        .with_max_inflight_bytes(usize::MAX);
    let log = specs
        .iter()
        .enumerate()
        .map(|(offset, spec)| spec.bootstrap_entry(offset as u64 + 1))
        .collect();
    let mut node = Node::from_bootstrap(
        config,
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log,
        },
    )
    .expect("generated bootstrap log is valid");

    for _ in 0..3 {
        let _ = node.step(Input::Tick);
    }
    assert_eq!(
        node.role(),
        Role::Candidate,
        "direct election campaigns on the third tick"
    );
    let term = node.current_term();
    let _ = node.step(Input::Message {
        from: FOLLOWER,
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term,
            voter_id: FOLLOWER,
            vote_granted: true,
        }),
    });
    assert_eq!(
        node.role(),
        Role::Leader,
        "one grant reaches a three-voter quorum"
    );

    for _ in 0..=specs.len() {
        let rejection = follower_append_response(&node, false);
        let _ = node.step(rejection);
    }
    let probe = node
        .leader_replication_progress()
        .into_iter()
        .find(|progress| progress.follower_id == FOLLOWER)
        .expect("the leader tracks follower 2");
    assert_eq!(
        probe.next_index,
        LogIndex(1),
        "rejections walked the probe cursor back to the log start"
    );

    let acknowledgement = follower_append_response(&node, true);
    node.step(acknowledgement)
        .into_iter()
        .filter_map(|output| match output {
            Output::Send {
                to,
                message: Message::AppendEntries(request),
            } if to == FOLLOWER => Some(request),
            _ => None,
        })
        .collect()
}

fn frame_replication_bytes(frame: &AppendEntries) -> usize {
    frame.entries.iter().map(LogEntry::replication_bytes).sum()
}

proptest! {
    #![proptest_config(suite_config(128))]

    #[test]
    fn batching_window_fill_partitions_the_pending_suffix(
        specs in arb_entry_specs(),
        budget in 64..=4096usize,
    ) {
        let mut expected: Vec<LogEntry> = specs.iter().map(EntrySpec::log_entry).collect();
        expected.push(LogEntry::noop(Term(2)));
        let frames = window_fill_frames(&specs, budget);

        prop_assert!(!frames.is_empty(), "a non-empty suffix produces frames");
        let mut replayed: Vec<LogEntry> = Vec::new();
        for frame in &frames {
            // Min-one-entry: a frame goes out even when its first entry
            // alone exceeds the budget.
            prop_assert!(!frame.entries.is_empty(), "the fill never sends an empty frame");
            // Each frame starts exactly where the previous one ended, and
            // its prev term names the boundary entry.
            prop_assert_eq!(frame.prev_log_index, LogIndex(replayed.len() as u64));
            let boundary_term = if replayed.is_empty() { Term(0) } else { Term(1) };
            prop_assert_eq!(frame.prev_log_term, boundary_term);
            // Nothing was committed: the current-term no-op is still only
            // locally appended until a follower acknowledges it.
            prop_assert_eq!(frame.leader_commit, LogIndex::ZERO);
            replayed.extend(frame.entries.iter().cloned());
        }
        // No gaps, no overlaps, no reordering, nothing invented: the frames
        // concatenate back into exactly the pending suffix.
        prop_assert_eq!(&replayed, &expected);
    }

    #[test]
    fn batching_window_fill_pins_the_budget_rule(
        specs in arb_entry_specs(),
        budget in 64..=4096usize,
    ) {
        let frames = window_fill_frames(&specs, budget);

        // The rule pinned from log_entries_from_bounded: the budget bounds
        // the batch beyond its first entry. A frame holding two or more
        // entries therefore fits the budget whole; only a single-entry frame
        // may exceed it.
        for frame in &frames {
            if frame.entries.len() >= 2 {
                prop_assert!(
                    frame_replication_bytes(frame) <= budget,
                    "a {}-entry frame of {} replication bytes exceeds the {}-byte budget",
                    frame.entries.len(),
                    frame_replication_bytes(frame),
                    budget
                );
            }
        }
        // Greedy maximality: a frame boundary exists only because the next
        // frame's first entry would have pushed the batch past the budget.
        for pair in frames.windows(2) {
            let filled = frame_replication_bytes(&pair[0]);
            let next_first = pair[1].entries[0].replication_bytes();
            prop_assert!(
                filled + next_first > budget,
                "the fill split at {} bytes although the next entry of {} bytes fit the {}-byte budget",
                filled,
                next_first,
                budget
            );
        }
    }

    #[test]
    fn batching_oversized_entries_always_travel_alone(
        specs in arb_entry_specs(),
        budget in 64..=2048usize,
    ) {
        // Force at least one oversized entry: a payload of `budget` bytes
        // costs budget-plus-overhead replication bytes.
        let mut specs = specs;
        specs.push(EntrySpec::Application { payload_len: budget });
        let frames = window_fill_frames(&specs, budget);

        let mut oversized_frames = 0u32;
        for frame in &frames {
            if frame
                .entries
                .iter()
                .any(|entry| entry.replication_bytes() > budget)
            {
                prop_assert_eq!(
                    frame.entries.len(),
                    1,
                    "an entry above the budget must be the only entry of its frame"
                );
                oversized_frames += 1;
            }
        }
        prop_assert!(oversized_frames >= 1, "the forced oversized entry was replicated");
    }
}

// ---------------------------------------------------------------------------
// Suite 3: bootstrap validation over arbitrary persisted shapes
// ---------------------------------------------------------------------------

/// The static bootstrap-suite configuration: voters {1, 2, 3}.
const STATIC_VOTERS: [u64; 3] = [1, 2, 3];

fn bootstrap_node_config() -> NodeConfig {
    NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3).expect("static test config is valid")
}

fn arb_snapshot_index() -> impl Strategy<Value = u64> {
    prop_oneof![8 => 1..=8u64, 1 => Just(u64::MAX - 1), 1 => Just(u64::MAX)]
}

fn arb_log_index() -> impl Strategy<Value = u64> {
    prop_oneof![
        16 => 0..=10u64,
        1 => Just(u64::MAX - 1),
        1 => Just(u64::MAX),
    ]
}

fn arb_applied_floor() -> impl Strategy<Value = u64> {
    prop_oneof![
        16 => 0..=12u64,
        1 => Just(u64::MAX - 1),
        1 => Just(u64::MAX),
    ]
}

/// Snapshot descriptors with arbitrary boundaries, terms, writers, payload
/// lengths, and optional committed memberships, generated through the
/// validating metadata constructor (candidates it rejects are filtered out).
fn arb_snapshot() -> impl Strategy<Value = RaftSnapshot> {
    (
        arb_snapshot_index(), // last_included_index
        0..=6u64,             // last_included_term (zero is constructor-rejected)
        0..=6u64,             // hard_state_term
        0..=6u64,             // writer id: 1..=3 are static voters, the rest are not
        0..=1024u64,          // application payload length
        proptest::option::of(arb_membership_config()),
    )
        .prop_filter_map(
            "RaftSnapshotMetadata::new rejected the candidate descriptor",
            |(index, term, hard_state_term, writer, payload_len, membership)| {
                let metadata = RaftSnapshotMetadata::new(
                    SnapshotGroupId::new("prop-group").expect("valid group id"),
                    NodeId(writer),
                    LogIndex(index),
                    Term(term),
                    Term(hard_state_term),
                    ApplicationSnapshotMetadata::new(
                        ApplicationSnapshotKind::new("prop-kind").expect("valid kind"),
                        ApplicationSnapshotVersion::new(1).expect("valid version"),
                    ),
                )
                .ok()?;
                let metadata = match membership {
                    Some(membership) => metadata.with_committed_membership(membership),
                    None => metadata,
                };
                Some(RaftSnapshot::new(metadata, payload_len, 0))
            },
        )
}

/// One persisted log entry candidate before materialization.
#[derive(Clone, Debug)]
struct RawLogEntry {
    index: u64,
    term: u64,
    configuration: bool,
}

fn arb_entry_term() -> impl Strategy<Value = u64> {
    // Mostly plausible terms, with zero terms and terms ahead of every
    // generated current term mixed in.
    prop_oneof![8 => 1..=6u64, 1 => Just(0u64), 2 => 5..=8u64]
}

fn arb_is_configuration() -> impl Strategy<Value = bool> {
    prop_oneof![4 => Just(false), 1 => Just(true)]
}

/// Mostly-contiguous logs anchored around the snapshot boundary: the first
/// index lands in `boundary - 1 ..= boundary + 2` and indexes usually step
/// by one (occasionally two, leaving a hole). Boundary sentinels, straddles,
/// compacted prefixes, holes, and repeated configuration entries all occur.
fn arb_structured_log(boundary: u64) -> impl Strategy<Value = Vec<RawLogEntry>> {
    (
        -1i64..=2,
        proptest::collection::vec(
            (
                arb_entry_term(),
                arb_is_configuration(),
                prop_oneof![9 => Just(1u64), 1 => Just(2u64)],
            ),
            0..=6,
        ),
    )
        .prop_map(move |(start_offset, entries)| {
            let mut index = boundary.saturating_add_signed(start_offset);
            let mut log = Vec::new();
            for (term, configuration, step) in entries {
                log.push(RawLogEntry {
                    index,
                    term,
                    configuration,
                });
                index = index.saturating_add(step);
            }
            log
        })
}

/// Fully arbitrary index/term shapes, untethered from the boundary.
fn arb_chaotic_log() -> impl Strategy<Value = Vec<RawLogEntry>> {
    proptest::collection::vec(
        (arb_log_index(), 0..=8u64, arb_is_configuration()).prop_map(
            |(index, term, configuration)| RawLogEntry {
                index,
                term,
                configuration,
            },
        ),
        0..=6,
    )
}

fn materialize_entry(raw: &RawLogEntry, offset: usize) -> BootstrapLogEntry {
    if raw.configuration {
        BootstrapLogEntry::configuration(
            LogIndex(raw.index),
            Term(raw.term),
            ConfigurationEntry::stable(
                ConfigurationId(offset as u64),
                MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
                    .expect("static membership is valid"),
            ),
        )
    } else {
        BootstrapLogEntry::application(LogIndex(raw.index), Term(raw.term), vec![0x5A; offset % 5])
    }
}

fn arb_bootstrap_state() -> impl Strategy<Value = BootstrapState> {
    (
        0..=6u64,
        proptest::option::of(0..=5u64),
        proptest::option::of(arb_snapshot()),
    )
        .prop_flat_map(|(current_term, voted_for, snapshot)| {
            let boundary = snapshot
                .as_ref()
                .map_or(0, |snapshot| snapshot.metadata.last_included_index.0);
            (
                Just(current_term),
                Just(voted_for),
                Just(snapshot),
                prop_oneof![3 => arb_structured_log(boundary), 1 => arb_chaotic_log()],
            )
        })
        .prop_map(
            |(current_term, voted_for, snapshot, raw_log)| BootstrapState {
                current_term: Term(current_term),
                voted_for: voted_for.map(NodeId),
                commit_index: LogIndex::ZERO,
                committed_configuration: None,
                snapshot,
                log: raw_log
                    .iter()
                    .enumerate()
                    .map(|(offset, raw)| materialize_entry(raw, offset))
                    .collect(),
            },
        )
}

fn boundary_of(state: &BootstrapState) -> u64 {
    state
        .snapshot
        .as_ref()
        .map_or(0, |snapshot| snapshot.metadata.last_included_index.0)
}

/// The (index, term) pairs the kernel would materialize above the boundary:
/// every log entry except boundary sentinels.
fn materialized_entries(state: &BootstrapState) -> Vec<(u64, u64)> {
    let boundary = boundary_of(state);
    state
        .log
        .iter()
        .filter(|entry| state.snapshot.is_none() || entry.index.0 != boundary)
        .map(|entry| (entry.index.0, entry.term.0))
        .collect()
}

/// An independent restatement of the documented acceptance rules: a vote
/// requires a nonzero term; a snapshot's hard-state term stays within the
/// current term and its writer is a replica — voter or learner — of the
/// snapshot's committed membership when it carries one, of the static
/// membership otherwise; the log is contiguous from the snapshot boundary with
/// nonzero terms no newer than the current term; boundary sentinel entries match the
/// snapshot term; nothing sits below the boundary; the commit index never lies
/// beyond the log; and at most one uncommitted configuration entry exists
/// above the recovered commit index.
fn accepted_by_documented_rules(state: &BootstrapState) -> bool {
    if state.voted_for.is_some() && state.current_term.0 == 0 {
        return false;
    }

    let mut boundary = 0u64;
    let mut boundary_term = 0u64;
    if let Some(snapshot) = &state.snapshot {
        let metadata = &snapshot.metadata;
        if metadata.hard_state_term > state.current_term {
            return false;
        }
        // Spelled as voter-or-learner rather than through `contains_replica`,
        // so this oracle does not restate the rule by calling the one predicate
        // the implementation calls. The static half needs no learner arm: this
        // suite's static membership (`bootstrap_node_config`) has none.
        let writer_is_replica = match metadata.committed_membership() {
            Some(membership) => {
                membership.contains_voter(metadata.writer_id)
                    || membership.contains_learner(metadata.writer_id)
            }
            None => STATIC_VOTERS.contains(&metadata.writer_id.0),
        };
        if !writer_is_replica {
            return false;
        }
        boundary = metadata.last_included_index.0;
        boundary_term = metadata.last_included_term.0;
    }

    let commit_index = state.commit_index.0.max(boundary);
    let Some(mut expected) = boundary.checked_add(1) else {
        return false;
    };
    let mut configuration_entries = 0usize;
    let mut last_log_index = boundary;
    for entry in &state.log {
        if entry.index.0 < boundary {
            return false; // compacted below the boundary
        }
        if state.snapshot.is_some() && entry.index.0 == boundary {
            if entry.term.0 != boundary_term {
                return false; // boundary sentinel term mismatch
            }
            continue;
        }
        if entry.index.0 != expected {
            return false; // hole or duplicate: not contiguous
        }
        if entry.term.0 == 0 || entry.term > state.current_term {
            return false; // term floor and ceiling
        }
        let Some(next_expected) = entry.index.0.checked_add(1) else {
            return false; // reaching the maximum index leaves no legal successor
        };
        expected = next_expected;
        last_log_index = entry.index.0;
        if entry.kind.is_configuration() && entry.index.0 > commit_index {
            configuration_entries += 1;
            if configuration_entries > 1 {
                return false; // more than one uncommitted configuration
            }
        }
    }
    commit_index <= last_log_index
}

/// On acceptance, the node's shape agrees with the input: boundary, commit
/// index, hard state, log end, and per-index terms.
fn assert_hydrated_shape(node: &Node, state: &BootstrapState) -> Result<(), TestCaseError> {
    let boundary = boundary_of(state);
    let materialized = materialized_entries(state);
    prop_assert_eq!(node.snapshot_index(), LogIndex(boundary));
    prop_assert_eq!(
        node.commit_index(),
        LogIndex(state.commit_index.0.max(boundary))
    );
    prop_assert_eq!(node.current_term(), state.current_term);
    prop_assert_eq!(node.voted_for(), state.voted_for);
    let last_hydrated_index = materialized
        .last()
        .map_or(boundary, |(index, _term)| *index);
    prop_assert_eq!(node.last_log_index(), LogIndex(last_hydrated_index));
    if let Some(snapshot) = &state.snapshot {
        prop_assert_eq!(
            node.term_at_index(LogIndex(boundary)),
            Some(snapshot.metadata.last_included_term)
        );
    }
    for (index, term) in materialized {
        prop_assert_eq!(node.term_at_index(LogIndex(index)), Some(Term(term)));
    }
    Ok(())
}

proptest! {
    #![proptest_config(suite_config(256))]

    #[test]
    fn snapshot_metadata_boundary_candidates_return_typed_results(
        last_included_index in prop_oneof![8 => 0..=8u64, 1 => Just(u64::MAX - 1), 1 => Just(u64::MAX)],
    ) {
        let result = RaftSnapshotMetadata::new(
            SnapshotGroupId::new("prop-group").expect("valid group id"),
            NodeId(1),
            LogIndex(last_included_index),
            Term(1),
            Term(1),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("prop-kind").expect("valid kind"),
                ApplicationSnapshotVersion::new(1).expect("valid version"),
            ),
        );

        match last_included_index {
            0 => prop_assert_eq!(
                result.expect_err("zero snapshot boundary is invalid"),
                SnapshotMetadataError::ZeroLastIncludedIndex
            ),
            u64::MAX => prop_assert_eq!(
                result.expect_err("maximum snapshot boundary is invalid"),
                SnapshotMetadataError::LastIncludedIndexAtMaximum
            ),
            _ => prop_assert!(
                result.is_ok(),
                "nonzero, non-maximum snapshot boundary should be accepted"
            ),
        }
    }

    #[test]
    fn bootstrap_rejects_contiguous_log_at_maximum_index_with_typed_error(
        term in 1..=6u64,
    ) {
        let metadata = RaftSnapshotMetadata::new(
            SnapshotGroupId::new("prop-group").expect("valid group id"),
            NodeId(1),
            LogIndex(u64::MAX - 1),
            Term(term),
            Term(term),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("prop-kind").expect("valid kind"),
                ApplicationSnapshotVersion::new(1).expect("valid version"),
            ),
        )
        .expect("maximum minus one snapshot boundary is valid");
        let state = BootstrapState {
            current_term: Term(term),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(RaftSnapshot::new(metadata, 0, 0)),
            log: vec![BootstrapLogEntry::application(
                LogIndex(u64::MAX),
                Term(term),
                vec![0x5A],
            )],
        };

        prop_assert_eq!(
            Node::from_bootstrap(bootstrap_node_config(), state),
            Err(BootstrapValidationError::LogIndexAtMaximum {
                index: LogIndex(u64::MAX),
            })
        );
    }

    #[test]
    fn bootstrap_accepts_exactly_the_documented_shapes(state in arb_bootstrap_state()) {
        let accepted = accepted_by_documented_rules(&state);
        // Calling the constructor is itself the never-panics property: a
        // panic fails this case and persists its seed.
        match Node::from_bootstrap(bootstrap_node_config(), state.clone()) {
            Ok(node) => {
                prop_assert!(
                    accepted,
                    "the kernel accepted a shape the documented rules reject: {:?}",
                    state
                );
                assert_hydrated_shape(&node, &state)?;
            }
            Err(error) => {
                prop_assert!(
                    !accepted,
                    "the kernel rejected a shape the documented rules accept with {}: {:?}",
                    error,
                    state
                );
            }
        }
    }

    #[test]
    fn bootstrap_applied_floor_is_validated_against_the_log_end_and_commit_index(
        state in arb_bootstrap_state(),
        applied_through in arb_applied_floor(),
    ) {
        let accepted = accepted_by_documented_rules(&state);
        let result = Node::from_bootstrap_applied_through(
            bootstrap_node_config(),
            state.clone(),
            LogIndex(applied_through),
        );
        if accepted {
            let boundary = boundary_of(&state);
            let last_log_index = materialized_entries(&state)
                .last()
                .map_or(boundary, |(index, _term)| *index);
            let commit_index = state.commit_index.0.max(boundary);
            if applied_through > last_log_index {
                prop_assert_eq!(
                    result,
                    Err(BootstrapValidationError::AppliedFloorBeyondLog {
                        applied_through: LogIndex(applied_through),
                        last_log_index: LogIndex(last_log_index),
                    })
                );
            } else if applied_through > commit_index {
                prop_assert_eq!(
                    result,
                    Err(BootstrapValidationError::AppliedFloorBeyondCommit {
                        applied_through: LogIndex(applied_through),
                        commit_index: LogIndex(commit_index),
                    })
                );
            } else {
                match result {
                    Ok(node) => assert_hydrated_shape(&node, &state)?,
                    Err(error) => prop_assert!(
                        false,
                        "an in-range applied floor {} was rejected with {}: {:?}",
                        applied_through,
                        error,
                        state
                    ),
                }
            }
        } else {
            prop_assert!(
                result.is_err(),
                "an invalid base shape must stay invalid under an applied floor: {:?}",
                state
            );
        }
    }
}
