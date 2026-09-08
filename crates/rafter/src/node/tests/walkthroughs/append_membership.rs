//! Append confirmation boundaries and the two constituent membership quorums.

use super::support::*;

pub(super) fn append_conflict() -> Scenario {
    let log = (1..=120)
        .map(|index| {
            BootstrapLogEntry::application(
                LogIndex(index),
                Term(1),
                vec![u8::try_from(index).unwrap()],
            )
        })
        .collect();
    let append = |prev, prev_term, entries: Vec<LogEntry>, leader_commit| {
        message(
            2,
            Message::AppendEntries(AppendEntries {
                term: Term(3),
                leader_id: NodeId(2),
                sequence: 1,
                prev_log_index: LogIndex(prev),
                prev_log_term: Term(prev_term),
                entries: entries.into(),
                leader_commit: LogIndex(leader_commit),
            }),
        )
    };
    Scenario {
        name: "The frame confirms a prefix, not the whole local suffix",
        explanation: "Follow [follower append reception](src/node/replication/receive.rs). Five matching entries above 100 confirm only 105, even with leader_commit=110 and local last index=120. Using the local last index would dispatch unrelated suffix entries (CM-01, LG-04). A conflict at an already committed entry is rejected; a later conflict replaces only the uncommitted suffix. See [empty_append_never_commits_the_followers_unconfirmed_suffix and committed-conflict rejection](src/node/tests/replication/follower.rs).",
        node: bootstrap(1, 100, log),
        steps: vec![
            ("A matching frame confirms only through 105", vec![append(100, 1,
                (101..=105).map(|index| LogEntry::application(Term(1), vec![index])).collect(), 110)]),
            ("A committed conflict is rejected", vec![append(104, 1,
                vec![LogEntry::application(Term(3), b"forbidden".to_vec())], 110)]),
            ("An uncommitted conflict replaces the tail", vec![append(105, 1,
                vec![LogEntry::application(Term(3), b"replacement".to_vec())], 106)]),
        ],
        expected: (Term(3), Role::Follower, LogIndex(106), LogIndex(106), 0),
    }
}

pub(super) fn joint_quorum() -> Scenario {
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![]).unwrap();
    let new = MembershipSet::new(vec![NodeId(3), NodeId(4), NodeId(5)], vec![]).unwrap();
    let node = bootstrap(
        2,
        0,
        vec![BootstrapLogEntry::configuration(
            LogIndex(1),
            Term(2),
            ConfigurationEntry::joint(ConfigurationId(1), JointMembership::new(old, new)),
        )],
    );
    Scenario {
        name: "Joint membership requires both majorities",
        explanation: "Follow [election quorum](src/node/election.rs) and [commit quorum](src/node/commit/advance.rs). The old voters are 1,2,3; the new voters are 3,4,5. Grants or acknowledgements from 1,2,3 give an old majority and a majority of the union, but only one new vote. Node 4 supplies the missing new majority (MB-02). The [recovered joint-configuration tests](src/node/tests/bootstrap/configuration_progress.rs) check both election and commitment with the same rule.",
        node,
        steps: vec![
            ("Election timeout starts pre-vote", vec![Input::Tick; 10]),
            ("Old majority alone cannot finish pre-vote", vec![vote(2, 3, true), vote(3, 3, true)]),
            ("New majority completes pre-vote", vec![vote(4, 3, true)]),
            ("Old majority alone cannot elect", vec![vote(2, 3, false), vote(3, 3, false)]),
            ("New majority completes election", vec![vote(4, 3, false)]),
            ("Old majority alone cannot commit", vec![ack(2, 3, 2, 1, true), ack(3, 3, 2, 1, true)]),
            ("New majority commits the configuration and no-op", vec![ack(4, 3, 2, 1, true)]),
        ],
        expected: (Term(3), Role::Leader, LogIndex(2), LogIndex(2), 0),
    }
}
