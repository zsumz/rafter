//! Append confirmation boundaries and the two constituent membership quorums.

use super::support::*;

pub(super) fn append_conflict() -> Scenario {
    let log = (1..=120)
        .map(|index| {
            BootstrapLogEntry::application(
                LogIndex(index),
                Term(1),
                format!("record {index}").into_bytes(),
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
            Step::inputs("A matching frame confirms only through 105",
                "The frame confirms 105, while the leader knows commitment through 110. Only the confirmed prefix may be committed here; the unrelated local suffix through 120 proves nothing further.", vec![append(100, 1,
                (101..=105).map(|index| LogEntry::application(Term(1), format!("record {index}").into_bytes())).collect(), 110)]),
            Step::inputs("A committed conflict is rejected",
                "Index 105 is already committed. A different entry there must be rejected without changing that committed history.", vec![append(104, 1,
                vec![LogEntry::application(Term(3), b"forbidden".to_vec())], 110)]),
            Step::inputs("An uncommitted conflict replaces the tail",
                "The conflict now starts at uncommitted index 106. Replace that suffix, then advance commitment only through the new frame's confirmed end.", vec![append(105, 1,
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
            Step::inputs("Election timeout starts pre-vote",
                "The pending joint configuration determines both constituent election quorums.", vec![Input::Tick; 10]),
            Step::inputs("Old majority alone cannot finish pre-vote",
                "Nodes 1, 2, 3 include all old voters but only new voter 3. A majority of the combined node set would incorrectly permit progress.", vec![pre_vote_granted(2, 3), pre_vote_granted(3, 3)]),
            Step::inputs("New majority completes pre-vote",
                "Node 4 joins node 3 to satisfy the new majority as well as the old one.", vec![pre_vote_granted(4, 3)]),
            Step::inputs("Old majority alone cannot elect",
                "Binding votes still require both majorities; pre-vote grants do not replace them.", vec![vote_granted(2, 3), vote_granted(3, 3)]),
            Step::inputs("New majority completes election",
                "The new majority permits leadership and its current-term no-op.", vec![vote_granted(4, 3)]),
            Step::replies("Old majority alone cannot commit",
                "Acknowledgements from nodes 2 and 3 plus the local log satisfy only the old replication majority.", vec![AppendReply::accepted(NodeId(2), LogIndex(2)), AppendReply::accepted(NodeId(3), LogIndex(2))]),
            Step::replies("New majority commits the configuration and no-op",
                "Node 4 supplies the missing new majority. The current-term no-op authorizes commitment and the earlier joint configuration is emitted first.", vec![AppendReply::accepted(NodeId(4), LogIndex(2))]),
        ],
        expected: (Term(3), Role::Leader, LogIndex(2), LogIndex(2), 0),
    }
}
