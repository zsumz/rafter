//! Supported catch-up histories must pass the composed simulator oracle.

use super::*;

fn configuration(id: u64) -> ConfigurationEntry {
    ConfigurationEntry::stable(
        ConfigurationId(id),
        MembershipSet::new(ids(&[1, 2, 3]), (4..id + 3).map(NodeId).collect()).unwrap(),
    )
}

fn configuration_state(id: u64) -> CommittedConfiguration {
    CommittedConfiguration {
        index: LogIndex(id),
        config_id: ConfigurationId(id),
    }
}

fn initial_cluster() -> Cluster {
    let mut cluster = one_node_cluster();
    let mut bootstrap = bootstrap_state(Term(3), &[]);
    bootstrap.commit_index = LogIndex(1);
    bootstrap.committed_configuration = Some(configuration_state(1));
    bootstrap.log = vec![
        BootstrapLogEntry::configuration(LogIndex(1), Term(1), configuration(1)),
        BootstrapLogEntry::application(LogIndex(2), Term(1), b"stale-boundary".to_vec()),
        BootstrapLogEntry::application(LogIndex(3), Term(1), b"stale-tail".to_vec()),
    ];
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap)
        .unwrap();
    cluster
}

fn deliver(state: &mut ExplorationState, request: AppendEntries) {
    state.drop_all_messages();
    state.inject_message(NodeId(2), NodeId(1), Message::AppendEntries(request));
    apply_to_state(state, Operation::DeliverReadyAt(0));
}

fn catchup(term: Term, leader_commit: LogIndex, noop: bool) -> AppendEntries {
    let mut entries = vec![
        LogEntry::configuration(term, configuration(2)),
        LogEntry::configuration(term, configuration(3)),
    ];
    if noop {
        entries.push(LogEntry::noop(Term(4)));
    }
    AppendEntries {
        sequence: 1,
        term: Term(4),
        leader_id: NodeId(2),
        prev_log_index: LogIndex(1),
        prev_log_term: Term(1),
        entries: entries.into(),
        leader_commit,
    }
}

#[test]
fn configuration_crash_images_pass_commit_safety_even_for_current_term_entries() {
    for term in [Term(2), Term(4)] {
        let mut live = ExplorationState::new(initial_cluster());
        deliver(&mut live, catchup(term, LogIndex(3), false));
        assert_eq!(live.cluster().commit_index(NodeId(1)), LogIndex(3));
        check_commit_safety(&live, &[]).expect("live catch-up passes the composed oracle");

        let mut image = live.cluster().bootstrap_state(NodeId(1));
        // Log publication completed; hard state still knows only C1's commitment.
        image.commit_index = LogIndex(1);
        image.committed_configuration = Some(configuration_state(1));
        let mut recovered = one_node_cluster();
        recovered
            .restart_node_from_bootstrap(NodeId(1), image)
            .unwrap();
        let state = ExplorationState::new(recovered);
        assert_eq!(state.cluster().commit_index(NodeId(1)), LogIndex(1));
        assert_eq!(state.cluster().last_log_index(NodeId(1)), LogIndex(3));
        assert!(state.commit_history().configuration_proposals.is_empty());
        check_commit_safety(&state, &[])
            .expect("a pre-publication crash image is valid regardless of entry term");
    }
}

#[test]
fn historical_configuration_catchup_and_restart_pass_the_composed_oracle() {
    let mut state = ExplorationState::new(initial_cluster());
    deliver(&mut state, catchup(Term(2), LogIndex(1), true));
    assert_eq!(state.cluster().commit_index(NodeId(1)), LogIndex(1));
    assert_eq!(state.cluster().last_log_index(NodeId(1)), LogIndex(4));
    assert!(state.commit_history().configuration_proposals.is_empty());
    check_commit_safety(&state, &[]).expect("historical catch-up is not a new proposal");
    crate::model_check::state::restart_node(&mut state, NodeId(1), &[]).unwrap();
    check_commit_safety(&state, &[]).expect("the same history remains valid after restart");

    deliver(
        &mut state,
        AppendEntries {
            sequence: 2,
            term: Term(4),
            leader_id: NodeId(2),
            prev_log_index: LogIndex(4),
            prev_log_term: Term(4),
            entries: Vec::new().into(),
            leader_commit: LogIndex(4),
        },
    );
    assert_eq!(state.cluster().commit_index(NodeId(1)), LogIndex(4));
    assert_eq!(
        state.cluster().committed_configuration_state(NodeId(1)),
        Some(configuration_state(3))
    );
    check_commit_safety(&state, &[]).expect("later commitment preserves the oracle history");
}
