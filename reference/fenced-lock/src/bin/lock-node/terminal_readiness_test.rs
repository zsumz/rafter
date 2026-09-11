//! Terminal readiness is checked directly, without racing process shutdown.

use std::{sync::mpsc, time::Duration};

use rafter::NodeId;
use rafter_reference_fenced_lock::LockConfig;

use super::{
    handle_job, ClientReply, Config, Disposition, Job, OpenRequest, PeerLink, RecoveryMode,
    Replica, Request, State, TerminalFailure,
};
use rafter_reference_harness::process::ScratchSpace;

#[test]
fn failed_replica_status_overrides_a_caught_up_application() {
    for failure in [
        TerminalFailure::Unpersisted(String::from("injected persistence failure")),
        TerminalFailure::Contradicted(String::from("injected contradictory checkpoint")),
    ] {
        let scratch = ScratchSpace::create("lock-node", "terminal-readiness").unwrap();
        let config = Config {
            node_id: NodeId(1),
            members: vec![NodeId(1)],
            cluster_dir: scratch.path().to_path_buf(),
            client_listen: String::from("127.0.0.1:0"),
            peer_listen: String::from("127.0.0.1:0"),
            election_timeout_ticks: 2,
            tick_interval: Duration::from_millis(20),
            ownership_wait: Duration::from_secs(1),
            request_timeout: Duration::from_secs(1),
            lock: LockConfig::new(4, 4).unwrap(),
            recover: RecoveryMode::Open,
            control_plane_fault_after: None,
        };
        let link = PeerLink::bind(
            &config.peer_listen,
            config.node_id,
            &config.members,
            &config.cluster_dir,
        )
        .unwrap();
        let node_dir = config.node_dir();
        std::fs::create_dir_all(&node_dir).unwrap();
        let replica = Replica::open(OpenRequest {
            node_dir: &node_dir,
            node_id: config.node_id,
            members: &config.members,
            election_timeout_ticks: config.election_timeout_ticks,
            lock_config: config.lock,
            mode: config.recover,
            transport: link.transport(),
            validator: link.validator(),
            control_plane_fault_after: None,
        })
        .unwrap();
        assert!(replica.is_ready(), "the application floor is caught up");
        let mut state = State::Serving(Box::new(replica));
        assert!(status(&mut state, &config).starts_with("STATUS ready "));
        let State::Serving(replica) = state else {
            unreachable!("STATUS does not change the serving state")
        };
        let mut state = State::Failed { replica, failure };
        let response = status(&mut state, &config);
        link.shut_down();
        assert!(
            response.starts_with("STATUS abandoned "),
            "terminal process state must override a caught-up replica: {response}"
        );
    }
}

fn status(state: &mut State, config: &Config) -> String {
    let (response, _receiver) = mpsc::channel();
    let (_sender, flushed) = mpsc::channel();
    let job = Job {
        request: Request::Status,
        reply: ClientReply { response, flushed },
    };
    match handle_job(state, config, &job, 1) {
        Disposition::Answered(response) => response,
        other => panic!("STATUS must answer immediately: {other:?}"),
    }
}
