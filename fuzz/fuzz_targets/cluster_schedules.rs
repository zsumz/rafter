//! Structured multi-node fuzzing over the deterministic simulator.
//!
//! The older `node_message_sequences` target drives one real kernel with
//! synthetic peer messages. This target drives a three-voter `Cluster`, so
//! every delivered message comes from a real `Node` and every restart
//! rehydrates through the normal bootstrap path.
//!
//! Fuzzed actions include ticking, proposing, delivering queued messages,
//! dropping queued messages, delaying one message so later messages can pass
//! it, and restarting nodes from durable state. Cross-node invariants are
//! checked after every action.

#![no_main]

use arbitrary::Unstructured;
use libfuzzer_sys::fuzz_target;
use rafter::{LogEntry, LogIndex, NodeConfig, NodeId};
use rafter_sim::{Cluster, Envelope, SimSeed};

const MAX_STEPS: usize = 128;
const NODE_IDS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

fn config(id: NodeId, peers: Vec<NodeId>, election_timeout_ticks: u64) -> NodeConfig {
    NodeConfig::new(id, peers, election_timeout_ticks).expect("static 3-voter config is valid")
}

fn cluster(seed: u64, minimal_posture: bool) -> Cluster {
    let mut configs = vec![
        config(NodeId(1), vec![NodeId(2), NodeId(3)], 3),
        config(NodeId(2), vec![NodeId(1), NodeId(3)], 5),
        config(NodeId(3), vec![NodeId(1), NodeId(2)], 7),
    ];
    if minimal_posture {
        configs = configs
            .into_iter()
            .map(|config| config.with_pre_vote(false).with_check_quorum(false))
            .collect();
    }
    Cluster::new_with_seed(configs, SimSeed(seed))
}

fn choose_node(selector: u8) -> NodeId {
    NODE_IDS[usize::from(selector) % NODE_IDS.len()]
}

fn tiny_payload(u: &mut Unstructured<'_>, step: usize, opcode: u8) -> Vec<u8> {
    let len = u.int_in_range(0..=8usize).unwrap_or(0);
    let mut payload = Vec::with_capacity(len + 2);
    payload.push(opcode);
    payload.push(u8::try_from(step % 256).expect("step modulo 256 fits in u8"));
    for _ in 0..len {
        payload.push(u.arbitrary().unwrap_or(0));
    }
    payload
}

fn nth_pending_predicate(target: usize) -> impl FnMut(&Envelope) -> bool {
    let mut seen = 0usize;
    move |_| {
        let matches = seen == target;
        seen += 1;
        matches
    }
}

fn pending_target(cluster: &Cluster, selector: u8) -> Option<usize> {
    let pending = cluster.pending().count();
    (pending > 0).then(|| usize::from(selector) % pending)
}

fn deliver_one(cluster: &mut Cluster, selector: u8) {
    let mut remaining = usize::from(selector % 8);
    let delivered = cluster.deliver_one_matching(|_| {
        if remaining == 0 {
            true
        } else {
            remaining -= 1;
            false
        }
    });
    if delivered {
        return;
    }

    cluster.advance_clock();
    let _ = cluster.deliver_one_matching(|_| true);
}

fn drop_one(cluster: &mut Cluster, selector: u8) {
    let Some(target) = pending_target(cluster, selector) else {
        return;
    };
    let dropped = cluster.drop_matching(nth_pending_predicate(target));
    assert!(
        dropped <= 1,
        "drop selector matched more than one queued message"
    );
}

fn delay_one_for_reorder(cluster: &mut Cluster, selector: u8) {
    let Some(target) = pending_target(cluster, selector) else {
        return;
    };
    let delayed = cluster.delay_matching(nth_pending_predicate(target), 1);
    assert!(
        delayed <= 1,
        "reorder selector matched more than one queued message"
    );
    if delayed == 0 {
        return;
    }

    let _ = cluster.deliver_one_matching(|_| true);
    cluster.advance_clock();
}

fn restart_clean(cluster: &mut Cluster, node_id: NodeId) {
    let bootstrap = cluster.bootstrap_state(node_id);
    cluster
        .restart_node_from_bootstrap(node_id, bootstrap)
        .expect("self-captured bootstrap state must be valid");
}

fn entry_at(cluster: &Cluster, node_id: NodeId, index: LogIndex) -> Option<LogEntry> {
    cluster.log_entries_from(node_id, index).first().cloned()
}

fn check_cluster_invariants(cluster: &Cluster) {
    for node_id in NODE_IDS {
        let commit_index = cluster.commit_index(node_id);
        let last_log_index = cluster.last_log_index(node_id);
        assert!(
            commit_index <= last_log_index,
            "node {node_id:?} commit_index {commit_index} > last_log_index {last_log_index}"
        );
    }

    for term in NODE_IDS.map(|node_id| cluster.current_term(node_id)) {
        let leaders = cluster.leaders_in_term(term);
        assert!(
            leaders.len() <= 1,
            "multiple leaders in term {term}: {leaders:?}"
        );
    }

    let max_commit = NODE_IDS
        .iter()
        .map(|node_id| cluster.commit_index(*node_id).0)
        .max()
        .unwrap_or(0);
    for raw_index in 1..=max_commit {
        let index = LogIndex(raw_index);
        let mut expected: Option<(NodeId, LogEntry)> = None;
        for node_id in NODE_IDS {
            if cluster.commit_index(node_id) < index {
                continue;
            }
            let Some(entry) = entry_at(cluster, node_id, index) else {
                panic!("node {node_id:?} committed {index} but no log entry is present");
            };
            if let Some((expected_node, expected_entry)) = &expected {
                assert_eq!(
                    &entry, expected_entry,
                    "committed entry mismatch at {index}: node {node_id:?} disagrees with \
                     node {expected_node:?}"
                );
            } else {
                expected = Some((node_id, entry));
            }
        }
    }
}

fn apply_action(cluster: &mut Cluster, u: &mut Unstructured<'_>, step: usize) {
    let opcode = u.arbitrary::<u8>().unwrap_or(0);
    let selector = u.arbitrary::<u8>().unwrap_or(0);
    match opcode % 12 {
        0..=2 => cluster.tick(choose_node(selector)),
        3..=4 => cluster.propose(choose_node(selector), tiny_payload(u, step, opcode)),
        5 => deliver_one(cluster, selector),
        6 => drop_one(cluster, selector),
        7 => delay_one_for_reorder(cluster, selector),
        8 => restart_clean(cluster, choose_node(selector)),
        9 => cluster.restart_node_lossy(choose_node(selector)),
        10 => {
            cluster.advance_clock();
            deliver_one(cluster, selector);
        }
        _ => {
            cluster.tick(NodeId(1));
            cluster.tick(NodeId(2));
            cluster.tick(NodeId(3));
        }
    }
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let seed = u.arbitrary::<u64>().unwrap_or(0x0052_4654_5f30_3238);
    let minimal_posture = u.arbitrary::<bool>().unwrap_or(false);
    let mut cluster = cluster(seed, minimal_posture);

    check_cluster_invariants(&cluster);
    for step in 0..MAX_STEPS {
        if u.is_empty() {
            break;
        }
        apply_action(&mut cluster, &mut u, step);
        check_cluster_invariants(&cluster);
    }
});
