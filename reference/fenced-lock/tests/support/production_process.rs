//! Black-box orchestration for the authenticated production-composition fixture.
//!
//! This harness knows the public command line, line protocol, JSON operations
//! surface, and durable caller-owned identity paths. It does not import the
//! process binary or inspect an in-process replica.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command as OsCommand,
    time::Duration,
};

use rafter::{LogIndex, NodeId};
use rafter_reference_fenced_lock::{
    production::{allocate_replica, AllocationCrashPoint, ReplicaIdentity, TransportReplayStore},
    Command, HistoryEvent, LockConfig, OperationId, ResourceName,
};
use rafter_reference_harness::process::{
    ChildProcess, ConnectionTimeouts, ReconnectingClient, Wait,
};

use crate::{
    process::{
        parse_query_response, parse_submit_response, process_history, render_command, QueryOutcome,
        SubmitOutcome,
    },
    scratch::ScratchDir,
};

pub const NODE_IDS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];
const GROUP_ID: u64 = 1;
const POLL_INTERVAL: Duration = Duration::from_millis(10);
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);
const WAIT: Wait = Wait::new(WAIT_TIMEOUT, POLL_INTERVAL);
const CONNECTION_TIMEOUTS: ConnectionTimeouts = ConnectionTimeouts::new(
    Duration::from_secs(5),
    Duration::from_secs(20),
    Duration::from_secs(5),
);

/// One flat JSON operations snapshot decoded independently from the producer.
#[derive(Clone, Debug)]
pub struct Observation {
    raw: String,
}

impl Observation {
    pub fn new(raw: String) -> Self {
        assert!(
            raw.starts_with('{') && raw.ends_with('}'),
            "OBSERVE returns one JSON object, observed {raw:?}"
        );
        Self { raw }
    }

    pub fn boolean(&self, name: &str) -> bool {
        match self.value(name) {
            "true" => true,
            "false" => false,
            value => panic!("{name} is not a JSON boolean in {:?}: {value:?}", self.raw),
        }
    }

    pub fn number(&self, name: &str) -> u64 {
        self.value(name)
            .parse()
            .unwrap_or_else(|_| panic!("{name} is not a JSON integer in {:?}", self.raw))
    }

    pub fn optional_number(&self, name: &str) -> Option<u64> {
        let value = self.value(name);
        (value != "null").then(|| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name} is not a JSON integer in {:?}", self.raw))
        })
    }

    pub fn string(&self, name: &str) -> &str {
        let value = self.value(name);
        value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
            .unwrap_or_else(|| panic!("{name} is not a JSON string in {:?}", self.raw))
    }

    pub fn contains_member(&self, name: &str, node_id: NodeId) -> bool {
        self.string(name)
            .split(',')
            .filter(|member| !member.is_empty())
            .any(|member| member == node_id.0.to_string())
    }

    pub fn replication_match(&self, node_id: NodeId) -> Option<LogIndex> {
        self.string("replication")
            .split(',')
            .filter_map(|entry| entry.split_once(':'))
            .find_map(|(node, index)| {
                (node.parse::<u64>().ok() == Some(node_id.0))
                    .then(|| index.parse().ok().map(LogIndex))
                    .flatten()
            })
    }

    fn value(&self, name: &str) -> &str {
        let needle = format!("\"{name}\":");
        let start = self
            .raw
            .find(&needle)
            .unwrap_or_else(|| panic!("{name} is absent from {:?}", self.raw))
            + needle.len();
        let rest = &self.raw[start..];
        let end = if rest.starts_with('"') {
            let mut escaped = false;
            rest.char_indices()
                .skip(1)
                .find_map(|(index, character)| {
                    if escaped {
                        escaped = false;
                        None
                    } else if character == '\\' {
                        escaped = true;
                        None
                    } else {
                        (character == '"').then_some(index + 1)
                    }
                })
                .expect("a JSON string is terminated")
        } else {
            rest.find([',', '}']).unwrap_or(rest.len())
        };
        &rest[..end]
    }
}

/// One authenticated production fixture process.
#[derive(Debug)]
pub struct ProductionNode {
    node_id: NodeId,
    child: ChildProcess,
    client_addr: SocketAddr,
    client: ReconnectingClient,
}

impl ProductionNode {
    pub fn spawn(root: &Path, node_id: NodeId, voters: &[NodeId], config: LockConfig) -> Self {
        Self::spawn_with_mode(root, node_id, voters, config, "open")
    }

    pub fn spawn_with_mode(
        root: &Path,
        node_id: NodeId,
        voters: &[NodeId],
        config: LockConfig,
        mode: &str,
    ) -> Self {
        let fixtures = fixture_dir();
        let identity = ReplicaIdentity::path(root, node_id);
        let members = voters
            .iter()
            .map(|node| node.0.to_string())
            .collect::<Vec<_>>()
            .join(",");
        let peer_certificates = (1..=4)
            .map(|node| {
                format!(
                    "{node}={}",
                    fixtures.join(format!("node-{node}.pem")).display()
                )
            })
            .collect::<Vec<_>>()
            .join(",");
        let election = match node_id.0 {
            1 => 20,
            2 => 30,
            3 => 40,
            _ => 50,
        };
        let mut command = OsCommand::new(env!("CARGO_BIN_EXE_lock-production-node"));
        command
            .arg("--id")
            .arg(node_id.0.to_string())
            .arg("--members")
            .arg(members)
            .arg("--cluster-dir")
            .arg(root)
            .arg("--identity")
            .arg(identity)
            .arg("--tls-ca")
            .arg(fixtures.join("ca.pem"))
            .arg("--tls-cert")
            .arg(fixtures.join(format!("node-{}.pem", node_id.0)))
            .arg("--tls-key")
            .arg(fixtures.join(format!("node-{}-key.pem", node_id.0)))
            .arg("--peer-certificates")
            .arg(peer_certificates)
            .arg("--election-timeout-ticks")
            .arg(election.to_string())
            .arg("--tick-interval-ms")
            .arg("20")
            .arg("--ownership-wait-ms")
            .arg("30000")
            .arg("--request-timeout-ms")
            .arg("5000")
            .arg("--max-clients")
            .arg(config.max_clients().to_string())
            .arg("--max-resources")
            .arg(config.max_resources().to_string())
            .arg("--recover")
            .arg(mode);
        let child = ChildProcess::spawn(format!("production replica {}", node_id.0), &mut command)
            .unwrap_or_else(|error| {
                panic!("could not spawn production replica {}: {error}", node_id.0)
            });
        let placeholder = "127.0.0.1:0".parse().expect("placeholder address parses");
        let mut process = Self {
            node_id,
            child,
            client_addr: placeholder,
            client: ReconnectingClient::new(placeholder, CONNECTION_TIMEOUTS),
        };
        let detail = process.wait_lifecycle("LISTENING");
        process.client_addr = detail
            .split_whitespace()
            .nth(1)
            .expect("LISTENING names the client address")
            .parse()
            .expect("the announced client address parses");
        process.client.set_addr(process.client_addr);
        process
    }

    pub const fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    pub fn peer_addr(&self, root: &Path) -> SocketAddr {
        std::fs::read_to_string(
            root.join(format!("node-{}", self.node_id.0))
                .join("peer.production.addr"),
        )
        .expect("a ready process published its authenticated peer address")
        .parse()
        .expect("the peer address parses")
    }

    pub fn wait_ready(&mut self) -> LogIndex {
        let node_id = self.node_id;
        let detail = match self.child.wait_for_stdout(
            "production replica readiness",
            WAIT,
            |lines| {
                lines
                    .iter()
                    .find_map(|line| lifecycle_detail(line, "READY"))
            },
        ) {
            Ok(detail) => detail,
            Err(error) => {
                let observation = self.ask("OBSERVE");
                panic!(
                    "production replica {} did not become ready: {error}; observation={observation:?}",
                    node_id.0
                );
            }
        };
        LogIndex(
            detail
                .split_whitespace()
                .nth(2)
                .expect("READY names an applied index")
                .parse()
                .expect("the READY applied index parses"),
        )
    }

    pub fn wait_lifecycle(&mut self, kind: &str) -> String {
        let condition = format!("production replica {} lifecycle {kind}", self.node_id.0);
        self.child
            .wait_for_stdout(&condition, WAIT, |lines| {
                lines.iter().find_map(|line| lifecycle_detail(line, kind))
            })
            .unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn ask(&mut self, line: &str) -> Result<String, String> {
        self.client.request(line).map_err(|error| error.to_string())
    }

    pub fn observe(&mut self) -> Observation {
        Observation::new(self.ask("OBSERVE").expect("a live replica answers OBSERVE"))
    }

    pub fn wait_refused(&mut self) -> (std::process::ExitStatus, String) {
        let status = self
            .child
            .wait_for_exit(WAIT)
            .unwrap_or_else(|error| panic!("{error}"));
        (status, self.child.stdout_lines().join("\n"))
    }

    pub fn kill(&mut self) {
        self.client.disconnect();
        drop(self.child.kill_and_reap());
    }

    pub fn shutdown(&mut self) {
        drop(self.ask("SHUTDOWN"));
        self.client.disconnect();
        self.child
            .wait_for_exit(WAIT)
            .unwrap_or_else(|error| panic!("{error}"));
    }
}

/// Three production-shaped replicas plus independent client history.
#[derive(Debug)]
pub struct ProductionCluster {
    root: ScratchDir,
    config: LockConfig,
    nodes: BTreeMap<NodeId, ProductionNode>,
    history: Vec<HistoryEvent>,
    next_operation_id: u64,
}

impl ProductionCluster {
    pub fn start(label: &str, config: LockConfig) -> Self {
        let root = ScratchDir::new(label);
        for expected in NODE_IDS {
            let allocated = allocate_replica(root.path(), GROUP_ID, AllocationCrashPoint::None)
                .expect("initial identity allocation succeeds");
            assert_eq!(allocated.node_id, expected, "allocation is monotonic");
        }
        let mut cluster = Self {
            root,
            config,
            nodes: BTreeMap::new(),
            history: Vec::new(),
            next_operation_id: 1,
        };
        for node_id in NODE_IDS {
            let process = ProductionNode::spawn(cluster.root(), node_id, &NODE_IDS, config);
            cluster.nodes.insert(node_id, process);
        }
        for node_id in NODE_IDS {
            cluster.node_mut(node_id).wait_ready();
        }
        cluster
    }

    pub fn root(&self) -> &Path {
        self.root.path()
    }

    pub fn config(&self) -> LockConfig {
        self.config
    }

    pub fn history(&self) -> &[HistoryEvent] {
        &self.history
    }

    pub fn live_nodes(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    pub fn node_mut(&mut self, node_id: NodeId) -> &mut ProductionNode {
        self.nodes
            .get_mut(&node_id)
            .unwrap_or_else(|| panic!("production replica {} is not running", node_id.0))
    }

    pub fn observe(&mut self, node_id: NodeId) -> Observation {
        self.node_mut(node_id).observe()
    }

    pub fn wait_for_leader(&mut self) -> NodeId {
        let live = self.live_nodes();
        wait_until("one production replica to lead", || {
            live.iter().find_map(|node_id| {
                let observation = self.nodes.get_mut(node_id)?.observe();
                (observation.boolean("ready") && observation.string("role") == "leader")
                    .then_some(*node_id)
            })
        })
    }

    pub fn wait_for_membership(&mut self, node_id: NodeId, member: NodeId, present: bool) {
        let mut last = None;
        let outcome = WAIT.until("a committed membership observation", || {
            let observation = self.observe(node_id);
            let matches = observation.contains_member("committed_members", member) == present;
            last = Some(observation);
            matches.then_some(())
        });
        if let Err(error) = outcome {
            panic!("{error}; last observation was {last:?}");
        }
    }

    pub fn submit_to_leader(&mut self, command: Command) -> SubmitOutcome {
        let operation_id = self.allocate_operation_id();
        self.history.push(HistoryEvent::Invoked {
            operation_id,
            command,
        });
        let line = render_command(command);
        let outcome = wait_until("a production leader to settle a command", || {
            let leader = self.wait_for_leader();
            let response = self.node_mut(leader).ask(&line).ok()?;
            let outcome = parse_submit_response(&response);
            (!matches!(
                outcome,
                SubmitOutcome::NotCommitted { .. } | SubmitOutcome::NotReady { .. }
            ))
            .then_some(outcome)
        });
        self.history
            .push(process_history::submit_terminal(operation_id, &outcome));
        outcome
    }

    pub fn query(&mut self, resource: ResourceName) -> QueryOutcome {
        let operation_id = self.allocate_operation_id();
        self.history
            .push(process_history::query_invocation(operation_id, resource));
        let line = format!("QUERY LOCK {}", resource.as_str());
        let outcome = wait_until("a production leader to answer a query", || {
            let leader = self.wait_for_leader();
            let response = self.node_mut(leader).ask(&line).ok()?;
            let outcome = parse_query_response(&response);
            matches!(outcome, QueryOutcome::Ready(_)).then_some(outcome)
        });
        self.history
            .push(process_history::query_terminal(operation_id, &outcome));
        outcome
    }

    pub fn ask_leader(&mut self, request: &str) -> String {
        wait_until("a production leader to accept an operation", || {
            let leader = self.wait_for_leader();
            let response = self.node_mut(leader).ask(request).ok()?;
            (!response.starts_with("NOT")).then_some(response)
        })
    }

    pub fn stop(&mut self, node_id: NodeId) {
        let mut process = self
            .nodes
            .remove(&node_id)
            .unwrap_or_else(|| panic!("production replica {} is not running", node_id.0));
        process.shutdown();
    }

    pub fn restart(&mut self, node_id: NodeId, voters: &[NodeId]) -> LogIndex {
        assert!(!self.nodes.contains_key(&node_id));
        let mut process = ProductionNode::spawn(self.root(), node_id, voters, self.config);
        let applied = process.wait_ready();
        self.nodes.insert(node_id, process);
        applied
    }

    pub fn start_process(&mut self, node_id: NodeId, voters: &[NodeId]) {
        let process = ProductionNode::spawn(self.root(), node_id, voters, self.config);
        assert!(
            self.nodes.insert(node_id, process).is_none(),
            "production replica {} is already running",
            node_id.0
        );
    }

    pub fn wait_ready(&mut self, node_id: NodeId) -> LogIndex {
        self.node_mut(node_id).wait_ready()
    }

    pub fn adopt(&mut self, node_id: NodeId, process: ProductionNode) {
        assert!(
            self.nodes.insert(node_id, process).is_none(),
            "production replica {} is already running",
            node_id.0
        );
    }

    pub fn replay_store(&self, node_id: NodeId) -> TransportReplayStore {
        TransportReplayStore::open(&self.root().join(format!("node-{}", node_id.0)), GROUP_ID)
            .expect("the process's durable replay store opens while it is stopped")
    }

    pub fn shutdown(&mut self) {
        for node_id in self.live_nodes() {
            self.node_mut(node_id).shutdown();
        }
        self.nodes.clear();
    }

    fn allocate_operation_id(&mut self) -> OperationId {
        let operation_id = OperationId::new(self.next_operation_id);
        self.next_operation_id += 1;
        operation_id
    }
}

impl Drop for ProductionCluster {
    fn drop(&mut self) {
        for (_, mut process) in std::mem::take(&mut self.nodes) {
            process.kill();
        }
    }
}

pub fn wait_until<T>(description: &str, predicate: impl FnMut() -> Option<T>) -> T {
    WAIT.until(description, predicate)
        .unwrap_or_else(|error| panic!("{error}"))
}

pub fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("production-tls")
}

fn lifecycle_detail(line: &str, kind: &str) -> Option<String> {
    let marker = format!("\"kind\":\"{kind}\"");
    if !line.contains(&marker) {
        return None;
    }
    let start = line.find("\"detail\":\"")? + "\"detail\":\"".len();
    let rest = &line[start..];
    let end = rest.find("\"}")?;
    Some(rest[..end].to_string())
}
