//! External orchestration for the sharded-counter process fixture.
//!
//! The harness owns its command lines, protocol parser, expected counter state,
//! and lifecycle assertions. It observes only process output, sockets, and
//! durable paths. Real time enters through bounded predicate waits; there are no
//! fixed sleeps that assume an election, recovery, or write has completed.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    net::{SocketAddr, TcpStream},
    path::Path,
    process::Command,
    time::Duration,
};

use rafter::{LogIndex, Message, NodeId, RequestVote, Term};
use rafter_codec::encode_message;
use rafter_crc32::crc32;
use rafter_reference_harness::process::{
    ChildProcess, ConnectionTimeouts, ReconnectingClient, ScratchSpace, Wait,
};

pub const NODE_IDS: [u64; 3] = [1, 2, 3];
pub const GROUP_COUNT: u32 = 16;

const ELECTION_TIMEOUTS: [(u64, u64); 3] = [(1, 20), (2, 30), (3, 40)];
const PROCESS_WAIT: Wait = Wait::new(Duration::from_secs(30), Duration::from_millis(10));
pub const CONNECTION_TIMEOUTS: ConnectionTimeouts = ConnectionTimeouts::new(
    Duration::from_secs(5),
    Duration::from_secs(20),
    Duration::from_secs(5),
);

#[derive(Debug)]
pub struct NodeProcess {
    node_id: u64,
    child: ChildProcess,
    client_addr: SocketAddr,
    client: ReconnectingClient,
}

impl NodeProcess {
    fn spawn(scratch: &ScratchSpace, node_id: u64) -> Self {
        Self::spawn_with_failpoint(scratch, node_id, None)
    }

    fn spawn_with_failpoint(scratch: &ScratchSpace, node_id: u64, failpoint: Option<&str>) -> Self {
        let election_timeout = ELECTION_TIMEOUTS
            .iter()
            .find_map(|(candidate, timeout)| (*candidate == node_id).then_some(*timeout))
            .expect("every process node has an election timeout");
        let mut command = Command::new(env!("CARGO_BIN_EXE_counter-node"));
        command
            .arg("--id")
            .arg(node_id.to_string())
            .arg("--members")
            .arg("1,2,3")
            .arg("--cluster-dir")
            .arg(scratch.path())
            .arg("--groups")
            .arg(GROUP_COUNT.to_string())
            .arg("--election-timeout-ticks")
            .arg(election_timeout.to_string())
            .arg("--tick-interval-ms")
            .arg("20")
            .arg("--request-timeout-ms")
            .arg("5000")
            .arg("--max-sessions")
            .arg("64")
            .arg("--quota")
            .arg("4")
            .arg("--workers")
            .arg("4")
            .arg("--max-group-queue")
            .arg("64")
            .arg("--max-global-queue")
            .arg("1024")
            .env(
                "RAFTER_COUNTER_FAILPOINT_FILE",
                scratch
                    .path()
                    .join(format!("host-{node_id}/armed.failpoint")),
            );
        if let Some(failpoint) = failpoint {
            command.env("RAFTER_COUNTER_FAILPOINT", failpoint);
        }
        let child =
            ChildProcess::spawn_in(format!("counter node {node_id}"), &mut command, scratch)
                .unwrap_or_else(|error| panic!("could not spawn counter node {node_id}: {error}"));
        let placeholder = "127.0.0.1:0".parse().expect("placeholder address parses");
        let mut node = Self {
            node_id,
            child,
            client_addr: placeholder,
            client: ReconnectingClient::new(placeholder, CONNECTION_TIMEOUTS),
        };
        let line = node
            .child
            .wait_for_stdout_prefix(&format!("LISTENING {node_id} "), PROCESS_WAIT)
            .unwrap_or_else(|error| panic!("{error}"));
        node.client_addr = line
            .split_whitespace()
            .nth(2)
            .expect("LISTENING names an address")
            .parse()
            .expect("announced client address parses");
        node.client.set_addr(node.client_addr);
        node
    }

    fn request(&mut self, line: &str) -> Result<String, String> {
        self.client
            .request(line)
            .map_err(|error| format!("node {} request {line:?} failed: {error}", self.node_id))
    }

    pub const fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    fn kill(&mut self) {
        self.child
            .kill_and_reap()
            .unwrap_or_else(|error| panic!("could not kill node {}: {error}", self.node_id));
    }

    fn shutdown(&mut self) {
        let response = self
            .request("SHUTDOWN")
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(response, format!("OK SHUTDOWN {}", self.node_id));
        let status = self
            .child
            .wait_for_exit(PROCESS_WAIT)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(status.success(), "node {} exited as {status}", self.node_id);
    }

    fn wait_for_failpoint_exit(&mut self, failpoint: &str) {
        let observed = self
            .child
            .wait_for_stdout_prefix(&format!("FAILPOINT {failpoint}"), PROCESS_WAIT)
            .unwrap_or_else(|error| panic!("{error}"));
        assert_eq!(observed, format!("FAILPOINT {failpoint}"));
        let status = self
            .child
            .wait_for_exit(PROCESS_WAIT)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(!status.success(), "{failpoint} must stop the process");
    }
}

#[derive(Debug)]
pub struct ProcessCluster {
    scratch: ScratchSpace,
    nodes: BTreeMap<u64, NodeProcess>,
}

impl ProcessCluster {
    pub fn start(label: &str) -> Self {
        let scratch = ScratchSpace::create("rafter-counter", label)
            .unwrap_or_else(|error| panic!("could not create process scratch space: {error}"));
        let nodes = NODE_IDS
            .into_iter()
            .map(|node_id| (node_id, NodeProcess::spawn(&scratch, node_id)))
            .collect();
        let mut cluster = Self { scratch, nodes };
        cluster.wait_ready();
        cluster
    }

    pub fn start_after_bootstrap_failpoint(label: &str, failpoint: &str) -> Self {
        let scratch = ScratchSpace::create("rafter-counter", label)
            .unwrap_or_else(|error| panic!("could not create process scratch space: {error}"));
        let mut interrupted = NodeProcess::spawn_with_failpoint(&scratch, 1, Some(failpoint));
        interrupted.wait_for_failpoint_exit(failpoint);
        let nodes = NODE_IDS
            .into_iter()
            .map(|node_id| (node_id, NodeProcess::spawn(&scratch, node_id)))
            .collect();
        let mut cluster = Self { scratch, nodes };
        cluster.wait_ready();
        cluster
    }

    pub fn scratch_path(&self) -> &Path {
        self.scratch.path()
    }

    pub fn node_addr(&self, node_id: u64) -> SocketAddr {
        self.nodes[&node_id].client_addr()
    }

    pub fn live_node_ids(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    pub fn request_on(&mut self, node_id: u64, line: &str) -> String {
        self.try_request_on(node_id, line)
            .unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn try_request_on(&mut self, node_id: u64, line: &str) -> Result<String, String> {
        self.nodes
            .get_mut(&node_id)
            .unwrap_or_else(|| panic!("node {node_id} is not live"))
            .request(line)
    }

    pub fn request_leader(&mut self, line: &str) -> String {
        PROCESS_WAIT
            .until(format!("a leader to answer {line:?}"), || {
                for node_id in self.live_node_ids() {
                    if let Ok(response) = self
                        .nodes
                        .get_mut(&node_id)
                        .expect("live node")
                        .request(line)
                    {
                        if response.starts_with("OK ") {
                            return Some(response);
                        }
                    }
                }
                None
            })
            .unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn request_each(&mut self, line: &str) -> Vec<String> {
        self.live_node_ids()
            .into_iter()
            .map(|node_id| self.request_on(node_id, line))
            .collect()
    }

    pub fn wait_response(&mut self, node_id: u64, line: &str, expected: &str) {
        let mut last = None;
        let result = PROCESS_WAIT.until(
            format!("node {node_id} to answer {line:?} with {expected:?}"),
            || match self
                .nodes
                .get_mut(&node_id)
                .expect("node is live")
                .request(line)
            {
                Ok(response) if response == expected => Some(()),
                Ok(response) => {
                    last = Some(response);
                    None
                }
                Err(error) => {
                    last = Some(format!("connection error: {error}"));
                    None
                }
            },
        );
        result.unwrap_or_else(|error| panic!("{error}; last response: {last:?}"));
    }

    pub fn wait_response_one_of(&mut self, node_id: u64, line: &str, expected: &[&str]) -> String {
        let mut last = None;
        let result = PROCESS_WAIT.until(
            format!("node {node_id} to answer {line:?} with one of {expected:?}"),
            || match self
                .nodes
                .get_mut(&node_id)
                .expect("node is live")
                .request(line)
            {
                Ok(response) if expected.contains(&response.as_str()) => Some(response),
                Ok(response) => {
                    last = Some(response);
                    None
                }
                Err(error) => {
                    last = Some(format!("connection error: {error}"));
                    None
                }
            },
        );
        result.unwrap_or_else(|error| panic!("{error}; last response: {last:?}"))
    }

    pub fn wait_ready(&mut self) {
        let mut last = BTreeMap::new();
        let result = PROCESS_WAIT.until(
            "all live nodes to recover and every group to have a live leader",
            || {
                let mut ready = true;
                let mut leader_groups = BTreeSet::new();
                let mut active_groups = 0;
                for node_id in self.live_node_ids() {
                    match self
                        .nodes
                        .get_mut(&node_id)
                        .expect("live node")
                        .request("STATUS")
                    {
                        Ok(status) => {
                            ready &= field(&status, "ready") == Some("true");
                            active_groups = active_groups.max(
                                usize::try_from(number_field(&status, "groups"))
                                    .expect("configured group count fits usize"),
                            );
                            if let Some(groups) = field(&status, "leader_groups") {
                                leader_groups.extend(
                                    groups
                                        .split(',')
                                        .filter(|group| *group != "-")
                                        .map(|group| {
                                            group.parse::<u32>().unwrap_or_else(|_| {
                                                panic!(
                                                    "invalid leader group {group:?} in \
                                                     status {status:?}"
                                                )
                                            })
                                        }),
                                );
                            }
                            last.insert(node_id, status);
                        }
                        Err(error) => {
                            ready = false;
                            last.insert(node_id, format!("connection error: {error}"));
                        }
                    }
                }
                (ready && leader_groups.len() == active_groups).then_some(())
            },
        );
        result.unwrap_or_else(|error| panic!("{error}; last statuses: {last:?}"));
    }

    pub fn wait_ready_on(&mut self, node_id: u64) {
        let mut last = None;
        let result = PROCESS_WAIT.until(format!("node {node_id} to recover"), || {
            match self
                .nodes
                .get_mut(&node_id)
                .expect("node is live")
                .request("STATUS")
            {
                Ok(status) => {
                    let ready = field(&status, "ready") == Some("true");
                    last = Some(status);
                    ready.then_some(())
                }
                Err(error) => {
                    last = Some(format!("connection error: {error}"));
                    None
                }
            }
        });
        result.unwrap_or_else(|error| panic!("{error}; last status: {last:?}"));
    }

    pub fn leader(&mut self) -> u64 {
        PROCESS_WAIT
            .until("one live host to lead at least one group", || {
                for node_id in self.live_node_ids() {
                    let Ok(status) = self
                        .nodes
                        .get_mut(&node_id)
                        .expect("live node")
                        .request("STATUS")
                    else {
                        continue;
                    };
                    if number_field(&status, "leaders") != 0 {
                        return Some(node_id);
                    }
                }
                None
            })
            .unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn leader_group_on(&mut self, node_id: u64) -> u32 {
        PROCESS_WAIT
            .until(format!("node {node_id} to lead one group"), || {
                let status = self
                    .nodes
                    .get_mut(&node_id)
                    .expect("node is live")
                    .request("STATUS")
                    .ok()?;
                field(&status, "leader_groups")?
                    .split(',')
                    .find(|group| *group != "-")
                    .and_then(|group| group.parse().ok())
            })
            .unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn leader_for_group(&mut self, group_id: u32) -> u64 {
        self.leader_for_group_excluding(group_id, None)
    }

    pub fn wait_for_group_leader(&mut self, node_id: u64, group_id: u32) {
        PROCESS_WAIT
            .until(format!("node {node_id} to lead group {group_id}"), || {
                let status = self
                    .nodes
                    .get_mut(&node_id)
                    .expect("node is live")
                    .request("STATUS")
                    .ok()?;
                field(&status, "leader_groups")?
                    .split(',')
                    .any(|group| group.parse::<u32>() == Ok(group_id))
                    .then_some(())
            })
            .unwrap_or_else(|error| panic!("{error}"));
    }

    pub fn leader_for_group_excluding(&mut self, group_id: u32, excluded: Option<u64>) -> u64 {
        PROCESS_WAIT
            .until(
                format!("one live host other than {excluded:?} to lead group {group_id}"),
                || {
                    for node_id in self.live_node_ids() {
                        if excluded == Some(node_id) {
                            continue;
                        }
                        let status = self
                            .nodes
                            .get_mut(&node_id)
                            .expect("live node")
                            .request("STATUS")
                            .ok()?;
                        if field(&status, "leader_groups")?
                            .split(',')
                            .any(|group| group.parse::<u32>() == Ok(group_id))
                        {
                            return Some(node_id);
                        }
                    }
                    None
                },
            )
            .unwrap_or_else(|error| panic!("{error}"))
    }

    pub fn kill(&mut self, node_id: u64) {
        let mut node = self
            .nodes
            .remove(&node_id)
            .unwrap_or_else(|| panic!("node {node_id} is not live"));
        node.kill();
    }

    pub fn clean_stop(&mut self, node_id: u64) {
        let mut node = self
            .nodes
            .remove(&node_id)
            .unwrap_or_else(|| panic!("node {node_id} is not live"));
        node.shutdown();
    }

    pub fn restart(&mut self, node_id: u64) {
        self.restart_with_failpoint(node_id, None);
    }

    pub fn restart_with_failpoint(&mut self, node_id: u64, failpoint: Option<&str>) {
        assert!(
            !self.nodes.contains_key(&node_id),
            "node {node_id} is already live"
        );
        self.nodes.insert(
            node_id,
            NodeProcess::spawn_with_failpoint(&self.scratch, node_id, failpoint),
        );
    }

    pub fn trigger_failpoint(&mut self, node_id: u64, line: &str, failpoint: &str) {
        let node = self
            .nodes
            .get_mut(&node_id)
            .unwrap_or_else(|| panic!("node {node_id} is not live"));
        PROCESS_WAIT
            .until(format!("{failpoint} to sever {line:?}"), || {
                match node.request(line) {
                    Err(_) => Some(()),
                    Ok(response) if response.starts_with("ERR BUSY ") => None,
                    Ok(response) => {
                        panic!("{failpoint} returned {response:?} instead of severing the request")
                    }
                }
            })
            .unwrap_or_else(|error| panic!("{error}"));
        node.wait_for_failpoint_exit(failpoint);
        self.nodes.remove(&node_id);
        self.clear_armed_failpoint(node_id);
    }

    pub fn wait_for_failpoint_exit(&mut self, node_id: u64, failpoint: &str) {
        let node = self
            .nodes
            .get_mut(&node_id)
            .unwrap_or_else(|| panic!("node {node_id} is not live"));
        node.wait_for_failpoint_exit(failpoint);
        self.nodes.remove(&node_id);
        self.clear_armed_failpoint(node_id);
    }

    pub fn arm_failpoint(&self, node_id: u64, failpoint: &str) {
        let path = self
            .scratch
            .path()
            .join(format!("host-{node_id}/armed.failpoint"));
        fs::write(&path, failpoint)
            .unwrap_or_else(|error| panic!("could not arm {}: {error}", path.display()));
    }

    pub fn restart_expect_fatal(&mut self, node_id: u64, expected: &str) {
        assert!(
            !self.nodes.contains_key(&node_id),
            "node {node_id} is already live"
        );
        let mut node = NodeProcess::spawn(&self.scratch, node_id);
        let fatal = node
            .child
            .wait_for_stdout_prefix("FATAL ", PROCESS_WAIT)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(
            fatal.contains(expected),
            "fatal refusal {fatal:?} did not contain {expected:?}"
        );
        let status = node
            .child
            .wait_for_exit(PROCESS_WAIT)
            .unwrap_or_else(|error| panic!("{error}"));
        assert!(!status.success(), "corrupt identity must fail closed");
    }

    fn clear_armed_failpoint(&self, node_id: u64) {
        let path = self
            .scratch
            .path()
            .join(format!("host-{node_id}/armed.failpoint"));
        if let Err(error) = fs::remove_file(&path) {
            assert_eq!(
                error.kind(),
                std::io::ErrorKind::NotFound,
                "could not clear {}: {error}",
                path.display()
            );
        }
    }

    pub fn wait_value(&mut self, node_id: u64, group: u32, incarnation: u32, expected: i64) {
        PROCESS_WAIT
            .until(
                format!("node {node_id} group {group}/{incarnation} to report value {expected}"),
                || {
                    let response = self
                        .nodes
                        .get_mut(&node_id)
                        .expect("live node")
                        .request(&format!("VALUE {group} {incarnation}"))
                        .ok()?;
                    (signed_field(&response, "value") == Some(expected)).then_some(())
                },
            )
            .unwrap_or_else(|error| panic!("{error}"));
    }

    pub fn wait_all_values(&mut self, expected: &BTreeMap<(u32, u32), i64>) {
        for node_id in self.live_node_ids() {
            for (&(group, incarnation), &value) in expected {
                self.wait_value(node_id, group, incarnation, value);
            }
        }
    }

    pub fn assert_audits(&mut self) {
        for node_id in self.live_node_ids() {
            let audit = self.request_on(node_id, "AUDIT");
            assert_eq!(field(&audit, "conserved"), Some("true"), "{audit}");
            assert_eq!(number_field(&audit, "invalid_plans"), 0, "{audit}");
            assert_eq!(number_field(&audit, "invalid_turns"), 0, "{audit}");
            assert_ne!(
                number_field(&audit, "certified_passes"),
                0,
                "fairness requires at least one fully closed pass: {audit}"
            );
            assert_ne!(
                number_field(&audit, "coverage"),
                0,
                "cumulative coverage is supplemental but must be non-vacuous: {audit}"
            );
            assert_ne!(field(&audit, "plan_digest"), Some("0000000000000000"));
            assert_ne!(field(&audit, "turn_digest"), Some("0000000000000000"));
        }
    }

    pub fn wait_refused_peer_above(&mut self, node_id: u64, baseline: u64) {
        self.wait_status_above(node_id, "refused_peer", baseline);
    }

    pub fn wait_status_above(&mut self, node_id: u64, name: &str, baseline: u64) {
        PROCESS_WAIT
            .until(
                format!("node {node_id} status {name} to exceed {baseline}"),
                || {
                    let status = self
                        .nodes
                        .get_mut(&node_id)
                        .expect("live node")
                        .request("STATUS")
                        .ok()?;
                    (number_field(&status, name) > baseline).then_some(())
                },
            )
            .unwrap_or_else(|error| panic!("{error}"));
    }

    pub fn wait_status_at_least(&mut self, node_id: u64, name: &str, minimum: u64) {
        PROCESS_WAIT
            .until(
                format!("node {node_id} status {name} to reach {minimum}"),
                || {
                    let status = self
                        .nodes
                        .get_mut(&node_id)
                        .expect("live node")
                        .request("STATUS")
                        .ok()?;
                    (number_field(&status, name) >= minimum).then_some(())
                },
            )
            .unwrap_or_else(|error| panic!("{error}"));
    }

    pub fn refused_peer(&mut self, node_id: u64) -> u64 {
        let status = self.request_on(node_id, "STATUS");
        number_field(&status, "refused_peer")
    }
}

impl Drop for ProcessCluster {
    fn drop(&mut self) {
        for node in self.nodes.values_mut() {
            node.kill();
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct AppliedAdd {
    group: u32,
    incarnation: u32,
    client: u32,
    sequence: u64,
    delta: i64,
    observed: i64,
}

#[derive(Debug, Default)]
pub struct ProcessHistory {
    events: Vec<AppliedAdd>,
    expected: BTreeMap<(u32, u32), i64>,
}

impl ProcessHistory {
    pub fn add(
        &mut self,
        cluster: &mut ProcessCluster,
        group: u32,
        incarnation: u32,
        client: u32,
        sequence: u64,
        delta: i64,
    ) -> String {
        let response = cluster.request_leader(&format!(
            "ADD {group} {incarnation} {client} 1 {sequence} {delta}"
        ));
        let observed = signed_field(&response, "value")
            .unwrap_or_else(|| panic!("an applied add must report its value: {response}"));
        let expected = self.expected.entry((group, incarnation)).or_default();
        *expected += delta;
        assert_eq!(observed, *expected, "independent history disagreed");
        self.events.push(AppliedAdd {
            group,
            incarnation,
            client,
            sequence,
            delta,
            observed,
        });
        response
    }

    pub fn reset_group(&mut self, group: u32, incarnation: u32) {
        self.expected.insert((group, incarnation), 0);
    }

    pub fn assert_complete(&self, cluster: &mut ProcessCluster) {
        assert!(
            !self.events.is_empty(),
            "a process scenario must record a nonempty history"
        );
        for event in &self.events {
            assert_ne!(event.delta, 0);
            assert_ne!(event.sequence, 0);
            assert_eq!(
                self.expected[&(event.group, event.incarnation)],
                self.events
                    .iter()
                    .filter(|candidate| {
                        candidate.group == event.group && candidate.incarnation == event.incarnation
                    })
                    .map(|candidate| candidate.delta)
                    .sum::<i64>()
            );
        }
        cluster.wait_all_values(&self.expected);
    }

    pub fn expected(&self, group: u32, incarnation: u32) -> i64 {
        self.expected[&(group, incarnation)]
    }
}

pub fn open_session(cluster: &mut ProcessCluster, group: u32, incarnation: u32, client: u32) {
    let response = cluster.request_leader(&format!("OPEN {group} {incarnation} {client} 1"));
    assert!(
        matches!(
            response.as_str(),
            "OK SESSION opened" | "OK SESSION already_open" | "OK SESSION replaced"
        ),
        "{response}"
    );
}

pub fn field<'a>(line: &'a str, name: &str) -> Option<&'a str> {
    line.split_whitespace().find_map(|field| {
        let (candidate, value) = field.split_once('=')?;
        (candidate == name).then_some(value)
    })
}

pub fn number_field(line: &str, name: &str) -> u64 {
    field(line, name)
        .unwrap_or_else(|| panic!("{line:?} has no {name} field"))
        .parse()
        .unwrap_or_else(|_| panic!("{line:?} has a nonnumeric {name} field"))
}

fn signed_field(line: &str, name: &str) -> Option<i64> {
    field(line, name)?.parse().ok()
}

pub fn send_stale_vote(cluster_dir: &Path, target: u64, group: u32, incarnation: u32) {
    let address = fs::read_to_string(cluster_dir.join(format!("host-{target}")).join("peer.addr"))
        .expect("target peer address is published")
        .trim()
        .parse::<SocketAddr>()
        .expect("published peer address parses");
    let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))
        .expect("late peer connection opens");
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .expect("late peer write timeout is installed");
    let claimed = NODE_IDS
        .into_iter()
        .find(|node_id| *node_id != target)
        .expect("a different peer exists");
    stream
        .write_all(&claimed.to_be_bytes())
        .expect("peer preamble sender writes");
    stream
        .write_all(&target.to_be_bytes())
        .expect("peer preamble target writes");
    let message = Message::RequestVote(RequestVote {
        term: Term(1),
        candidate_id: NodeId(claimed),
        last_log_index: LogIndex(0),
        last_log_term: Term(0),
    });
    let message = encode_message(&message).expect("late vote message encodes");
    let mut body = Vec::new();
    body.extend_from_slice(b"RCPE");
    body.push(1);
    body.extend_from_slice(&group.to_be_bytes());
    body.extend_from_slice(&incarnation.to_be_bytes());
    body.extend_from_slice(&claimed.to_be_bytes());
    body.extend_from_slice(&target.to_be_bytes());
    body.extend_from_slice(
        &u32::try_from(message.len())
            .expect("encoded vote length fits u32")
            .to_be_bytes(),
    );
    body.extend_from_slice(&message);
    body.extend_from_slice(&crc32(&body).to_be_bytes());
    stream
        .write_all(
            &u32::try_from(body.len())
                .expect("peer frame length fits u32")
                .to_be_bytes(),
        )
        .expect("peer frame length writes");
    stream.write_all(&body).expect("peer frame writes");
    stream.flush().expect("late peer frame flushes");
}

pub fn fill_peer_connection_bound(cluster_dir: &Path, target: u64) -> Vec<TcpStream> {
    let address = fs::read_to_string(cluster_dir.join(format!("host-{target}")).join("peer.addr"))
        .expect("target peer address is published")
        .trim()
        .parse::<SocketAddr>()
        .expect("published peer address parses");
    let claimed = NODE_IDS
        .into_iter()
        .find(|node_id| *node_id != target)
        .expect("a different peer exists");
    let mut connections = Vec::new();
    for _ in 0..70 {
        let mut stream = TcpStream::connect_timeout(&address, Duration::from_secs(5))
            .expect("bounded peer connection opens");
        stream
            .set_write_timeout(Some(Duration::from_secs(5)))
            .expect("bounded peer write timeout is installed");
        let sender = stream.write_all(&claimed.to_be_bytes());
        let target = sender.and_then(|()| stream.write_all(&target.to_be_bytes()));
        match target {
            Ok(()) => connections.push(stream),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::BrokenPipe
                        | std::io::ErrorKind::ConnectionAborted
                        | std::io::ErrorKind::ConnectionReset
                ) => {}
            Err(error) => panic!("bounded peer preamble failed unexpectedly: {error}"),
        }
    }
    connections
}
