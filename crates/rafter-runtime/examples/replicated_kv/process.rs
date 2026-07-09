use std::collections::{BTreeMap, VecDeque};
use std::io::{BufRead, BufReader, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::{Duration, Instant};

use rafter::{Input, LogIndex, Message, NodeId, Output, ReadId, Role};
use rafter_transport_tcp_insecure::{InsecureTcpTransport, ReconnectBackoff};

use super::{
    app_state::{load_app_state, persist_app_state},
    codec::{
        apply_set_with_parts, decode_snapshot, decode_value, encode_set, encode_value, fields,
        parse_log_index, parse_node_id, parse_peer,
    },
    storage::{compact_kv_snapshot, node_dir, open_node, read_snapshot_payload},
    types::{
        FileNode, ScenarioOptions, ScenarioReport, ELECTION_TIMEOUT_TICKS, PROCESS_DRIVER_INTERVAL,
        PROCESS_PENDING_LIMIT, PROCESS_STEP_TIMEOUT,
    },
};

/// Spawner used by the process-per-node scenario.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessSpawn {
    executable: PathBuf,
    mode: ProcessSpawnMode,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum ProcessSpawnMode {
    ExampleBinary,
    RustTestHarness,
}

impl ProcessSpawn {
    /// Runs child nodes by reinvoking this example binary with `--process-node`.
    #[must_use]
    pub fn example_binary(executable: PathBuf) -> Self {
        Self {
            executable,
            mode: ProcessSpawnMode::ExampleBinary,
        }
    }

    /// Runs child nodes by reinvoking an ignored Rust integration-test child.
    #[must_use]
    pub fn test_harness(executable: PathBuf) -> Self {
        Self {
            executable,
            mode: ProcessSpawnMode::RustTestHarness,
        }
    }

    fn command(&self, node_id: NodeId, root: &Path) -> Command {
        match self.mode {
            ProcessSpawnMode::ExampleBinary => {
                let mut command = Command::new(&self.executable);
                command
                    .arg("--process-node")
                    .arg("--id")
                    .arg(node_id.0.to_string())
                    .arg("--root")
                    .arg(root);
                command
            }
            ProcessSpawnMode::RustTestHarness => {
                let mut command = Command::new(&self.executable);
                command
                    .arg("--ignored")
                    .arg("--exact")
                    .arg("replicated_kv_process_node_child")
                    .arg("--nocapture")
                    .env("RAFTER_KV_PROCESS_NODE", "1")
                    .env("RAFTER_KV_NODE_ID", node_id.0.to_string())
                    .env("RAFTER_KV_ROOT", root);
                command
            }
        }
    }
}

#[derive(Debug)]
struct ProcessLine {
    node_id: NodeId,
    line: String,
}

#[derive(Debug)]
struct ProcessNode {
    child: Child,
    stdin: ChildStdin,
}

#[derive(Debug)]
struct ProcessCluster {
    root: PathBuf,
    spawn: ProcessSpawn,
    processes: BTreeMap<NodeId, ProcessNode>,
    addresses: BTreeMap<NodeId, SocketAddr>,
    stdout_rx: mpsc::Receiver<ProcessLine>,
    stdout_tx: mpsc::Sender<ProcessLine>,
    pending: VecDeque<ProcessLine>,
    next_read_id: u64,
}

impl ProcessCluster {
    fn open(root: PathBuf, spawn: ProcessSpawn) -> Self {
        let (stdout_tx, stdout_rx) = mpsc::channel();
        let mut cluster = Self {
            root,
            spawn,
            processes: BTreeMap::new(),
            addresses: BTreeMap::new(),
            stdout_rx,
            stdout_tx,
            pending: VecDeque::new(),
            next_read_id: 1,
        };
        for node_id in super::types::NODE_IDS {
            cluster.spawn_node(node_id);
        }
        for node_id in super::types::NODE_IDS {
            let (ready_id, addr, _) = cluster.wait_ready(node_id);
            assert_eq!(ready_id, node_id);
            cluster.addresses.insert(node_id, addr);
        }
        cluster.broadcast_peers();
        cluster
    }

    fn spawn_node(&mut self, node_id: NodeId) {
        let mut command = self.spawn.command(node_id, &self.root);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let mut child = command.spawn().expect("spawn replicated KV process node");
        let stdin = child.stdin.take().expect("child stdin is piped");
        let stdout = child.stdout.take().expect("child stdout is piped");
        let stderr = child.stderr.take().expect("child stderr is piped");
        let stdout_tx = self.stdout_tx.clone();
        thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(line) => {
                        stdout_tx
                            .send(ProcessLine { node_id, line })
                            .expect("process event receiver is alive");
                    }
                    Err(error) => {
                        stdout_tx
                            .send(ProcessLine {
                                node_id,
                                line: format!("STDOUT_ERROR {error}"),
                            })
                            .expect("process event receiver is alive");
                        break;
                    }
                }
            }
        });
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                eprintln!("process {node_id} stderr: {line}");
            }
        });
        self.processes.insert(node_id, ProcessNode { child, stdin });
    }

    fn wait_ready(&mut self, node_id: NodeId) -> (NodeId, SocketAddr, LogIndex) {
        let event = self.wait_for_event(
            PROCESS_STEP_TIMEOUT,
            |_| {},
            |event| event.node_id == node_id && event.line.starts_with("READY "),
        );
        let fields = fields(&event.line);
        assert_eq!(fields.len(), 4, "unexpected READY line: {}", event.line);
        (
            parse_node_id(fields[1]),
            fields[2].parse().expect("READY address parses"),
            parse_log_index(fields[3]),
        )
    }

    fn broadcast_peers(&mut self) {
        let mut command = String::from("PEERS");
        for (node_id, addr) in &self.addresses {
            command.push(' ');
            command.push_str(&node_id.0.to_string());
            command.push('=');
            command.push_str(&addr.to_string());
        }
        let live: Vec<_> = self.processes.keys().copied().collect();
        for node_id in &live {
            self.send_command(*node_id, &command);
        }
        for node_id in live {
            self.wait_for_line(
                PROCESS_STEP_TIMEOUT,
                |_| {},
                |event| event.node_id == node_id && event.line == format!("PEERS_OK {}", node_id.0),
            );
        }
    }

    fn elect_node_one(&mut self) -> NodeId {
        for _ in 0..ELECTION_TIMEOUT_TICKS {
            self.send_command(NodeId(1), "TICK");
        }
        self.wait_role(NodeId(1), Role::Leader);
        NodeId(1)
    }

    fn propose_set(&mut self, leader: NodeId, key: &str, value: &str) {
        self.send_command(leader, &format!("PROPOSE {key} {value}"));
        self.wait_applied(leader, leader, key, value);
    }

    fn linearizable_get(&mut self, leader: NodeId, key: &str) -> Option<String> {
        let request_id = self.next_read_id;
        self.next_read_id += 1;
        self.send_command(leader, &format!("READ {request_id} {key}"));
        let event = self.wait_for_line(
            PROCESS_STEP_TIMEOUT,
            |cluster| cluster.tick_node(leader),
            |event| {
                event.node_id == leader
                    && event
                        .line
                        .starts_with(&format!("READ {} {request_id} {key} ", leader.0))
            },
        );
        let fields = fields(&event.line);
        assert_eq!(fields.len(), 6, "unexpected READ line: {}", event.line);
        decode_value(fields[4])
    }

    fn restart_node(&mut self, node_id: NodeId) -> LogIndex {
        if self.processes.contains_key(&node_id) {
            self.kill_node(node_id);
        }
        self.spawn_node(node_id);
        let (_, addr, applied_floor) = self.wait_ready(node_id);
        self.addresses.insert(node_id, addr);
        self.broadcast_peers();
        applied_floor
    }

    fn kill_node(&mut self, node_id: NodeId) {
        let mut process = self.processes.remove(&node_id).expect("process exists");
        drop(process.stdin);
        process.child.kill().ok();
        process.child.wait().ok();
    }

    fn compact_snapshot(&mut self, leader: NodeId) -> LogIndex {
        self.send_command(leader, "SNAPSHOT");
        let event = self.wait_for_line(
            PROCESS_STEP_TIMEOUT,
            |cluster| cluster.tick_node(leader),
            |event| event.node_id == leader && event.line.starts_with("SNAPSHOT "),
        );
        let fields = fields(&event.line);
        assert_eq!(fields.len(), 3, "unexpected SNAPSHOT line: {}", event.line);
        parse_log_index(fields[2])
    }

    fn wait_role(&mut self, node_id: NodeId, role: Role) {
        self.wait_for_line(
            PROCESS_STEP_TIMEOUT,
            |cluster| {
                cluster.tick_node(node_id);
                cluster.send_command(node_id, "ROLE");
            },
            |event| {
                event.node_id == node_id
                    && event
                        .line
                        .starts_with(&format!("ROLE {} {role} ", node_id.0))
            },
        );
    }

    fn wait_applied(
        &mut self,
        drive_node: NodeId,
        node_id: NodeId,
        key: &str,
        value: &str,
    ) -> LogIndex {
        let event = self.wait_for_line(
            PROCESS_STEP_TIMEOUT,
            |cluster| cluster.tick_node(drive_node),
            |event| {
                event.node_id == node_id
                    && event.line.starts_with(&format!("APPLIED {} ", node_id.0))
                    && event.line.ends_with(&format!(" {key} {value}"))
            },
        );
        let fields = fields(&event.line);
        assert_eq!(fields.len(), 5, "unexpected APPLIED line: {}", event.line);
        parse_log_index(fields[2])
    }

    fn wait_value(
        &mut self,
        drive_node: NodeId,
        node_id: NodeId,
        key: &str,
        value: &str,
    ) -> LogIndex {
        let event = self.wait_for_line(
            PROCESS_STEP_TIMEOUT,
            |cluster| {
                cluster.tick_node(drive_node);
                cluster.send_command(node_id, &format!("VALUE {key}"));
            },
            |event| {
                event.node_id == node_id
                    && event
                        .line
                        .starts_with(&format!("VALUE {} {key} {value} ", node_id.0))
            },
        );
        let fields = fields(&event.line);
        assert_eq!(fields.len(), 5, "unexpected VALUE line: {}", event.line);
        parse_log_index(fields[4])
    }

    fn transfer_leadership(&mut self, leader: NodeId, target: NodeId) {
        self.send_command(leader, &format!("TRANSFER {}", target.0));
        self.wait_role(target, Role::Leader);
    }

    fn collect_values(&mut self, node_id: NodeId) -> BTreeMap<String, String> {
        let mut values = BTreeMap::new();
        for key in ["alpha", "beta", "gamma", "delta"] {
            self.send_command(node_id, &format!("VALUE {key}"));
            let event = self.wait_for_line(
                PROCESS_STEP_TIMEOUT,
                |cluster| cluster.tick_node(node_id),
                |event| {
                    event.node_id == node_id
                        && event
                            .line
                            .starts_with(&format!("VALUE {} {key} ", node_id.0))
                },
            );
            let event_fields = fields(&event.line);
            if let Some(value) = decode_value(event_fields[3]) {
                values.insert(key.to_string(), value);
            }
        }
        values
    }

    fn tick_node(&mut self, node_id: NodeId) {
        if self.processes.contains_key(&node_id) {
            self.send_command(node_id, "TICK");
        }
    }

    fn shutdown(mut self) -> PathBuf {
        let live: Vec<_> = self.processes.keys().copied().collect();
        for node_id in &live {
            self.send_command(*node_id, "STOP");
        }
        for node_id in live {
            if let Some(mut process) = self.processes.remove(&node_id) {
                drop(process.stdin);
                process.child.wait().ok();
            }
        }
        self.root
    }

    fn send_command(&mut self, node_id: NodeId, command: &str) {
        let process = self.processes.get_mut(&node_id).expect("process exists");
        writeln!(process.stdin, "{command}").expect("send process command");
        process.stdin.flush().expect("flush process command");
    }

    fn wait_for_line<F, D>(&mut self, timeout: Duration, driver: D, predicate: F) -> ProcessLine
    where
        F: FnMut(&ProcessLine) -> bool,
        D: FnMut(&mut Self),
    {
        self.wait_for_event(timeout, driver, predicate)
    }

    fn push_pending(&mut self, event: ProcessLine) {
        if self.pending.len() == PROCESS_PENDING_LIMIT {
            self.pending.pop_front();
        }
        self.pending.push_back(event);
    }

    fn wait_for_event<F, D>(
        &mut self,
        timeout: Duration,
        mut driver: D,
        mut predicate: F,
    ) -> ProcessLine
    where
        F: FnMut(&ProcessLine) -> bool,
        D: FnMut(&mut Self),
    {
        let deadline = Instant::now() + timeout;
        let mut next_drive = Instant::now();
        loop {
            let pending_len = self.pending.len();
            for _ in 0..pending_len {
                let event = self.pending.pop_front().expect("pending event exists");
                if predicate(&event) {
                    return event;
                }
                self.pending.push_back(event);
            }

            let now = Instant::now();
            if now >= next_drive {
                driver(self);
                next_drive = now + PROCESS_DRIVER_INTERVAL;
            }
            let now = Instant::now();
            assert!(
                now < deadline,
                "timed out waiting for process event; pending={:?}",
                self.pending
            );
            let remaining = deadline.saturating_duration_since(now);
            match self
                .stdout_rx
                .recv_timeout(remaining.min(PROCESS_DRIVER_INTERVAL))
            {
                Ok(event) if predicate(&event) => return event,
                Ok(event) => self.push_pending(event),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("process event channel disconnected")
                }
            }
        }
    }
}

/// Runs the process-per-node durable KV scenario under `root`.
///
/// # Panics
///
/// Panics when a child process cannot be spawned, when a process event does
/// not arrive before its deadline, or when the replicated KV invariant fails.
#[must_use]
pub fn run_process_demo_with_spawn(
    root: PathBuf,
    options: ScenarioOptions,
    spawn: ProcessSpawn,
) -> ScenarioReport {
    std::fs::create_dir_all(&root).expect("create process example directory");
    let mut cluster = ProcessCluster::open(root, spawn);

    let initial_leader = cluster.elect_node_one();
    cluster.propose_set(initial_leader, "alpha", "1");
    cluster.propose_set(initial_leader, "beta", "2");
    cluster.wait_applied(initial_leader, NodeId(2), "beta", "2");
    let alpha_read = cluster.linearizable_get(initial_leader, "alpha");

    let restarted_applied_floor = cluster.restart_node(NodeId(2));
    assert!(restarted_applied_floor > LogIndex::ZERO);
    assert!(cluster.wait_value(initial_leader, NodeId(2), "alpha", "1") >= restarted_applied_floor);

    cluster.kill_node(NodeId(3));
    cluster.propose_set(initial_leader, "gamma", "3");
    let snapshot_index = cluster.compact_snapshot(initial_leader);
    let restarted_three_floor = cluster.restart_node(NodeId(3));
    assert!(restarted_three_floor <= snapshot_index);
    assert!(cluster.wait_value(initial_leader, NodeId(3), "gamma", "3") >= snapshot_index);

    cluster.transfer_leadership(initial_leader, NodeId(2));
    let transferred_leader = NodeId(2);
    cluster.propose_set(transferred_leader, "delta", "4");
    assert_eq!(
        cluster.linearizable_get(transferred_leader, "delta"),
        Some("4".to_string())
    );

    let final_values = cluster.collect_values(transferred_leader);
    if options.verbose {
        println!(
            "replicated kv over tcp: leader {initial_leader} -> {transferred_leader}, read alpha={alpha_read:?}, snapshot {snapshot_index}, final {final_values:?}"
        );
    }
    let root = cluster.shutdown();
    if options.keep_dir {
        println!("kept process example directory: {}", root.display());
    } else {
        std::fs::remove_dir_all(&root).ok();
    }

    ScenarioReport {
        initial_leader,
        transferred_leader,
        alpha_read,
        final_values,
        snapshot_index,
        restarted_applied_floor,
    }
}

#[derive(Debug)]
struct PendingRead {
    key: String,
    read_index: LogIndex,
}

#[derive(Debug)]
struct ProcessReplica {
    node_id: NodeId,
    node: FileNode,
    transport: Arc<InsecureTcpTransport>,
    kv: BTreeMap<String, String>,
    applied: LogIndex,
    pending_reads: BTreeMap<u64, PendingRead>,
    node_dir: PathBuf,
}

impl ProcessReplica {
    fn open(root: &Path, node_id: NodeId) -> Self {
        let app = load_app_state(root, node_id);
        let (node, recovery_outputs) = open_node(root, node_id, app.applied);
        let transport = InsecureTcpTransport::bind("127.0.0.1:0", BTreeMap::new())
            .expect("process node binds TCP listener")
            .with_reconnect_backoff(ReconnectBackoff {
                max_attempts: 2,
                initial_delay: Duration::from_millis(5),
                max_delay: Duration::from_millis(25),
            });
        let mut replica = Self {
            node_id,
            node,
            transport: Arc::new(transport),
            kv: app.kv,
            applied: app.applied,
            pending_reads: BTreeMap::new(),
            node_dir: node_dir(root, node_id),
        };
        replica.handle_outputs(recovery_outputs);
        replica
    }

    fn step(&mut self, input: Input) {
        let outputs = self
            .node
            .step(input)
            .expect("durable process step succeeds");
        self.handle_outputs(outputs);
    }

    fn handle_outputs(&mut self, outputs: Vec<Output>) {
        for output in outputs {
            match output {
                Output::Send { to, message } => {
                    if let Err(error) = self.transport.send(to, &message) {
                        println!("SEND_ERROR {} {} {error}", self.node_id.0, to.0);
                    }
                }
                Output::Apply { index, payload, .. } => {
                    let command = std::str::from_utf8(payload.as_slice())
                        .expect("example commands are UTF-8");
                    let (key, value) = apply_set_with_parts(command, &mut self.kv);
                    self.applied = index;
                    persist_app_state(&self.node_dir, &self.kv, self.applied);
                    println!(
                        "APPLIED {} {} {} {}",
                        self.node_id.0, self.applied.0, key, value
                    );
                    self.flush_reads();
                }
                Output::ApplySnapshot { snapshot } => {
                    let payload = read_snapshot_payload(&self.node, &snapshot);
                    self.kv = decode_snapshot(&payload);
                    self.applied = snapshot.metadata.last_included_index;
                    persist_app_state(&self.node_dir, &self.kv, self.applied);
                    println!("SNAPSHOT_APPLIED {} {}", self.node_id.0, self.applied.0);
                    self.flush_reads();
                }
                Output::ReadIndexGranted {
                    read_id,
                    read_index,
                } => {
                    let request_id = read_id.0;
                    if let Some(read) = self.pending_reads.get_mut(&request_id) {
                        read.read_index = read_index;
                    }
                    self.flush_reads();
                }
                Output::RejectProposal { reason, .. } => {
                    println!("PROPOSAL_REJECTED {} {reason}", self.node_id.0);
                }
                Output::ReadIndexRejected { read_id, reason } => {
                    let request_id = read_id.0;
                    println!("READ_REJECTED {} {request_id} {reason}", self.node_id.0);
                    self.pending_reads.remove(&request_id);
                }
                Output::ReadIndexCanceled { read_id, reason } => {
                    let request_id = read_id.0;
                    println!("READ_CANCELED {} {request_id} {reason:?}", self.node_id.0);
                    self.pending_reads.remove(&request_id);
                }
                Output::LeadershipTransferRejected { target, reason } => {
                    println!("TRANSFER_REJECTED {} {} {reason}", self.node_id.0, target.0);
                }
                Output::LocalProposalAppended { .. }
                | Output::LocalProposalDropped { .. }
                | Output::StageSnapshotChunk { .. } => {}
                Output::SendSnapshotChunk { .. } => {
                    panic!("runtime should resolve snapshot chunk sends")
                }
            }
        }
        std::io::stdout().flush().expect("flush process events");
    }

    fn handle_command(&mut self, command: &str) -> bool {
        let mut parts = command.split_whitespace();
        match parts.next() {
            Some("PEERS") => {
                let peers = parts.map(parse_peer).collect();
                self.transport.set_peers(peers);
                println!("PEERS_OK {}", self.node_id.0);
            }
            Some("TICK") => self.step(Input::Tick),
            Some("PROPOSE") => {
                let key = parts.next().expect("proposal key");
                let value = parts.next().expect("proposal value");
                self.step(Input::ClientProposal {
                    payload: encode_set(key, value),
                });
            }
            Some("READ") => {
                let request_id = parts
                    .next()
                    .expect("read request id")
                    .parse()
                    .expect("read request id parses");
                let key = parts.next().expect("read key").to_string();
                self.pending_reads.insert(
                    request_id,
                    PendingRead {
                        key,
                        read_index: LogIndex(u64::MAX),
                    },
                );
                self.step(Input::ReadIndex {
                    read_id: ReadId(request_id),
                });
            }
            Some("TRANSFER") => {
                let target = parse_node_id(parts.next().expect("transfer target"));
                self.step(Input::TransferLeadership { target });
            }
            Some("SNAPSHOT") => {
                let boundary =
                    compact_kv_snapshot(self.node_id, &mut self.node, &self.kv, self.applied);
                println!("SNAPSHOT {} {}", self.node_id.0, boundary.0);
            }
            Some("ROLE") => {
                println!(
                    "ROLE {} {} {} {}",
                    self.node_id.0,
                    self.node.role(),
                    self.node.current_term().0,
                    self.applied.0
                );
            }
            Some("VALUE") => {
                let key = parts.next().expect("value key");
                println!(
                    "VALUE {} {} {} {}",
                    self.node_id.0,
                    key,
                    encode_value(self.kv.get(key)),
                    self.applied.0
                );
            }
            Some("STOP") => {
                println!("STOPPED {}", self.node_id.0);
                std::io::stdout().flush().expect("flush stop event");
                return false;
            }
            Some(other) => panic!("unknown process command {other:?}"),
            None => {}
        }
        std::io::stdout().flush().expect("flush process event");
        true
    }

    fn flush_reads(&mut self) {
        let ready: Vec<_> = self
            .pending_reads
            .iter()
            .filter_map(|(request_id, read)| {
                (self.applied >= read.read_index).then_some(*request_id)
            })
            .collect();
        for request_id in ready {
            let read = self
                .pending_reads
                .remove(&request_id)
                .expect("pending read exists");
            println!(
                "READ {} {} {} {} {}",
                self.node_id.0,
                request_id,
                read.key,
                encode_value(self.kv.get(&read.key)),
                read.read_index.0
            );
        }
    }
}

/// Runs one process-mode node from environment variables set by the harness.
///
/// # Panics
///
/// Panics when the required environment variables are absent or invalid, or
/// when the child node cannot open its durable state.
pub fn run_process_node_from_env() {
    let node_id = NodeId(
        std::env::var("RAFTER_KV_NODE_ID")
            .expect("RAFTER_KV_NODE_ID is set")
            .parse()
            .expect("node id parses"),
    );
    let root = PathBuf::from(std::env::var("RAFTER_KV_ROOT").expect("RAFTER_KV_ROOT is set"));
    run_process_node(&root, node_id);
}

#[derive(Debug)]
enum TcpInbound {
    Message { from: NodeId, message: Box<Message> },
    Error(String),
}

pub(crate) fn run_process_node(root: &Path, node_id: NodeId) {
    let mut replica = ProcessReplica::open(root, node_id);
    let local_addr = replica
        .transport
        .local_addr()
        .expect("process node local address");
    println!("READY {} {} {}", node_id.0, local_addr, replica.applied.0);
    std::io::stdout().flush().expect("flush ready event");

    let (tcp_tx, tcp_rx) = mpsc::channel();
    let transport = Arc::clone(&replica.transport);
    thread::spawn(move || loop {
        match transport.receive() {
            Ok(received) => tcp_tx
                .send(TcpInbound::Message {
                    from: received.from,
                    message: Box::new(received.message),
                })
                .expect("TCP receiver channel is open"),
            Err(error) => tcp_tx
                .send(TcpInbound::Error(error.to_string()))
                .expect("TCP receiver channel is open"),
        }
    });

    let (command_tx, command_rx) = mpsc::channel();
    thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let line = line.expect("read command line");
            command_tx.send(line).expect("command receiver is alive");
        }
    });

    loop {
        while let Ok(inbound) = tcp_rx.try_recv() {
            match inbound {
                TcpInbound::Message { from, message } => {
                    replica.step(Input::Message {
                        from,
                        message: *message,
                    });
                }
                TcpInbound::Error(error) => {
                    println!("RECEIVE_ERROR {} {error}", node_id.0);
                }
            }
        }
        match command_rx.recv_timeout(Duration::from_millis(10)) {
            Ok(command) => {
                if !replica.handle_command(&command) {
                    return;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return,
        }
    }
}
