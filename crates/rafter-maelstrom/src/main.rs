use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io::{BufRead, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use rafter::{Input, LogIndex, Message, NodeConfig, NodeId, Output, ReadId, Role};
use rafter_codec::{decode_message, encode_message};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const ELECTION_TIMEOUT_TICKS: u64 = 5;
const HEARTBEAT_INTERVAL_TICKS: u64 = 2;
const TICK_INTERVAL: Duration = Duration::from_millis(50);
const ERROR_TEMPORARILY_UNAVAILABLE: u64 = 11;
const ERROR_KEY_DOES_NOT_EXIST: u64 = 20;
const ERROR_PRECONDITION_FAILED: u64 = 22;

type FileNode = DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Envelope {
    src: String,
    dest: String,
    body: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedApp {
    applied: u64,
    kv: BTreeMap<String, Value>,
}

#[derive(Clone, Debug)]
struct AppState {
    applied: LogIndex,
    kv: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct Command {
    origin: String,
    client: String,
    in_reply_to: u64,
    request: ClientMutation,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum ClientMutation {
    Write { key: Value, value: Value },
    Cas { key: Value, from: Value, to: Value },
}

#[derive(Clone, Debug)]
enum ClientRequest {
    Read { key: Value },
    Write { key: Value, value: Value },
    Cas { key: Value, from: Value, to: Value },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum ClientResult {
    ReadOk { value: Value },
    WriteOk,
    CasOk,
    Error { code: u64, text: String },
}

#[derive(Clone, Debug)]
struct PendingRead {
    origin: String,
    client: String,
    in_reply_to: u64,
    key: Value,
    read_index: LogIndex,
}

#[derive(Debug)]
struct InitializedNode {
    name: String,
    node: FileNode,
    root: PathBuf,
    app: AppState,
    name_to_id: BTreeMap<String, NodeId>,
    id_to_name: BTreeMap<NodeId, String>,
    known_leader: Option<NodeId>,
    pending_reads: BTreeMap<u64, PendingRead>,
    completed_replies: BTreeSet<(String, u64)>,
    next_msg_id: u64,
    next_read_id: u64,
    last_reported_role: Role,
}

#[derive(Debug, Default)]
struct MaelstromNode {
    initialized: Option<InitializedNode>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("rafter-maelstrom failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let stdin_rx = spawn_stdin_reader();
    let mut node = MaelstromNode::default();
    let mut last_tick = Instant::now();

    loop {
        match stdin_rx.recv_timeout(TICK_INTERVAL / 5) {
            Ok(line) => node.handle_line(&line)?,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        }

        if node.is_initialized() && last_tick.elapsed() >= TICK_INTERVAL {
            node.tick();
            last_tick = Instant::now();
        }
    }
}

fn spawn_stdin_reader() -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else {
                break;
            };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

impl MaelstromNode {
    fn is_initialized(&self) -> bool {
        self.initialized.is_some()
    }

    fn tick(&mut self) {
        if let Some(node) = self.initialized.as_mut() {
            node.step(Input::Tick);
        }
    }

    fn handle_line(&mut self, line: &str) -> Result<(), Box<dyn Error>> {
        if line.trim().is_empty() {
            return Ok(());
        }
        let envelope: Envelope = serde_json::from_str(line)?;
        if body_type(&envelope.body) == Some("init") {
            self.initialize(&envelope)?;
            return Ok(());
        }
        let Some(node) = self.initialized.as_mut() else {
            return Ok(());
        };
        node.handle_envelope(envelope);
        Ok(())
    }

    fn initialize(&mut self, envelope: &Envelope) -> Result<(), Box<dyn Error>> {
        let node_name = required_str(&envelope.body, "node_id")?.to_string();
        let node_names = required_array(&envelope.body, "node_ids")?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(ToString::to_string)
                    .ok_or("node_ids must contain strings")
            })
            .collect::<Result<Vec<_>, _>>()?;
        let name_to_id = node_id_map(&node_names);
        let id_to_name = name_to_id
            .iter()
            .map(|(name, node_id)| (*node_id, name.clone()))
            .collect::<BTreeMap<_, _>>();
        let node_id = *name_to_id.get(&node_name).ok_or("node_id must be listed")?;
        let peers = name_to_id
            .values()
            .copied()
            .filter(|peer| *peer != node_id)
            .collect();
        let root = node_root(&node_name);
        std::fs::create_dir_all(&root)?;
        let app = load_app_state(&root)?;
        let node = open_node(&root, node_id, peers, app.applied)?;
        let last_reported_role = node.role();

        let initialized = InitializedNode {
            name: node_name,
            node,
            root,
            app,
            name_to_id,
            id_to_name,
            known_leader: None,
            pending_reads: BTreeMap::new(),
            completed_replies: BTreeSet::new(),
            next_msg_id: 1,
            next_read_id: 1,
            last_reported_role,
        };
        initialized.emit(
            &envelope.src,
            json!({
                "type": "init_ok",
                "in_reply_to": required_u64(&envelope.body, "msg_id")?,
            }),
        );
        self.initialized = Some(initialized);
        Ok(())
    }
}

impl InitializedNode {
    fn handle_envelope(&mut self, envelope: Envelope) {
        match body_type(&envelope.body) {
            Some("raft") => self.handle_raft(&envelope),
            Some("client_forward") => self.handle_forward(envelope),
            Some("client_result") => self.handle_client_result(&envelope),
            Some("read" | "write" | "cas") => self.handle_client(envelope),
            Some(other) => eprintln!("ignoring unsupported Maelstrom message type {other:?}"),
            None => eprintln!("ignoring Maelstrom message without body.type"),
        }
    }

    fn handle_raft(&mut self, envelope: &Envelope) {
        let Some(from) = self.name_to_id.get(&envelope.src).copied() else {
            eprintln!("ignoring raft message from unknown node {}", envelope.src);
            return;
        };
        let Some(frame) = envelope.body.get("frame").and_then(Value::as_str) else {
            eprintln!("ignoring raft message without frame");
            return;
        };
        let bytes = match decode_hex(frame) {
            Ok(bytes) => bytes,
            Err(error) => {
                eprintln!("ignoring raft message with invalid hex: {error}");
                return;
            }
        };
        match decode_message(&bytes) {
            Ok(message) => {
                self.observe_leader(&message);
                self.step(Input::Message { from, message });
            }
            Err(error) => eprintln!("ignoring raft message with invalid frame: {error}"),
        }
    }

    fn handle_forward(&mut self, envelope: Envelope) {
        let origin = envelope.src;
        let Some(client) = envelope.body.get("client").and_then(Value::as_str) else {
            return;
        };
        let Some(in_reply_to) = envelope.body.get("in_reply_to").and_then(Value::as_u64) else {
            return;
        };
        let Some(request) = envelope.body.get("request").cloned() else {
            return;
        };
        self.handle_client_request(origin, client.to_string(), in_reply_to, &request);
    }

    fn handle_client_result(&mut self, envelope: &Envelope) {
        let Some(client) = envelope.body.get("client").and_then(Value::as_str) else {
            return;
        };
        let Some(in_reply_to) = envelope.body.get("in_reply_to").and_then(Value::as_u64) else {
            return;
        };
        let Some(result) = envelope.body.get("result") else {
            return;
        };
        let result = match serde_json::from_value(result.clone()) {
            Ok(result) => result,
            Err(error) => {
                eprintln!("ignoring invalid client_result: {error}");
                return;
            }
        };
        self.reply_to_client(client, in_reply_to, result);
    }

    fn handle_client(&mut self, envelope: Envelope) {
        let Some(in_reply_to) = envelope.body.get("msg_id").and_then(Value::as_u64) else {
            return;
        };
        self.handle_client_request(self.name.clone(), envelope.src, in_reply_to, &envelope.body);
    }

    fn handle_client_request(
        &mut self,
        origin: String,
        client: String,
        in_reply_to: u64,
        body: &Value,
    ) {
        let request = match parse_client_request(body) {
            Ok(request) => request,
            Err(result) => {
                self.deliver_result(&origin, &client, in_reply_to, result);
                return;
            }
        };
        if self.node.role() != Role::Leader {
            self.forward_or_reply(&origin, &client, in_reply_to, body);
            return;
        }
        self.known_leader = Some(self.node.id());
        match request {
            ClientRequest::Read { key } => self.start_read(origin, client, in_reply_to, key),
            ClientRequest::Write { key, value } => {
                self.propose(
                    origin,
                    client,
                    in_reply_to,
                    ClientMutation::Write { key, value },
                );
            }
            ClientRequest::Cas { key, from, to } => {
                self.propose(
                    origin,
                    client,
                    in_reply_to,
                    ClientMutation::Cas { key, from, to },
                );
            }
        }
    }

    fn forward_or_reply(&mut self, origin: &str, client: &str, in_reply_to: u64, body: &Value) {
        if let Some(leader) = self.known_leader.filter(|leader| *leader != self.node.id()) {
            self.send_to_node(
                leader,
                json!({
                    "type": "client_forward",
                    "client": client,
                    "in_reply_to": in_reply_to,
                    "request": body,
                }),
            );
        } else {
            self.deliver_result(
                origin,
                client,
                in_reply_to,
                ClientResult::Error {
                    code: ERROR_TEMPORARILY_UNAVAILABLE,
                    text: "no Raft leader known yet".to_string(),
                },
            );
        }
    }

    fn start_read(&mut self, origin: String, client: String, in_reply_to: u64, key: Value) {
        let request_id = self.next_read_id;
        self.next_read_id += 1;
        self.pending_reads.insert(
            request_id,
            PendingRead {
                origin,
                client,
                in_reply_to,
                key,
                read_index: LogIndex(u64::MAX),
            },
        );
        self.step(Input::ReadIndex {
            read_id: ReadId(request_id),
        });
    }

    fn propose(
        &mut self,
        origin: String,
        client: String,
        in_reply_to: u64,
        request: ClientMutation,
    ) {
        let command = Command {
            origin,
            client,
            in_reply_to,
            request,
        };
        let payload = serde_json::to_vec(&command).expect("command serializes");
        self.step(Input::ClientProposal { payload });
    }

    fn step(&mut self, input: Input) {
        let outputs = match self.node.step(input) {
            Ok(outputs) => outputs,
            Err(error) => {
                eprintln!("runtime step failed: {error}");
                return;
            }
        };
        self.report_role_transition();
        self.handle_outputs(outputs);
    }

    fn report_role_transition(&mut self) {
        let role = self.node.role();
        if role == self.last_reported_role {
            return;
        }
        self.last_reported_role = role;
        eprintln!(
            "rafter-maelstrom role node={} role={} term={}",
            self.name,
            role,
            self.node.current_term()
        );
    }

    fn handle_outputs(&mut self, outputs: Vec<Output>) {
        for output in outputs {
            match output {
                Output::Send { to, message } => self.send_raft(to, &message),
                Output::Apply { index, payload, .. } => {
                    self.apply_command(index, payload.as_slice());
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
                Output::ReadIndexRejected { read_id, reason } => {
                    let request_id = read_id.0;
                    if let Some(read) = self.pending_reads.remove(&request_id) {
                        self.deliver_result(
                            &read.origin,
                            &read.client,
                            read.in_reply_to,
                            ClientResult::Error {
                                code: ERROR_TEMPORARILY_UNAVAILABLE,
                                text: reason.to_string(),
                            },
                        );
                    }
                }
                Output::ReadIndexCanceled { read_id, reason } => {
                    let request_id = read_id.0;
                    if let Some(read) = self.pending_reads.remove(&request_id) {
                        self.deliver_result(
                            &read.origin,
                            &read.client,
                            read.in_reply_to,
                            ClientResult::Error {
                                code: ERROR_TEMPORARILY_UNAVAILABLE,
                                text: format!("{reason:?}"),
                            },
                        );
                    }
                }
                Output::RejectProposal { reason, .. } => eprintln!("proposal rejected: {reason}"),
                Output::LeadershipTransferRejected { target, reason } => {
                    eprintln!("leadership transfer to {target} rejected: {reason}");
                }
                Output::LocalProposalAppended { .. }
                | Output::LocalProposalDropped { .. }
                | Output::ApplySnapshot { .. }
                | Output::StageSnapshotChunk { .. }
                | Output::SendSnapshotChunk { .. } => {}
            }
        }
    }

    fn apply_command(&mut self, index: LogIndex, payload: &[u8]) {
        let command: Command = match serde_json::from_slice(payload) {
            Ok(command) => command,
            Err(error) => {
                eprintln!("ignoring invalid committed command: {error}");
                return;
            }
        };
        let result = apply_mutation(&mut self.app.kv, &command.request);
        self.app.applied = index;
        if let Err(error) = persist_app_state(&self.root, &self.app) {
            eprintln!("failed to persist app state: {error}");
            return;
        }
        self.deliver_result(
            &command.origin,
            &command.client,
            command.in_reply_to,
            result,
        );
        self.flush_reads();
    }

    fn flush_reads(&mut self) {
        let ready = self
            .pending_reads
            .iter()
            .filter_map(|(request_id, read)| {
                (self.app.applied >= read.read_index).then_some(*request_id)
            })
            .collect::<Vec<_>>();
        for request_id in ready {
            let read = self
                .pending_reads
                .remove(&request_id)
                .expect("pending read exists");
            let result = read_value(&self.app.kv, &read.key);
            self.deliver_result(&read.origin, &read.client, read.in_reply_to, result);
        }
    }

    fn send_raft(&mut self, to: NodeId, message: &Message) {
        let frame = match encode_message(message) {
            Ok(frame) => encode_hex(&frame),
            Err(error) => {
                eprintln!("failed to encode raft message: {error}");
                return;
            }
        };
        self.send_to_node(to, json!({ "type": "raft", "frame": frame }));
    }

    fn send_to_node(&mut self, to: NodeId, body: Value) {
        if let Some(dest) = self.id_to_name.get(&to).cloned() {
            self.emit(&dest, body);
        }
    }

    fn deliver_result(
        &mut self,
        origin: &str,
        client: &str,
        in_reply_to: u64,
        result: ClientResult,
    ) {
        if origin == self.name {
            self.reply_to_client(client, in_reply_to, result);
        } else if self.node.role() == Role::Leader {
            self.emit(
                origin,
                json!({
                    "type": "client_result",
                    "client": client,
                    "in_reply_to": in_reply_to,
                    "result": result,
                }),
            );
        }
    }

    fn reply_to_client(&mut self, client: &str, in_reply_to: u64, result: ClientResult) {
        if !self
            .completed_replies
            .insert((client.to_string(), in_reply_to))
        {
            return;
        }
        let body = self.result_body(in_reply_to, result);
        self.emit(client, body);
    }

    fn result_body(&mut self, in_reply_to: u64, result: ClientResult) -> Value {
        let msg_id = self.next_msg_id;
        self.next_msg_id += 1;
        match result {
            ClientResult::ReadOk { value } => {
                json!({"type": "read_ok", "msg_id": msg_id, "in_reply_to": in_reply_to, "value": value})
            }
            ClientResult::WriteOk => {
                json!({"type": "write_ok", "msg_id": msg_id, "in_reply_to": in_reply_to})
            }
            ClientResult::CasOk => {
                json!({"type": "cas_ok", "msg_id": msg_id, "in_reply_to": in_reply_to})
            }
            ClientResult::Error { code, text } => {
                json!({"type": "error", "msg_id": msg_id, "in_reply_to": in_reply_to, "code": code, "text": text})
            }
        }
    }

    fn emit(&self, dest: &str, body: Value) {
        let envelope = Envelope {
            src: self.name.clone(),
            dest: dest.to_string(),
            body,
        };
        println!(
            "{}",
            serde_json::to_string(&envelope).expect("envelope serializes")
        );
        std::io::stdout().flush().expect("flush Maelstrom message");
    }

    fn observe_leader(&mut self, message: &Message) {
        match message {
            Message::AppendEntries(request) => self.known_leader = Some(request.leader_id),
            Message::InstallSnapshot(request) => self.known_leader = Some(request.leader_id),
            Message::InstallSnapshotChunk(request) => self.known_leader = Some(request.leader_id),
            Message::TimeoutNow(request) => self.known_leader = Some(request.leader_id),
            Message::RequestVote(_)
            | Message::RequestVoteResponse(_)
            | Message::PreVote(_)
            | Message::PreVoteResponse(_)
            | Message::AppendEntriesResponse(_)
            | Message::InstallSnapshotResponse(_) => {}
        }
    }
}

fn open_node(
    root: &Path,
    node_id: NodeId,
    peers: Vec<NodeId>,
    applied_through: LogIndex,
) -> Result<FileNode, Box<dyn Error>> {
    let raft_dir = root.join("raft");
    std::fs::create_dir_all(&raft_dir)?;
    let (hard_state, log, snapshots) = FileRaftNodeStores::open(&raft_dir)?.into_parts();
    let config = NodeConfig::new(node_id, peers, ELECTION_TIMEOUT_TICKS)?
        .with_heartbeat_interval_ticks(HEARTBEAT_INTERVAL_TICKS);
    Ok(
        DurableRaftNode::with_storage_and_snapshot_store_applied_through(
            config,
            hard_state,
            log,
            snapshots,
            applied_through,
        )?,
    )
}

fn node_root(node_name: &str) -> PathBuf {
    std::env::var_os("RAFTER_MAELSTROM_ROOT").map_or_else(
        || {
            std::env::temp_dir()
                .join("rafter-maelstrom")
                .join(node_name)
        },
        |root| PathBuf::from(root).join(node_name),
    )
}

fn load_app_state(root: &Path) -> Result<AppState, Box<dyn Error>> {
    let path = root.join("app.json");
    if !path.exists() {
        return Ok(AppState {
            applied: LogIndex::ZERO,
            kv: BTreeMap::new(),
        });
    }
    let persisted: PersistedApp = serde_json::from_slice(&std::fs::read(path)?)?;
    Ok(AppState {
        applied: LogIndex(persisted.applied),
        kv: persisted.kv,
    })
}

fn persist_app_state(root: &Path, app: &AppState) -> Result<(), Box<dyn Error>> {
    std::fs::create_dir_all(root)?;
    let tmp = root.join("app.json.tmp");
    let path = root.join("app.json");
    let persisted = PersistedApp {
        applied: app.applied.0,
        kv: app.kv.clone(),
    };
    std::fs::write(&tmp, serde_json::to_vec(&persisted)?)?;
    std::fs::rename(tmp, path)?;
    Ok(())
}

fn node_id_map(node_names: &[String]) -> BTreeMap<String, NodeId> {
    node_names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            let protocol_id = u64::try_from(index + 1).expect("node count fits u64");
            (name.clone(), NodeId(protocol_id))
        })
        .collect()
}

fn parse_client_request(body: &Value) -> Result<ClientRequest, ClientResult> {
    match body_type(body) {
        Some("read") => Ok(ClientRequest::Read {
            key: required_value(body, "key")?,
        }),
        Some("write") => Ok(ClientRequest::Write {
            key: required_value(body, "key")?,
            value: required_value(body, "value")?,
        }),
        Some("cas") => Ok(ClientRequest::Cas {
            key: required_value(body, "key")?,
            from: required_value(body, "from")?,
            to: required_value(body, "to")?,
        }),
        Some(other) => Err(ClientResult::Error {
            code: ERROR_TEMPORARILY_UNAVAILABLE,
            text: format!("unsupported request type {other}"),
        }),
        None => Err(ClientResult::Error {
            code: ERROR_TEMPORARILY_UNAVAILABLE,
            text: "request body missing type".to_string(),
        }),
    }
}

fn required_value(body: &Value, field: &str) -> Result<Value, ClientResult> {
    body.get(field).cloned().ok_or_else(|| ClientResult::Error {
        code: ERROR_TEMPORARILY_UNAVAILABLE,
        text: format!("request missing {field}"),
    })
}

fn apply_mutation(kv: &mut BTreeMap<String, Value>, request: &ClientMutation) -> ClientResult {
    match request {
        ClientMutation::Write { key, value } => {
            kv.insert(canonical_key(key), value.clone());
            ClientResult::WriteOk
        }
        ClientMutation::Cas { key, from, to } => {
            let key = canonical_key(key);
            let Some(current) = kv.get_mut(&key) else {
                return ClientResult::Error {
                    code: ERROR_KEY_DOES_NOT_EXIST,
                    text: "key does not exist".to_string(),
                };
            };
            if current != from {
                return ClientResult::Error {
                    code: ERROR_PRECONDITION_FAILED,
                    text: "current value did not match CAS precondition".to_string(),
                };
            }
            *current = to.clone();
            ClientResult::CasOk
        }
    }
}

fn read_value(kv: &BTreeMap<String, Value>, key: &Value) -> ClientResult {
    kv.get(&canonical_key(key)).map_or_else(
        || ClientResult::Error {
            code: ERROR_KEY_DOES_NOT_EXIST,
            text: "key does not exist".to_string(),
        },
        |value| ClientResult::ReadOk {
            value: value.clone(),
        },
    )
}

fn canonical_key(key: &Value) -> String {
    serde_json::to_string(key).expect("JSON value serializes")
}

fn body_type(body: &Value) -> Option<&str> {
    body.get("type").and_then(Value::as_str)
}

fn required_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, Box<dyn Error>> {
    body.get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| format!("body.{field} must be a string").into())
}

fn required_array<'a>(body: &'a Value, field: &str) -> Result<&'a Vec<Value>, Box<dyn Error>> {
    body.get(field)
        .and_then(Value::as_array)
        .ok_or_else(|| format!("body.{field} must be an array").into())
}

fn required_u64(body: &Value, field: &str) -> Result<u64, Box<dyn Error>> {
    body.get(field)
        .and_then(Value::as_u64)
        .ok_or_else(|| format!("body.{field} must be an unsigned integer").into())
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn decode_hex(encoded: &str) -> Result<Vec<u8>, String> {
    let bytes = encoded.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err("hex string has odd length".to_string());
    }
    bytes
        .chunks_exact(2)
        .map(|pair| {
            let high = hex_value(pair[0])?;
            let low = hex_value(pair[1])?;
            Ok((high << 4) | low)
        })
        .collect()
}

fn hex_value(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(format!("invalid hex byte {byte}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lin_kv_write_read_and_cas_apply_in_log_order() {
        let mut kv = BTreeMap::new();
        assert!(matches!(
            apply_mutation(
                &mut kv,
                &ClientMutation::Write {
                    key: json!(1),
                    value: json!(7),
                },
            ),
            ClientResult::WriteOk
        ));
        assert_eq!(
            read_value(&kv, &json!(1)),
            ClientResult::ReadOk { value: json!(7) }
        );
        assert!(matches!(
            apply_mutation(
                &mut kv,
                &ClientMutation::Cas {
                    key: json!(1),
                    from: json!(7),
                    to: json!(8),
                },
            ),
            ClientResult::CasOk
        ));
        assert_eq!(
            read_value(&kv, &json!(1)),
            ClientResult::ReadOk { value: json!(8) }
        );
    }

    #[test]
    fn lin_kv_cas_reports_maelstrom_error_codes() {
        let mut kv = BTreeMap::new();
        assert!(matches!(
            apply_mutation(
                &mut kv,
                &ClientMutation::Cas {
                    key: json!("missing"),
                    from: json!(1),
                    to: json!(2),
                },
            ),
            ClientResult::Error {
                code: ERROR_KEY_DOES_NOT_EXIST,
                ..
            }
        ));
        apply_mutation(
            &mut kv,
            &ClientMutation::Write {
                key: json!("x"),
                value: json!(1),
            },
        );
        assert!(matches!(
            apply_mutation(
                &mut kv,
                &ClientMutation::Cas {
                    key: json!("x"),
                    from: json!(2),
                    to: json!(3),
                },
            ),
            ClientResult::Error {
                code: ERROR_PRECONDITION_FAILED,
                ..
            }
        ));
    }

    #[test]
    fn raft_frames_round_trip_through_hex_for_maelstrom_body() {
        let message = Message::RequestVote(rafter::RequestVote {
            term: rafter::Term(3),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(5),
            last_log_term: rafter::Term(2),
        });
        let frame = encode_message(&message).expect("message encodes");
        let decoded_frame = decode_hex(&encode_hex(&frame)).expect("hex decodes");
        let decoded = decode_message(&decoded_frame).expect("message decodes");
        assert_eq!(decoded, message);
    }

    #[test]
    fn node_ids_follow_maelstrom_init_order() {
        let map = node_id_map(&["n3".to_string(), "n1".to_string(), "n2".to_string()]);
        assert_eq!(map["n3"], NodeId(1));
        assert_eq!(map["n1"], NodeId(2));
        assert_eq!(map["n2"], NodeId(3));
    }
}
