use rafter::{Input, LogIndex, ReadId, Role};
use serde_json::{json, Value};

use crate::{
    app::{
        parse_client_request, read_value, ClientMutation, ClientRequest, ClientResult, Command,
        ERROR_TEMPORARILY_UNAVAILABLE,
    },
    protocol::Envelope,
    InitializedNode, PendingRead,
};

impl InitializedNode {
    pub(crate) fn handle_forward(&mut self, envelope: Envelope) {
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

    pub(crate) fn handle_client_result(&mut self, envelope: &Envelope) {
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

    pub(crate) fn handle_client(&mut self, envelope: Envelope) {
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

    pub(crate) fn flush_reads(&mut self) {
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

    pub(crate) fn deliver_result(
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
}
