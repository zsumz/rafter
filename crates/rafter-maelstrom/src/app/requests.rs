//! The client-facing vocabulary: what a client may ask for, what the log
//! carries on its behalf, and what it is answered with.
//!
//! The match in [`parse_client_request`] is the single list of operations this
//! harness knows, so one it does not know becomes an error answer from a funnel
//! that recorded the request rather than a request dropped in silence.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::protocol::body_type;

use super::{
    canonical_key, ERROR_KEY_DOES_NOT_EXIST, ERROR_PRECONDITION_FAILED,
    ERROR_TEMPORARILY_UNAVAILABLE,
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Command {
    pub(crate) origin: String,
    pub(crate) client: String,
    pub(crate) in_reply_to: u64,
    pub(crate) request: ClientMutation,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(crate) enum ClientMutation {
    Write { key: Value, value: Value },
    Cas { key: Value, from: Value, to: Value },
}

#[derive(Clone, Debug)]
pub(crate) enum ClientRequest {
    Read { key: Value },
    Write { key: Value, value: Value },
    Cas { key: Value, from: Value, to: Value },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum ClientResult {
    ReadOk { value: Value },
    WriteOk,
    CasOk,
    Error { code: u64, text: String },
}

pub(crate) fn parse_client_request(body: &Value) -> Result<ClientRequest, ClientResult> {
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

pub(crate) fn apply_mutation(
    kv: &mut BTreeMap<String, Value>,
    request: &ClientMutation,
) -> ClientResult {
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

pub(crate) fn read_value(kv: &BTreeMap<String, Value>, key: &Value) -> ClientResult {
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
