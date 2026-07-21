//! Bounded, ordered interpretation of Maelstrom client-history operations.

use std::collections::BTreeMap;

use edn_format::{parse_str, Keyword, Value};

use crate::verification::AggregateError;

pub(super) const LIMITS: Limits = Limits {
    operations: 131_072,
    pending: 4_096,
    line_bytes: 64 * 1024,
};

#[derive(Clone, Copy)]
pub(crate) struct Limits {
    pub(crate) operations: usize,
    pub(crate) pending: usize,
    pub(crate) line_bytes: usize,
}

pub(crate) fn completion_count(
    source: &str,
    client: &str,
    message: u64,
) -> Result<u64, AggregateError> {
    completion_count_with_limits(source, client, message, LIMITS)
}

pub(crate) fn completion_count_with_limits(
    source: &str,
    client: &str,
    message: u64,
    limits: Limits,
) -> Result<u64, AggregateError> {
    let expected = format!("[rafter-lease-probe client={client} msg_id={message} code=11]");
    let mut pending = BTreeMap::<Value, (Value, Value)>::new();
    let mut completions = 0;
    let mut last_index = None;
    for (operation_index, line) in source
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
    {
        if operation_index == limits.operations {
            return Err(error(format!(
                "Maelstrom history exceeds {} operations",
                limits.operations
            )));
        }
        if line.len() > limits.line_bytes {
            return Err(error(format!(
                "Maelstrom history operation exceeds {} bytes",
                limits.line_bytes
            )));
        }
        let parsed = parse_str(line)
            .map_err(|parse_error| error(format!("parse Maelstrom history: {parse_error}")))?;
        let Value::Map(operation) = parsed else {
            return Err(error("Maelstrom history operation is not an EDN map"));
        };
        let index = unsigned(field(&operation, "index")?)?;
        if last_index.is_some_and(|previous| index <= previous) {
            return Err(error("Maelstrom history indices are not strictly ordered"));
        }
        last_index = Some(index);
        let process = field(&operation, "process")?.clone();
        let function = field(&operation, "f")?.clone();
        let operation_value = field(&operation, "value")?.clone();
        let operation_type = keyword_name(field(&operation, "type")?)?;
        if operation_type == "invoke" {
            if pending
                .insert(process, (function, operation_value))
                .is_some()
            {
                return Err(error("Maelstrom process invoked twice without a terminal"));
            }
            if pending.len() > limits.pending {
                return Err(error(format!(
                    "Maelstrom history exceeds {} pending operations",
                    limits.pending
                )));
            }
            continue;
        }
        if !matches!(operation_type, "ok" | "fail" | "info") {
            return Err(error(format!(
                "unknown Maelstrom history operation type :{operation_type}"
            )));
        }
        let (invoked_function, invoked_value) = pending
            .remove(&process)
            .ok_or_else(|| error("Maelstrom terminal has no preceding invoke"))?;
        if invoked_function != function
            || !operation_identity_matches(&function, &invoked_value, &operation_value)
        {
            return Err(error(
                "Maelstrom terminal function does not match its invoke",
            ));
        }
        let tagged = match operation.get(&keyword("error")) {
            Some(Value::Vector(error)) if error.len() == 2 => {
                matches!(&error[0], Value::Keyword(value) if value.name() == "temporarily-unavailable")
                    && matches!(&error[1], Value::String(text) if text.ends_with(&expected))
            }
            _ => false,
        };
        if tagged {
            if operation_type != "fail"
                || !matches!(&function, Value::Keyword(value) if value.name() == "read")
                || operation_value != invoked_value
            {
                return Err(error(
                    "lease probe tag appeared outside its exact failed read completion",
                ));
            }
            completions += 1;
        }
    }
    if !pending.is_empty() {
        return Err(error("Maelstrom history ended with an unterminated invoke"));
    }
    Ok(completions)
}

fn operation_identity_matches(function: &Value, invoked: &Value, terminal: &Value) -> bool {
    if matches!(function, Value::Keyword(value) if value.name() == "read") {
        match (invoked, terminal) {
            (Value::Vector(invoked), Value::Vector(terminal)) => {
                invoked.first() == terminal.first()
            }
            _ => invoked == terminal,
        }
    } else {
        invoked == terminal
    }
}

fn keyword(name: &str) -> Value {
    Value::Keyword(Keyword::from_name(name))
}

fn field<'a>(
    operation: &'a BTreeMap<Value, Value>,
    name: &str,
) -> Result<&'a Value, AggregateError> {
    operation
        .get(&keyword(name))
        .ok_or_else(|| error(format!("Maelstrom history operation omitted :{name}")))
}

fn keyword_name(value: &Value) -> Result<&str, AggregateError> {
    match value {
        Value::Keyword(value) => Ok(value.name()),
        _ => Err(error(format!("expected EDN keyword, got {value}"))),
    }
}

fn unsigned(value: &Value) -> Result<u64, AggregateError> {
    match value {
        Value::Integer(value) => u64::try_from(*value)
            .map_err(|_| error(format!("expected nonnegative history index, got {value}"))),
        _ => Err(error(format!(
            "expected integer history index, got {value}"
        ))),
    }
}

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}
