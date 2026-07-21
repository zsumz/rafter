//! Producer-owned semantic matching for the exact lease-probe history operation.
//!
//! The parser binds one retained client invocation to its correlated terminal.

use std::collections::BTreeMap;

use edn_format::{parse_str, Keyword, Value};

const MAX_OPERATIONS: usize = 131_072;
const MAX_PENDING: usize = 4_096;
pub(in crate::producer) const MAX_LINE_BYTES: usize = 64 * 1024;

pub(in crate::producer) fn probe_completion_count(
    source: &str,
    client: &str,
    msg_id: u64,
) -> Result<u64, String> {
    let expected = format!("[rafter-lease-probe client={client} msg_id={msg_id} code=11]");
    let mut pending = BTreeMap::<Value, (Value, Value)>::new();
    let mut completions = 0;
    let mut last_index = None;
    for (operation_index, line) in source
        .lines()
        .filter(|line| !line.trim().is_empty())
        .enumerate()
    {
        if operation_index == MAX_OPERATIONS {
            return Err(format!(
                "Maelstrom history exceeds {MAX_OPERATIONS} operations"
            ));
        }
        if line.len() > MAX_LINE_BYTES {
            return Err(format!(
                "Maelstrom history operation exceeds {MAX_LINE_BYTES} bytes"
            ));
        }
        let parsed = parse_str(line).map_err(|error| format!("parse history EDN: {error}"))?;
        let operation = as_map(&parsed)?;
        let index = unsigned(field(operation, "index")?)?;
        if last_index.is_some_and(|previous| index <= previous) {
            return Err("Maelstrom history indices are not strictly ordered".to_owned());
        }
        last_index = Some(index);
        let process = field(operation, "process")?.clone();
        let function = field(operation, "f")?.clone();
        let operation_value = field(operation, "value")?.clone();
        let operation_type = keyword_name(field(operation, "type")?)?;
        if operation_type == "invoke" {
            if pending
                .insert(process, (function, operation_value))
                .is_some()
            {
                return Err("Maelstrom process invoked twice without a terminal".to_owned());
            }
            if pending.len() > MAX_PENDING {
                return Err(format!(
                    "Maelstrom history exceeds {MAX_PENDING} pending operations"
                ));
            }
            continue;
        }
        if !matches!(operation_type, "ok" | "fail" | "info") {
            return Err(format!(
                "unknown Maelstrom history operation type :{operation_type}"
            ));
        }
        let (invoked_function, invoked_value) = pending
            .remove(&process)
            .ok_or_else(|| "Maelstrom terminal has no preceding invoke".to_owned())?;
        if invoked_function != function
            || !operation_identity_matches(&function, &invoked_value, &operation_value)
        {
            return Err("Maelstrom terminal function does not match its invoke".to_owned());
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
                return Err(
                    "lease probe tag appeared outside its exact failed read completion".to_owned(),
                );
            }
            completions += 1;
        }
    }
    if !pending.is_empty() {
        return Err("Maelstrom history ended with an unterminated invoke".to_owned());
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

fn keyword_name(value: &Value) -> Result<&str, String> {
    match value {
        Value::Keyword(value) => Ok(value.name()),
        _ => Err(format!("expected EDN keyword, got {value}")),
    }
}

fn as_map(value: &Value) -> Result<&BTreeMap<Value, Value>, String> {
    match value {
        Value::Map(value) => Ok(value),
        _ => Err(format!("expected EDN map, got {value}")),
    }
}

fn field<'a>(map: &'a BTreeMap<Value, Value>, name: &str) -> Result<&'a Value, String> {
    map.get(&keyword(name))
        .ok_or_else(|| format!("Maelstrom result omitted :{name}"))
}

fn keyword(name: &str) -> Value {
    Value::Keyword(Keyword::from_name(name))
}

fn unsigned(value: &Value) -> Result<u64, String> {
    match value {
        Value::Integer(value) => {
            u64::try_from(*value).map_err(|_| format!("expected nonnegative integer, got {value}"))
        }
        _ => Err(format!("expected integer, got {value}")),
    }
}
