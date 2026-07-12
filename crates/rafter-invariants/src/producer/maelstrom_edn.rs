use std::collections::BTreeMap;

use edn_format::{parse_str, Keyword, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Validity {
    Valid,
    Invalid,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct MaelstromSummary {
    pub validity: Validity,
    pub linearizability: Validity,
    pub operation_count: u64,
    pub ok_count: u64,
    pub read_ok: u64,
    pub write_ok: u64,
    pub cas_ok: u64,
}

pub(crate) fn parse(source: &str) -> Result<MaelstromSummary, String> {
    let parsed = parse_str(source).map_err(|error| format!("parse EDN: {error}"))?;
    let root = as_map(&parsed)?;
    let top = validity(field(root, "valid?")?)?;
    let workload = as_map(field(root, "workload")?)?;
    let workload_validity = validity(field(workload, "valid?")?)?;
    let failures_empty =
        matches!(field(workload, "failures")?, Value::Vector(values) if values.is_empty());
    let linearizability = workload
        .get(&keyword("results"))
        .map_or(Ok(Validity::Unknown), linearizability_validity)?;
    let stats = as_map(field(root, "stats")?)?;
    let by_function = as_map(field(stats, "by-f")?)?;
    let read_ok = operation_ok_count(by_function, "read")?;
    let write_ok = operation_ok_count(by_function, "write")?;
    let cas_ok = operation_ok_count(by_function, "cas")?;
    let validity = combine([
        top,
        workload_validity,
        linearizability,
        if failures_empty {
            Validity::Valid
        } else {
            Validity::Invalid
        },
    ]);
    Ok(MaelstromSummary {
        validity,
        linearizability,
        operation_count: unsigned(field(stats, "count")?)?,
        ok_count: unsigned(field(stats, "ok-count")?)?,
        read_ok,
        write_ok,
        cas_ok,
    })
}

pub(crate) fn lease_probe_completion_count(
    source: &str,
    client: &str,
    msg_id: u64,
) -> Result<u64, String> {
    let expected = format!("[rafter-lease-probe client={client} msg_id={msg_id} code=11]");
    let mut pending = BTreeMap::<Value, (Value, Value)>::new();
    let mut completions = 0;
    let mut last_index = None;
    for line in source.lines().filter(|line| !line.trim().is_empty()) {
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

fn linearizability_validity(value: &Value) -> Result<Validity, String> {
    let results = as_map(value)?;
    if results.is_empty() {
        return Ok(Validity::Unknown);
    }
    results
        .values()
        .map(|result| {
            let result = as_map(result)?;
            let linearizable = as_map(field(result, "linearizable")?)?;
            validity(field(linearizable, "valid?")?)
        })
        .try_fold(Validity::Valid, |current, next| {
            Ok(combine([current, next?]))
        })
}

fn operation_ok_count(by_function: &BTreeMap<Value, Value>, name: &str) -> Result<u64, String> {
    let operation = as_map(
        by_function
            .get(&keyword(name))
            .ok_or_else(|| format!("Maelstrom stats omitted {name}"))?,
    )?;
    unsigned(field(operation, "ok-count")?)
}

fn validity(value: &Value) -> Result<Validity, String> {
    match value {
        Value::Boolean(true) => Ok(Validity::Valid),
        Value::Boolean(false) => Ok(Validity::Invalid),
        Value::Keyword(value) if value.name() == "unknown" => Ok(Validity::Unknown),
        _ => Err(format!(
            "expected boolean or :unknown validity, got {value}"
        )),
    }
}

fn combine<const N: usize>(values: [Validity; N]) -> Validity {
    if values.contains(&Validity::Invalid) {
        Validity::Invalid
    } else if values.contains(&Validity::Unknown) {
        Validity::Unknown
    } else {
        Validity::Valid
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

#[cfg(test)]
mod tests {
    use super::{lease_probe_completion_count, parse, MaelstromSummary, Validity};

    const VALID: &str = r"{
      :stats {:count 9 :ok-count 6 :by-f {
        :read {:ok-count 2} :write {:ok-count 3} :cas {:ok-count 1}}}
      :workload {:valid? true :failures [] :results {
        0 {:linearizable {:valid? true
                          :model #knossos.model.CASRegister{:value 2}}}}}
      :valid? true}";

    #[test]
    fn parses_structural_linearizability_and_operation_counts() {
        assert_eq!(
            parse(VALID),
            Ok(MaelstromSummary {
                validity: Validity::Valid,
                linearizability: Validity::Valid,
                operation_count: 9,
                ok_count: 6,
                read_ok: 2,
                write_ok: 3,
                cas_ok: 1,
            })
        );
    }

    #[test]
    fn false_or_unknown_checker_results_never_pass() {
        assert_eq!(
            parse(&VALID.replace(":valid? true}", ":valid? false}"))
                .map(|summary| summary.validity),
            Ok(Validity::Invalid)
        );
        assert_eq!(
            parse(&VALID.replace(
                ":linearizable {:valid? true\n",
                ":linearizable {:valid? :unknown\n"
            ))
            .map(|summary| summary.validity),
            Ok(Validity::Unknown)
        );
    }

    #[test]
    fn non_linearizable_history_is_distinct_from_other_invalid_results() {
        let non_linearizable = parse(&VALID.replace(
            ":linearizable {:valid? true\n",
            ":linearizable {:valid? false\n",
        ))
        .map(|summary| (summary.validity, summary.linearizability));
        assert_eq!(non_linearizable, Ok((Validity::Invalid, Validity::Invalid)));

        let workload_failure = parse(&VALID.replace(":failures []", ":failures [:timeout]"))
            .map(|summary| (summary.validity, summary.linearizability));
        assert_eq!(workload_failure, Ok((Validity::Invalid, Validity::Valid)));
    }

    #[test]
    fn missing_checker_structure_is_a_harness_error() {
        assert!(parse("{:valid? true}").is_err());
    }

    #[test]
    fn lease_probe_completion_is_bound_to_exact_client_and_message() {
        let history = concat!(
            "{:index 1 :type :invoke :process 0 :f :read :value [0 nil]}\n",
            "{:index 2 :type :fail :process 0 :f :read :value [0 nil] ",
            ":error [:temporarily-unavailable \"LeadershipLost [rafter-lease-probe client=c1 msg_id=11 code=11]\"]}\n",
        );
        assert_eq!(lease_probe_completion_count(history, "c1", 11), Ok(1));
        assert_eq!(lease_probe_completion_count(history, "c2", 11), Ok(0));
        assert_eq!(lease_probe_completion_count(history, "c1", 12), Ok(0));
        assert_eq!(lease_probe_completion_count("", "c1", 11), Ok(0));
    }

    #[test]
    fn lease_probe_history_rejects_completion_only_truncation_and_swapped_processes() {
        let completion = "{:index 2 :type :fail :process 0 :f :read :value nil :error [:temporarily-unavailable \"x [rafter-lease-probe client=c1 msg_id=11 code=11]\"]}";
        assert!(lease_probe_completion_count(completion, "c1", 11).is_err());

        let truncated = "{:index 1 :type :invoke :process 0 :f :read :value nil}";
        assert!(lease_probe_completion_count(truncated, "c1", 11).is_err());

        let swapped = format!(
            "{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{{:index 2 :type :invoke :process 1 :f :write :value 1}}\n{}",
            completion.replace(":index 2", ":index 3").replace(":process 0", ":process 1")
        );
        assert!(lease_probe_completion_count(&swapped, "c1", 11).is_err());

        let intervening = format!(
            "{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{{:index 2 :type :fail :process 0 :f :read :error :net-timeout}}\n{}",
            completion.replace(":index 2", ":index 3")
        );
        assert!(lease_probe_completion_count(&intervening, "c1", 11).is_err());

        let exact_pair = format!(
            "{{:index 1 :type :invoke :process 0 :f :read :value [0 nil]}}\n{}",
            completion.replace(":value nil", ":value [1 nil]")
        );
        assert!(lease_probe_completion_count(&exact_pair, "c1", 11).is_err());
        let missing_value = format!(
            "{{:index 1 :type :invoke :process 0 :f :read :value nil}}\n{}",
            completion.replace(" :value nil", "")
        );
        assert!(lease_probe_completion_count(&missing_value, "c1", 11).is_err());
    }
}
