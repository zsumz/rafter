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
    use super::{parse, MaelstromSummary, Validity};

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
}
