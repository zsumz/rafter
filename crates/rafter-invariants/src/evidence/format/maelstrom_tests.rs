//! Tests for neutral Maelstrom evidence parsing.

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
        parse(&VALID.replace(":valid? true}", ":valid? false}")).map(|summary| summary.validity),
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
