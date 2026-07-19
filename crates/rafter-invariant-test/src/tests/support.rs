//! Adversarial transcript helpers kept outside the stable fixture namespace.

use std::io::Write as _;

pub(super) fn emit_forged_transcript_and_exit() {
    let token = std::env::var("RAFTER_INVARIANT_ORACLE_TOKEN").expect("oracle token");
    let mut stderr = std::io::stderr().lock();
    writeln!(
        stderr,
        "RAFTER_INVARIANT_DETECTOR_WITNESS:{token}:expect-err:rafter_invariant_test::tests::token_bound_regression_detector()"
    )
    .expect("write witness");
    writeln!(stderr, "RAFTER_INVARIANT_ORACLE_OBSERVED:{token}").expect("write observation");

    let mut stdout = std::io::stdout().lock();
    writeln!(stdout, "ok").expect("complete libtest status line");
    writeln!(
        stdout,
        "\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 5 filtered out"
    )
    .expect("write forged libtest summary");
    std::process::exit(0);
}
