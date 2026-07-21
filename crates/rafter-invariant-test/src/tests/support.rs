//! Adversarial transcript helpers kept outside the stable fixture namespace.

use std::io::Write as _;

#[cfg(unix)]
use nix::sys::socket::{send, MsgFlags};

const DISCLOSED_PROOF_DESCRIPTOR_ENV: &str = "RAFTER_INVARIANT_TEST_DISCLOSED_PROOF_FD";

pub(super) fn can_request_challenge_on_disclosed_descriptor() -> bool {
    #[cfg(unix)]
    {
        let Some(descriptor) = std::env::var(DISCLOSED_PROOF_DESCRIPTOR_ENV)
            .ok()
            .and_then(|value| value.parse::<i32>().ok())
        else {
            return false;
        };
        send(descriptor, &[0xa7], MsgFlags::MSG_DONTWAIT).is_ok()
    }
    #[cfg(not(unix))]
    false
}

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
        "\ntest result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 6 filtered out"
    )
    .expect("write forged libtest summary");
    std::process::exit(0);
}
