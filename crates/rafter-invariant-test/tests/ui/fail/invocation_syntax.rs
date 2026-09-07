//! Proves the oracle invocation macros take a bare detector name.
//!
//! A qualified path is refused by the macros' own rules rather than accepted
//! and half-expanded, so the detector a marker names is always the identifier
//! written at the call site. Which arities are accepted is the passing
//! fixture's scenario, not this one.

use rafter_invariant_test::{oracle_expect_err, oracle_invoke_recorder};

mod detector {
    pub fn reject() -> Result<(), ()> {
        Err(())
    }

    pub fn record() {}
}

fn main() {
    let _ = oracle_expect_err!(detector::reject(), "qualified path");
    oracle_invoke_recorder!(detector::record());
}
