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
