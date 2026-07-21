//! Neutral mutation-transcript decoder scenarios.

use super::{parse_mutation_transcript, MutationSummary};

#[test]
fn mutation_transcript_decodes_inventory_without_deciding_acceptance() {
    let transcript = parse_mutation_transcript(
        "running 1 tests\n\
         test producer::tla_exec::mutation_tests::probe ... ok\n\
         test result: ok. 1 passed; 0 failed; 2 ignored; 3 measured; finished\n",
    );
    assert_eq!(transcript.running_counts, [1]);
    assert_eq!(
        transcript.passed_tests,
        ["producer::tla_exec::mutation_tests::probe"]
    );
    assert_eq!(
        transcript.summaries,
        [MutationSummary {
            passed: 1,
            failed: 0,
            ignored: 2,
            measured: 3,
        }]
    );
}

#[test]
fn mutation_transcript_rejects_noncanonical_cargo_counts() {
    for transcript in [
        "running 01 tests\n",
        "test result: ok. 01 passed; 0 failed; 0 ignored; 0 measured; finished\n",
        "test result: ok. 1  passed; 0 failed; 0 ignored; 0 measured; finished\n",
    ] {
        let transcript = parse_mutation_transcript(transcript);
        assert!(transcript.running_counts.is_empty());
        assert!(transcript.summaries.is_empty());
    }
}
