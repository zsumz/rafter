//! The closure claim on the torn-tail taxonomy.

use crate::store::TornTail;

/// The closure claim on [`TornTail`], as a check rather than a paragraph.
///
/// The `match` names every variant, so adding one to the enum stops this
/// compiling until somebody has decided which side of the truncation rule it
/// falls on. That is the whole point: "exactly one of these is residue" is
/// the sentence the destructive branch reads, and a new variant silently
/// defaulting to either answer is how that sentence goes stale.
#[test]
fn exactly_one_torn_tail_is_residue_an_interrupted_append_leaves() {
    let every = [
        TornTail::UnsealedAppend { present: 8 },
        TornTail::UnsealedCompleteFrame { len: 64 },
        TornTail::ZeroFilledToEnd { present: 8 },
        TornTail::NotALedgerFrame { magic: [0xAD; 4] },
        TornTail::PartialBeginRecord,
        TornTail::BeginRecordCorrupt,
        TornTail::UnsupportedFrameVersion { version: 2 },
        TornTail::PartialImage,
        TornTail::ImageCorrupt,
        TornTail::MissingCommitRecord,
        TornTail::PartialCommitRecord,
        TornTail::CommitRecordCorrupt,
    ];
    for tail in every {
        // Exhaustive by name, twice, because the two predicates carry two
        // different proofs. A variant added later has to be placed on both.
        let (interrupted, truncatable) = match tail {
            TornTail::UnsealedAppend { .. } => (true, true),
            // Truncated, but not on the interrupted-append proof: see
            // `TornTail::is_truncatable_residue` for the premise it does
            // rest on and the loss that premise admits.
            TornTail::ZeroFilledToEnd { .. } => (false, true),
            TornTail::UnsealedCompleteFrame { .. }
            | TornTail::NotALedgerFrame { .. }
            | TornTail::PartialBeginRecord
            | TornTail::BeginRecordCorrupt
            | TornTail::UnsupportedFrameVersion { .. }
            | TornTail::PartialImage
            | TornTail::ImageCorrupt
            | TornTail::MissingCommitRecord
            | TornTail::PartialCommitRecord
            | TornTail::CommitRecordCorrupt => (false, false),
        };
        assert_eq!(
            tail.is_interrupted_append(),
            interrupted,
            "{tail:?} changed sides of the interrupted-append proof"
        );
        assert_eq!(
            tail.is_truncatable_residue(),
            truncatable,
            "{tail:?} changed sides of the truncation rule"
        );
    }
    assert_eq!(
        every
            .iter()
            .filter(|tail| tail.is_interrupted_append())
            .count(),
        1,
        "exactly one torn tail is what an interrupted append leaves"
    );
    assert_eq!(
        every
            .iter()
            .filter(|tail| tail.is_truncatable_residue())
            .count(),
        2,
        "exactly two torn tails may be truncated, on two separate arguments"
    );
}
