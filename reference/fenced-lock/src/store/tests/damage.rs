//! The closure claim on the damage taxonomy.

use crate::store::SlotDamage;

/// The closure claim on [`SlotDamage`], as a check rather than a paragraph.
///
/// The `match` names every variant, so adding one to the enum stops this
/// compiling until somebody has decided which side of the skip rule it falls
/// on. That is the whole point: "exactly one of these is residue" is the
/// sentence the skip branch reads, and a new variant silently defaulting to
/// either answer is how that sentence goes stale.
#[test]
fn exactly_one_slot_damage_is_residue_an_interrupted_publication_leaves() {
    let every = [
        SlotDamage::SlotEmptied,
        SlotDamage::UnsealedPublication { present: 20 },
        SlotDamage::UnsealedCompleteImage {
            len: 180,
            generation: 6,
        },
        SlotDamage::HeaderIncomplete { present: 12 },
        SlotDamage::NotALockImage { magic: [b'Z'; 4] },
        SlotDamage::UnsupportedFormatVersion { version: 2 },
        SlotDamage::HeaderChecksumMismatch {
            declared: 1,
            computed: 2,
        },
        SlotDamage::PayloadIncomplete {
            declared: 9,
            present: 3,
        },
        SlotDamage::MissingCommitChecksum,
        SlotDamage::PartialCommitChecksum { present: 2 },
        SlotDamage::CommitChecksumMismatch {
            declared: 1,
            computed: 2,
        },
        SlotDamage::TrailingBytes { extra: 4 },
    ];
    for damage in every {
        // Exhaustive by name. A variant added later has to be added here.
        let expected = match damage {
            SlotDamage::UnsealedPublication { .. } => true,
            SlotDamage::SlotEmptied
            | SlotDamage::UnsealedCompleteImage { .. }
            | SlotDamage::HeaderIncomplete { .. }
            | SlotDamage::NotALockImage { .. }
            | SlotDamage::UnsupportedFormatVersion { .. }
            | SlotDamage::HeaderChecksumMismatch { .. }
            | SlotDamage::PayloadIncomplete { .. }
            | SlotDamage::MissingCommitChecksum
            | SlotDamage::PartialCommitChecksum { .. }
            | SlotDamage::CommitChecksumMismatch { .. }
            | SlotDamage::TrailingBytes { .. } => false,
        };
        assert_eq!(
            damage.is_publication_residue(),
            expected,
            "{damage:?} changed sides of the skip rule"
        );
    }
    assert_eq!(
        every
            .iter()
            .filter(|damage| damage.is_publication_residue())
            .count(),
        1,
        "exactly one damage may be skipped"
    );
}
