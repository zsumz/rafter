//! The recognition order, and the invariants it has to hold over every byte.

use super::support::{sealed_image, sealed_image_of};
use crate::store::{
    format::{SEALED_MARK, SLOT_FORMAT_VERSION, SLOT_HEADER_LEN, SLOT_TRAILER_LEN, UNSEALED_MARK},
    image::verify_slot,
    SlotDamage,
};

/// The invariant that closes the mark byte's hole, checked exhaustively.
///
/// Every byte of a sealed image, set to every other value it could take:
/// none of them may produce residue. Before the mark carried a completeness
/// test beside it, one of these 45,000-odd mutants did — byte zero to
/// `0x00`, and only that one — and recovery answered it by adopting the
/// stale partner and regressing an acknowledged fencing high-water mark.
///
/// This is the single-fault assumption in
/// [`SlotDamage::is_publication_residue`]'s proof, checked rather than
/// asserted.
#[test]
fn no_single_byte_change_to_a_sealed_image_is_ever_publication_residue() {
    let image = sealed_image();
    for offset in 0..image.len() {
        let original = image[offset];
        for value in 0..=u8::MAX {
            if value == original {
                continue;
            }
            let mut mutant = image.clone();
            mutant[offset] = value;
            if let Err(damage) = verify_slot(&mutant) {
                assert!(
                    !damage.is_publication_residue(),
                    "byte {offset} of a sealed image changed from {original:#04x} to \
                     {value:#04x} reads as {damage:?}, which recovery would skip"
                );
            }
        }
    }
}

/// The byte-zero mutant on its own, named, so a regression says which byte.
#[test]
fn a_sealed_images_mark_byte_rotting_to_zero_is_a_whole_image_not_residue() {
    let mut mutant = sealed_image();
    let len = mutant.len() as u64;
    assert_eq!(mutant[0], SEALED_MARK, "the fixture is sealed");
    mutant[0] = UNSEALED_MARK;
    assert_eq!(
        verify_slot(&mutant).err(),
        Some(SlotDamage::UnsealedCompleteImage { len, generation: 1 }),
        "a live slot whose mark byte rotted must be named a whole image, not an interrupted \
         publication"
    );
}

/// The recognition order, as a table rather than a paragraph.
///
/// The sibling ledger consumer answers the same questions in the same order
/// over its own format, and its
/// `the_recognition_order_matches_the_sibling_lock_store` holds the mirror
/// of this table. Neither store can depend on the other — they are
/// independent acceptance consumers — so the agreement is kept by two tables
/// that have to be edited together, and each names the other.
///
/// The fourth-generation hunt found the two disagreeing one commit after
/// they were aligned on the version byte, because the alignment was argued
/// in prose about one byte instead of pinned as an order over all of them.
/// This store was the one that had it right; the table is here so that stays
/// a property rather than a coincidence.
#[test]
fn the_recognition_order_matches_the_sibling_ledger_store() {
    let image = sealed_image();
    let at = |mutate: &dyn Fn(&mut Vec<u8>)| {
        let mut bytes = image.clone();
        mutate(&mut bytes);
        verify_slot(&bytes).err()
    };

    // 1. The creation mark, ahead of everything. This is the row the ledger
    //    fills with its zeros-to-end-of-file rule: each store's one shape
    //    that must be recognized before identity can refuse it, and each
    //    store's is different because a slot is a whole file of a known
    //    shape while a journal has a tail whose length is evidence.
    assert_eq!(
        verify_slot(&crate::store::format::CREATION_MARK).err(),
        None,
        "the creation mark is recognized before anything else"
    );

    // 2. Identity — byte zero is one of the two marks, and bytes one through
    //    three are the magic — at every length, above the seal test.
    for offset in 0..4_usize {
        let magic = at(&|bytes| bytes[offset] ^= 0xFF);
        assert!(
            matches!(magic, Some(SlotDamage::NotALockImage { .. })),
            "byte {offset} is identity and must be answered above the seal test, not \
             {magic:?}"
        );
    }
    assert!(
        matches!(
            verify_slot(&[SEALED_MARK, 0xFF]).err(),
            Some(SlotDamage::NotALockImage { .. })
        ),
        "identity must be asked at two bytes, not deferred to a full header"
    );
    // The load-bearing row, and the one the ledger had wrong: identity is
    // asked above the mark, so a slot whose mark reads unsealed *and* whose
    // magic is broken is a foreign image and not residue. Asking the mark
    // first sends these bytes to `classify_unsealed`, which folds a broken
    // magic into `UnsealedPublication` — the one verdict recovery may skip
    // on. Two zeroed bytes are enough to produce it.
    for offset in 1..4_usize {
        let unsealed = at(&|bytes| {
            bytes[0] = UNSEALED_MARK;
            bytes[offset] = 0x00;
        });
        assert!(
            matches!(unsealed, Some(SlotDamage::NotALockImage { .. })),
            "an unsealed mark beside a broken magic byte {offset} must be foreign, not \
             {unsealed:?}"
        );
    }

    // 3. The seal, below identity and above everything length-dependent.
    assert!(
        matches!(
            at(&|bytes| bytes[0] = UNSEALED_MARK),
            Some(SlotDamage::UnsealedCompleteImage { .. })
        ),
        "the seal test comes after identity and sends an unsealed slot to be re-read"
    );

    // 4. The version, wherever the field is present — on both sides of the
    //    seal test, and before any check that depends on how many bytes
    //    there are. This is the row the ledger had drifted off, by gating it
    //    on a full begin record.
    assert_eq!(
        verify_slot(&[SEALED_MARK, b'F', b'L', b'K', 9]).err(),
        Some(SlotDamage::UnsupportedFormatVersion { version: 9 }),
        "a foreign version must be answered at five bytes, not deferred"
    );
    assert_eq!(
        verify_slot(&[UNSEALED_MARK, b'F', b'L', b'K', 9]).err(),
        Some(SlotDamage::UnsupportedFormatVersion { version: 9 }),
        "a foreign version must be answered the same way on the unsealed side"
    );

    // 5. Only now, length.
    assert!(
        matches!(
            verify_slot(&[SEALED_MARK, b'F', b'L', b'K', SLOT_FORMAT_VERSION]).err(),
            Some(SlotDamage::HeaderIncomplete { .. })
        ),
        "length is the last question, not the first"
    );
}

/// The other direction, so the fix cannot be "refuse everything".
///
/// Ordinary crash residue — a strict prefix of an image carrying the
/// unsealed mark — must still be residue at every length, or a crash in the
/// middle of a publication would need an operator.
#[test]
fn every_strict_prefix_of_an_unsealed_image_is_publication_residue() {
    let image = sealed_image();
    assert!(
        image.len() > SLOT_HEADER_LEN + SLOT_TRAILER_LEN,
        "the fixture carries a payload"
    );
    // A one-byte slot is the creation mark, which is not damage at all.
    for present in 2..image.len() {
        let mut residue = image[..present].to_vec();
        residue[0] = UNSEALED_MARK;
        assert_eq!(
            verify_slot(&residue).err(),
            Some(SlotDamage::UnsealedPublication {
                present: present as u64
            }),
            "a {present} byte prefix of an unsealed publication must stay skippable"
        );
    }
}

/// The one artifact in this store no checksum can cover, pinned at every
/// value a byte can take.
///
/// A slot of one byte is the creation mark, and "nothing has ever been
/// sealed here" is not damage — recovery adopts the partner and
/// `establish_slot_files` will finish an interrupted creation over it. That
/// makes it the last place where one byte's value decides something other
/// than a refusal, and it cannot be checksummed, because the whole artifact
/// is the byte.
///
/// What holds it up instead is that a publication never shortens a slot
/// below one header and one trailer, so the only one-byte slot this store
/// writes is the one creation writes. A sealed image cut to one byte
/// therefore stays damage, and reaching the benign answer from a sealed
/// image needs the truncation *and* a change to the surviving byte — two
/// faults, which is the same bound the rest of this file's single-fault
/// reasoning rests on.
#[test]
fn a_one_byte_slot_is_benign_only_at_the_creation_mark() {
    assert_eq!(
        verify_slot(&[UNSEALED_MARK]).map(|image| image.is_none()),
        Ok(true),
        "the creation mark is not damage"
    );
    assert_eq!(
        verify_slot(&[SEALED_MARK]).err(),
        Some(SlotDamage::HeaderIncomplete { present: 1 }),
        "a sealed image cut to one byte is damage, not a fresh slot"
    );
    assert_eq!(
        verify_slot(&[]).err(),
        Some(SlotDamage::SlotEmptied),
        "and a slot of no bytes is damage too"
    );
    for byte in 0..=u8::MAX {
        if byte == UNSEALED_MARK || byte == SEALED_MARK {
            continue;
        }
        assert_eq!(
            verify_slot(&[byte]).err(),
            Some(SlotDamage::NotALockImage {
                magic: [byte, 0, 0, 0]
            }),
            "a one-byte slot holding {byte:#04x} is not this store's"
        );
    }
}

/// A shorter image published over a longer one, interrupted before the slot
/// is cut back, leaves a new prefix followed by the older image's tail.
///
/// That mixture is what a real interrupted publication looks like on the
/// medium — the store cuts the slot back to the new length only after the
/// bytes are out — and recovery must be able to set it aside at every
/// boundary without an operator. It is the case the completeness test is
/// most likely to get wrong, because both halves are images this build wrote
/// and carry its magic and version.
///
/// Two shapes come out of it and both are answerable. Until the two images
/// first differ, the slot still holds the *whole older image* with its mark
/// overwritten, and its generation is the older one, which the sealed
/// partner outranks. From the first differing byte on, the mixture verifies
/// as nothing and is ordinary residue. What must never appear is a whole
/// image carrying the *newer* generation, because that is the one shape
/// recovery cannot resolve on its own.
#[test]
fn a_new_prefix_over_an_older_tail_is_never_the_newer_generation() {
    let older = sealed_image_of(4, 7);
    let newer = sealed_image_of(1, 8);
    assert!(
        newer.len() < older.len(),
        "the newer image has to be the shorter one for a tail to survive"
    );
    let mut whole_older = 0_usize;
    for boundary in 1..newer.len() {
        let mut mixture = newer[..boundary].to_vec();
        mixture.extend_from_slice(&older[boundary..]);
        mixture[0] = UNSEALED_MARK;
        let damage = verify_slot(&mixture)
            .err()
            .expect("an unsealed slot never verifies as a sealed image");
        match damage {
            SlotDamage::UnsealedPublication { .. } => {}
            SlotDamage::UnsealedCompleteImage { generation, .. } => {
                assert_eq!(
                    generation, 7,
                    "the whole image still in the slot at byte {boundary} is the older one; \
                     a newer generation here would be unresolvable"
                );
                whole_older += 1;
            }
            other => panic!("a publication interrupted at byte {boundary} left {other:?}"),
        }
    }
    assert!(
        whole_older > 0,
        "the sweep never reached the boundaries where the older image survives whole, so it \
         proved nothing about them"
    );
}
