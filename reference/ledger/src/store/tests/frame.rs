//! The recognition order, and the invariants it has to hold over every byte.

use super::support::sealed_frame;
use crate::store::{
    format::{SEALED_FRAME_MARK, UNSEALED_FRAME_MARK},
    frame::read_frame,
    TornTail,
};

/// The recognition order, as a table rather than a paragraph.
///
/// The sibling fenced-lock store answers the same questions in the same
/// order over its own format, and its
/// `the_recognition_order_matches_the_sibling_ledger_store` holds the
/// mirror of this table. Neither store can depend on the other — they are
/// independent acceptance consumers — so the agreement is kept by two
/// tables that have to be edited together, and each names the other.
///
/// The fourth-generation hunt found the two disagreeing one commit after
/// they were aligned on the version byte, because the alignment was argued
/// in prose about one byte instead of pinned as an order over all of them.
/// Each row below is a question, the byte it is asked about, and the
/// verdict class it produces; a store that moves a question past another
/// changes a row.
#[test]
fn the_recognition_order_matches_the_sibling_lock_store() {
    let frame = sealed_frame();
    let at = |mutate: &dyn Fn(&mut Vec<u8>)| {
        let mut bytes = frame.clone();
        mutate(&mut bytes);
        read_frame(&bytes).err()
    };

    // 1. Zeros to the end of the file, ahead of everything, at every length.
    //    The lock store has no counterpart: its slots are whole files of a
    //    known shape, so it has no tail whose length is evidence, and its
    //    row here is the creation mark it recognizes in the same position.
    assert_eq!(
        read_frame(&[0_u8; 64]).err(),
        Some(TornTail::ZeroFilledToEnd { present: 64 }),
        "zeros to end of file are recognized before anything else"
    );

    // 2. Identity — byte zero is one of the two marks, and bytes one
    //    through three are the magic — at every length, above the seal test.
    for offset in 0..4_usize {
        let magic = at(&|bytes| bytes[offset] ^= 0xFF);
        assert!(
            matches!(magic, Some(TornTail::NotALedgerFrame { .. })),
            "byte {offset} is identity and must be answered above the seal test, not \
             {magic:?}"
        );
    }
    // Identity holds at every length that carries a byte of it, which is
    // what stops a short tail being attributed to this build.
    assert!(
        matches!(
            read_frame(&[SEALED_FRAME_MARK, 0xFF]).err(),
            Some(TornTail::NotALedgerFrame { .. })
        ),
        "identity must be asked at two bytes, not deferred to a full begin record"
    );
    // The load-bearing row, and the one this store had wrong: identity is
    // asked above the mark, so a tail whose mark reads unsealed *and* whose
    // magic is broken is foreign and not residue. Asking the mark first
    // sends these bytes to `classify_unsealed`, whose re-read fails at
    // `BeginRecordCorrupt` and is folded into `UnsealedAppend` — the one
    // verdict `open` may truncate on. Two zeroed bytes were enough.
    for offset in 1..4_usize {
        let unsealed = at(&|bytes| {
            bytes[0] = UNSEALED_FRAME_MARK;
            bytes[offset] = 0x00;
        });
        assert!(
            matches!(unsealed, Some(TornTail::NotALedgerFrame { .. })),
            "an unsealed mark beside a broken magic byte {offset} must be foreign, not \
             {unsealed:?}"
        );
        assert!(
            !unsealed.is_some_and(TornTail::is_truncatable_residue),
            "byte {offset} zeroed beside an unsealed mark must never be truncated"
        );
    }

    // 3. The seal, below identity and above everything length-dependent.
    assert!(
        matches!(
            at(&|bytes| bytes[0] = UNSEALED_FRAME_MARK),
            Some(TornTail::UnsealedCompleteFrame { .. })
        ),
        "the seal test comes after identity and sends an unsealed frame to be re-read"
    );

    // 4. The version, wherever the field is present — on both sides of the
    //    seal test, and before any check that depends on how many bytes
    //    there are. This is the row commit 1dd109b1 aligned and the row the
    //    ledger had drifted back off, by gating it on a full begin record.
    assert_eq!(
        read_frame(&[SEALED_FRAME_MARK, b'L', b'B', b'G', 9]).err(),
        Some(TornTail::UnsupportedFrameVersion { version: 9 }),
        "a foreign version must be answered at five bytes, not deferred"
    );
    assert_eq!(
        read_frame(&[UNSEALED_FRAME_MARK, b'L', b'B', b'G', 9]).err(),
        Some(TornTail::UnsupportedFrameVersion { version: 9 }),
        "a foreign version must be answered the same way on the unsealed side"
    );

    // 5. Only now, length. A begin record cut short is a length answer and
    //    must not be reachable by anything above.
    assert_eq!(
        read_frame(&[SEALED_FRAME_MARK, b'L', b'B', b'G']).err(),
        Some(TornTail::PartialBeginRecord),
        "length is the last question, not the first"
    );
}

/// The invariant that closes the mark byte's hole, checked exhaustively.
///
/// Every byte of a sealed frame, set to every other value it could take:
/// none of them may produce residue. Before the mark carried a completeness
/// test beside it, one of these 33,000-odd mutants did — byte zero to
/// `0x00`, and only that one — and `open` answered it by deleting the frame
/// and everything after it from the medium.
///
/// This is the single-fault assumption in
/// [`TornTail::is_interrupted_append`]'s proof, checked rather than
/// asserted. It asserts on the wider predicate
/// [`TornTail::is_truncatable_residue`], because the claim being made is
/// about what `open` shortens, not about which of the two arguments it
/// shortened on.
#[test]
fn no_single_byte_change_to_a_sealed_frame_is_ever_truncatable() {
    let frame = sealed_frame();
    for offset in 0..frame.len() {
        let original = frame[offset];
        for value in 0..=u8::MAX {
            if value == original {
                continue;
            }
            let mut mutant = frame.clone();
            mutant[offset] = value;
            if let Err(tail) = read_frame(&mutant) {
                assert!(
                    !tail.is_truncatable_residue(),
                    "byte {offset} of a sealed frame changed from {original:#04x} to \
                     {value:#04x} reads as {tail:?}, which recovery would truncate"
                );
            }
        }
    }
}

/// The byte-zero mutant on its own, named, so a regression says which byte.
#[test]
fn a_sealed_frames_mark_byte_rotting_to_zero_is_a_whole_frame_not_residue() {
    let mut mutant = sealed_frame();
    let len = mutant.len() as u64;
    assert_eq!(mutant[0], SEALED_FRAME_MARK, "the fixture is sealed");
    mutant[0] = UNSEALED_FRAME_MARK;
    assert_eq!(
        read_frame(&mutant).err(),
        Some(TornTail::UnsealedCompleteFrame { len }),
        "a committed frame whose mark byte rotted must be named a whole frame, not an \
         interrupted append"
    );
}

/// The other direction, so the fix cannot be "refuse everything".
///
/// Ordinary crash residue — a strict prefix of a frame carrying the unsealed
/// mark — must still be residue at every length, or a crash in the middle of
/// an append would need an operator.
///
/// The one-byte prefix is the frame's mark and nothing else, so it is zeros
/// to the end of the file and it is [`TornTail::ZeroFilledToEnd`] that
/// covers it. Every longer prefix carries the identity and is
/// [`TornTail::UnsealedAppend`]. Both are truncated; the assertion is on the
/// predicate the truncating branch reads, and the variant is named beside it
/// so the boundary between the two arguments is visible here rather than
/// inferred.
#[test]
fn every_strict_prefix_of_an_unsealed_frame_is_truncatable_residue() {
    let frame = sealed_frame();
    for present in 1..frame.len() {
        let mut residue = frame[..present].to_vec();
        residue[0] = UNSEALED_FRAME_MARK;
        let expected = if present == 1 {
            TornTail::ZeroFilledToEnd { present: 1 }
        } else {
            TornTail::UnsealedAppend {
                present: present as u64,
            }
        };
        let found = read_frame(&residue).err();
        assert_eq!(
            found,
            Some(expected),
            "a {present} byte prefix of an unsealed append must stay truncatable"
        );
        assert!(
            found.is_some_and(TornTail::is_truncatable_residue),
            "a {present} byte prefix of an unsealed append must stay truncatable"
        );
    }
}

/// A zero-filled tail is residue at every length, which is what a crash on a
/// delayed-allocation filesystem actually leaves.
///
/// `durable_zero_tail.rs` proves this through the store; this proves the
/// classifier underneath it still says so once the mark has a completeness
/// test beside it, at lengths that suite does not enumerate.
///
/// It is [`TornTail::ZeroFilledToEnd`] rather than
/// [`TornTail::UnsealedAppend`] since the identity test moved above the
/// mark, and the rename is the point: these bytes are truncated on a premise
/// about the physical world, and the report now says which premise it used.
#[test]
fn a_zero_filled_tail_is_truncatable_residue_at_every_length() {
    for present in 1..96_usize {
        let zeros = vec![0_u8; present];
        assert_eq!(
            read_frame(&zeros).err(),
            Some(TornTail::ZeroFilledToEnd {
                present: present as u64
            }),
            "{present} zero bytes must read as truncatable residue"
        );
    }
}

/// The boundary of [`TornTail::ZeroFilledToEnd`], on both sides.
///
/// Its scope is "zeros all the way to the end of the file". One non-zero
/// byte anywhere after them — which is what a committed frame behind the
/// damage looks like — takes the bytes out of that scope, and the answer
/// flips from truncation to refusal. This is the boundary the fourth
/// generation found untested: the rule was right and nothing checked the
/// first case on each side of it.
#[test]
fn one_non_zero_byte_after_a_zero_run_takes_it_out_of_the_zero_fill_rule() {
    for zeros in [1_usize, 2, 3, 512] {
        let all_zero = vec![0_u8; zeros];
        assert_eq!(
            read_frame(&all_zero).err(),
            Some(TornTail::ZeroFilledToEnd {
                present: zeros as u64
            }),
            "{zeros} zeros to end of file are inside the rule"
        );

        let mut trailed = all_zero.clone();
        trailed.push(0x01);
        let found = read_frame(&trailed).err();
        assert!(
            matches!(found, Some(TornTail::NotALedgerFrame { .. })),
            "{zeros} zeros followed by one non-zero byte are outside the rule and must \
             refuse, not {found:?}"
        );
        assert!(
            !found.is_some_and(TornTail::is_truncatable_residue),
            "{zeros} zeros followed by one non-zero byte must not be truncated"
        );
    }
}
