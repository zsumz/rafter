//! The sentence upstream of the whole fairness derivation, and what holds it.
//!
//! CONTRACT.md's "Ready" definition names five conditions. Condition 3 — not
//! stalled by an external readiness report — is the only one the history
//! *reports* rather than implies, and therefore the only one the audited
//! scheduler authors for itself. Everything the fairness bound says rests on
//! the paragraph that constrains it.
//!
//! That paragraph read:
//!
//! > It is **bounded rather than derived** — see "A stall excuses only while it
//! > is unbroken" — and the boundary is that a stall which is cleared and
//! > re-raised without a plan naming the group in between excuses nothing,
//! > **wherever in the pass cycle it is cleared**.
//!
//! Both clauses were false, and refuted by a row this crate had already
//! written: the fourth row of "What falls outside that scope" is marked **No**
//! under "Bounded?", and the escape it names is a position in the pass cycle.
//! Cleared inside a pass whose plan outruns the pool, a stall cleared and
//! re-raised with no plan naming the group in between excuses everything:
//!
//! ```text
//! widest_gap=0 passes_armed=12 passes_completed=12 serviced=36
//! and the group still holds 1 items.
//! ```
//!
//! The code was honest and the escape's own section was honest; the definition
//! upstream of both was not, and two audits that hunted for exactly this shape
//! had passed over it. So this file pins both halves, because only the pair
//! catches it: what the mechanism does, and that every statement of it in
//! CONTRACT.md names the exception. A prose universal has nothing else that can
//! fail when it stops being true.

mod support;

use rafter_reference_sharded_counter::{GroupAvailability, SchedulingViolation, SystemClass};
use support::{config, group, pass, system, History};

/// This crate's own contract, so the checks below are over the document they
/// are about.
const CONTRACT: &str = include_str!("../CONTRACT.md");

/// The exception, in the wordings CONTRACT.md is allowed to state it in.
///
/// Two rather than one because the document says it long in prose and short in
/// tables, and pinning a single phrasing would be pinning an edit rather than a
/// claim.
const ESCAPE_MARKERS: &[&str] = &[
    "more entries pending than the pool has free workers",
    "wider than the pool",
];

/// Every place the document states condition 3's boundary, by a phrase stable
/// enough to find it and specific enough to be that place.
///
/// A table row is checked as a line and prose as a paragraph, because a row's
/// neighbours are inside the same block and would satisfy the check for it.
const BOUNDARY_CLAIMS: &[(&str, &str)] = &[
    (
        "the \"Ready\" definition of condition 3",
        "Condition 3 is the only one the history reports rather than implies",
    ),
    (
        "the freedoms table's stall row",
        "| Whether a group is stalled |",
    ),
    (
        "the opening of \"A stall excuses only while it is unbroken\"",
        "it was a third freedom over readiness",
    ),
    (
        "\"Stickiness is what the audit charges for\"",
        "So the excuse a stall buys is charged",
    ),
    (
        "invariant 23",
        "An external stall excuses a plan only while it is unbroken",
    ),
];

/// The universals the escape's row refutes, and must not come back.
///
/// Each was in the document, each was read past by two audits, and each is a
/// claim about the mechanism below rather than a turn of phrase.
const REFUTED_UNIVERSALS: &[(&str, &str)] = &[
    (
        "wherever in the pass cycle it is cleared",
        "the escape is exactly a position in the pass cycle",
    ),
    (
        "bounded rather than derived",
        "the row that governs condition 3 is marked No under \"Bounded?\"",
    ),
    (
        "not by which side of a pass boundary it is broken on",
        "which side of the boundary the stall breaks on is what the escape turns on",
    ),
    (
        "An external input, but not an unlimited one",
        "the fourth qualification is unlimited, and costs the host nothing",
    ),
];

/// What the mechanism does at the instant the old sentence quantified over, and
/// what actually decides it.
///
/// One history, folded twice. The stall is cleared and re-raised at the same
/// point in the pass cycle in both runs — inside an open pass, with nothing
/// dispatched and every worker free — so the pass cycle cannot be what separates
/// the verdicts. The pool can: two workers against a three-group plan is the
/// escape, and three workers against the same plan is the rule.
///
/// The accepted leg is the one the contract sentence denied. Twelve passes,
/// each armed and retired owing nothing, three items serviced per pass at the
/// full rate of the pool, a ready group denied every plan, and `widest_gap` of
/// zero. It is the same history as
/// `redteam_occupancy::a_plan_wider_than_the_pool_is_excused_from_its_arming_instant`,
/// kept here because it is the history the definition was refuted by and this
/// file is where that refutation is written down.
#[test]
fn the_stall_boundary_turns_on_the_plan_and_the_pool_rather_than_the_pass_cycle() {
    const PASSES: u64 = 12;
    let starved = group(3);

    // Two workers: the three-group plan outruns the pool from its arming tick,
    // so every restoration inside it is excused.
    let replay = stall_broken_inside_each_pass(PASSES).replay(config(8, 2, 2, 64, 512));
    let report = replay
        .fairness
        .expect("the audit accepts this history without complaint");
    let view = replay
        .view
        .groups
        .iter()
        .find(|view| view.group == starved)
        .copied()
        .expect("the starved group exists");
    println!(
        "cleared inside a pass wider than the pool: passes_armed={} \
         passes_completed={} serviced={} widest_gap={}; starved group holds {} \
         items after {PASSES} passes",
        report.passes_armed,
        report.passes_completed,
        report.serviced,
        report.widest_gap,
        view.queued,
    );

    assert_eq!(
        report.widest_gap, 0,
        "CONTRACT.md, \"Ready\", condition 3: the constraint holds at every \
         point in the pass cycle but one, and this is that one. A stall cleared \
         and re-raised with no plan naming the group in between excuses every \
         plan the pass spans, for as long as the plan outruns the pool."
    );
    assert_eq!(
        view.queued, 1,
        "the starved group holds the item it was never offered a turn for"
    );
    assert_eq!(
        report.passes_completed, PASSES,
        "every pass the host armed it also retired, owing nothing"
    );
    assert_eq!(
        report.serviced,
        3 * PASSES,
        "and the service floor certifies it: three items a pass, the full rate \
         of a two-worker pool. No number in the report bounds this row."
    );

    // Three workers: the identical history, at the identical point in the pass
    // cycle, with the plan no longer wider than the pool.
    let violation = stall_broken_inside_each_pass(PASSES)
        .audit(config(8, 3, 2, 64, 512))
        .expect_err("a plan the pool can hold opens the debt the rule describes");
    println!("the same history, one worker more: {violation:?}");
    assert_eq!(
        violation,
        SchedulingViolation::OpportunityGap {
            group: starved,
            from_pass: pass(2),
            denied_passes: u32::try_from(PASSES - 1).expect("twelve passes fit in u32"),
        },
        "the axis is the plan against the pool, not where in the pass the stall \
         was broken: nothing about the report's position changed"
    );
}

/// Every statement of that boundary in CONTRACT.md names the exception.
///
/// The defect this file exists for was a documentation defect: the mechanism
/// and the section describing the escape were both honest, and the definition
/// upstream of them stated a universal the escape refutes. Nothing could fail
/// when that happened, which is why it survived two audits looking for it. This
/// is the thing that fails.
///
/// It is decided from the text, which is the honest form of "this part is not
/// the compiler's" — `oracle::tests::every_fault_site_is_raised_by_exactly_one_check`
/// makes the same trade for the same reason.
#[test]
fn every_statement_of_the_stall_boundary_names_its_exception() {
    for (place, anchor) in BOUNDARY_CLAIMS {
        let claim = claim_containing(anchor).unwrap_or_else(|| {
            panic!(
                "{place} is anchored on {anchor:?}, which is not in CONTRACT.md — \
                 re-anchor this check rather than dropping it"
            )
        });
        assert!(
            ESCAPE_MARKERS.iter().any(|marker| claim.contains(marker)),
            "{place} states condition 3's boundary and must name the one \
             instant that escapes it, because the escape is unbounded and \
             freely available. Expected one of {ESCAPE_MARKERS:?} in:\n{claim}"
        );
    }

    let contract = flattened(CONTRACT);
    for (universal, why) in REFUTED_UNIVERSALS {
        assert!(
            !contract.contains(&flattened(universal)),
            "CONTRACT.md says {universal:?}, and {why}"
        );
    }

    // And the row every one of those paragraphs is qualified by still says so.
    let row = claim_containing("| Restored while the open pass has more entries pending")
        .expect("the fourth row of \"What falls outside that scope\" exists");
    assert!(
        row.contains("**No**"),
        "the escape's own row is what the paragraphs above are measured \
         against; if it stops saying No under \"Bounded?\", they are measured \
         against nothing:\n{row}"
    );
}

/// The one claim `anchor` identifies: its line when that line is a table row,
/// and its blank-line paragraph otherwise, with its line wrapping flattened.
///
/// Flattened because a claim is a sentence and the document wraps at eighty
/// columns, so where the newlines fall is a fact about the editor rather than
/// about the claim. Matching the raw text would make this check pass or fail on
/// a reflow.
fn claim_containing(anchor: &str) -> Option<String> {
    let anchor = flattened(anchor);
    if anchor.starts_with('|') {
        return CONTRACT
            .lines()
            .map(flattened)
            .find(|line| line.contains(&anchor));
    }
    CONTRACT
        .split("\n\n")
        .map(flattened)
        .find(|paragraph| paragraph.contains(&anchor))
}

/// One run of whitespace, one space.
fn flattened(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// `PASSES` passes over a three-group plan, with the starved group's stall
/// cleared and re-raised inside each one.
///
/// The clear and the re-raise land at the arming tick, after the plan is armed
/// and before anything is dispatched: the pool is whole, every worker is free,
/// and the only thing distinguishing this instant from the arming instant
/// beside it is how wide the plan is. Each pass then finishes normally, inside
/// its own span, owing nothing.
fn stall_broken_inside_each_pass(passes: u64) -> History {
    // Armed at `b`, last entry offered and the pass retired at `b + 1`, its
    // worker back at `b + 2`.
    const SPAN: u64 = 3;
    let busy = [group(0), group(1), group(2)];
    let starved = group(3);

    let mut history = History::new();
    for id in busy {
        history.open_group(id, 1);
    }
    history.open_group(starved, 1);
    for id in busy {
        for _ in 0..passes {
            history.submit(id, system(SystemClass::Bulk, 1));
        }
    }
    history.submit(starved, system(SystemClass::Bulk, 1));
    history.reported(1, starved, GroupAvailability::Stalled);

    for index in 0..passes {
        let pass = index + 1;
        let base = index * SPAN + 1;
        history.armed(pass, base, busy.to_vec());

        history.reported(base, starved, GroupAvailability::Available);
        history.reported(base, starved, GroupAvailability::Stalled);

        history.dispatched(pass, base, busy[0], 1, 1);
        history.serviced(pass, busy[0], index + 1);
        history.dispatched(pass, base, busy[1], 1, 1);
        history.serviced(pass, busy[1], passes + index + 1);
        history.released(base + 1, busy[0]);
        history.released(base + 1, busy[1]);
        history.dispatched(pass, base + 1, busy[2], 1, 1);
        history.serviced(pass, busy[2], 2 * passes + index + 1);
        history.retired(pass, base + 1);
        history.released(base + 2, busy[2]);
    }
    history
}
