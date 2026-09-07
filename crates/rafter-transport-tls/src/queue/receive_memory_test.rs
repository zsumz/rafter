//! Inbound memory is reserved before allocation and released exactly once.
//!
//! A declared frame length must reserve before any allocation happens and
//! release exactly what it took, and retained scratch and frames must draw on
//! one global budget — a double release or a second budget would break the
//! bound the transport's admission decisions depend on.

use super::*;

#[test]
fn declared_lengths_reserve_before_allocation_and_release_exactly() {
    let limits = ReceiveMemoryLimits::new(320, 32).expect("valid memory limits");
    let budget = ReceiveMemoryBudget::new(limits, 8);
    let first = budget.try_acquire_frame(6).expect("200 weighted bytes");
    assert_eq!(budget.used(), 200);
    assert!(matches!(
        budget.try_acquire_frame(4),
        Err(ReceiveMemoryFull {
            required: 136,
            maximum: 320,
        })
    ));
    drop(first);
    assert_eq!(budget.used(), 0);
    assert!(budget.try_acquire_frame(9).is_ok());
}

#[test]
fn retained_scratch_and_frames_share_one_global_budget() {
    let limits = ReceiveMemoryLimits::new(320, 32).expect("valid memory limits");
    let budget = ReceiveMemoryBudget::new(limits, 8);
    let scratch = budget.try_acquire_scratch(120).expect("connection scratch");
    let frame = budget.try_acquire_frame(6).expect("remaining frame budget");

    assert_eq!(budget.used(), 320);
    assert!(matches!(
        budget.try_acquire_scratch(1),
        Err(ReceiveMemoryFull {
            required: 1,
            maximum: 320,
        })
    ));
    drop(frame);
    drop(scratch);
    assert_eq!(budget.used(), 0);
}
