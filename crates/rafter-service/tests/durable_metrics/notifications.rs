//! Notification callbacks see released credits, including after result collection.
use super::support::*;
use std::sync::{OnceLock, Weak};

fn released_before_notification(collect: bool) {
    let slot = Arc::new(OnceLock::<Weak<DurableReadSender<u8>>>::new());
    let armed = Arc::new(AtomicBool::new(false));
    let observed = Arc::new(AtomicUsize::new(0));
    let callback_slot = Arc::clone(&slot);
    let callback_armed = Arc::clone(&armed);
    let callback_observed = Arc::clone(&observed);
    let mut replica = Replica::new_with_wake(1, vec![], 2, false, move || {
        if callback_armed.load(Ordering::Acquire) {
            let sender = callback_slot.get().unwrap().upgrade().unwrap();
            assert_eq!(sender.retained().reads, 0, "woke before releasing credit");
            callback_observed.fetch_add(1, Ordering::AcqRel);
        }
    });
    let sender = Arc::new(replica.sender.clone());
    slot.set(Arc::downgrade(&sender)).unwrap();
    for _ in 0..3 {
        replica.owner.try_step(Input::Tick).unwrap();
        drain(&mut replica.owner);
    }
    let mut read = sender.read(
        vec![0],
        ReadConsistency::Linearizable,
        ReadOptions::default(),
    );
    until(&mut replica.owner, |_| {
        replica.queries.load(Ordering::Acquire) == 1
    });
    assert_eq!(replica.owner.metrics().unwrap().completed_query_reads, 1);
    assert_eq!(sender.retained().reads, 1);
    armed.store(true, Ordering::Release);
    let finished = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if collect {
            assert!(matches!(poll(&mut read), Poll::Ready(Ok(_))));
        }
        drop(read);
    }));
    // A deliberately broken notification order must not panic again while
    // workers are being joined during the regression's assertion unwind.
    armed.store(false, Ordering::Release);
    finished.unwrap();
    assert_eq!(observed.load(Ordering::Acquire), 1);
    assert_eq!(replica.owner.metrics().unwrap().completed_query_reads, 0);
}

#[test]
fn collecting_a_completed_proof_notifies_after_client_credit_release() {
    released_before_notification(true);
}

#[test]
fn dropping_a_completed_proof_notifies_after_client_credit_release() {
    released_before_notification(false);
}
