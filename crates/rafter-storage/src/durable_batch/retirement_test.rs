//! Concurrency and queue-bound proof for compacted-entry destruction.

use super::EntryDropper;
use std::{
    sync::{mpsc, Arc, Barrier},
    thread,
};

struct Probe {
    id: u8,
    events: mpsc::Sender<(u8, thread::ThreadId)>,
    block: Option<Arc<Barrier>>,
}

impl Drop for Probe {
    fn drop(&mut self) {
        self.events.send((self.id, thread::current().id())).unwrap();
        if let Some(block) = &self.block {
            block.wait();
        }
    }
}

#[test]
fn one_batch_drops_off_thread_and_a_busy_worker_cannot_queue_another() {
    let current = thread::current().id();
    let (events, received) = mpsc::channel();
    let block = Arc::new(Barrier::new(2));
    let mut dropper = EntryDropper::new();
    dropper.retire(vec![Probe {
        id: 1,
        events: events.clone(),
        block: Some(block.clone()),
    }]);
    let first = received.recv().unwrap();
    assert_ne!(first.1, current);

    dropper.retire(vec![Probe {
        id: 2,
        events,
        block: None,
    }]);
    assert_eq!(received.recv().unwrap(), (2, current));
    block.wait();
    drop(dropper);
}
