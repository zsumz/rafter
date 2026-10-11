//! Publish completion or disconnection before notifying a sleeping owner.

use super::PersistenceWorkerCompletion;
use crate::pipelined::PersistenceWork;
use rafter::SnapshotChunkSource;
use rafter_storage::{telemetry, RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};
use std::{
    panic::{catch_unwind, AssertUnwindSafe},
    sync::mpsc::{Receiver, SyncSender},
};

struct Channels<W, E, F: Fn()> {
    receive: Option<Receiver<W>>,
    send: Option<SyncSender<E>>,
    wake: F,
}

impl<W, E, F: Fn()> Drop for Channels<W, E, F> {
    fn drop(&mut self) {
        // A wake must observe disconnected channels even during unwinding.
        self.receive.take();
        self.send.take();
        if std::thread::panicking() {
            let _ = catch_unwind(AssertUnwindSafe(|| (self.wake)()));
        } else {
            (self.wake)();
        }
    }
}

pub(super) fn run<H, L, S>(
    receive: Receiver<Box<PersistenceWork<H, L, S>>>,
    send: SyncSender<PersistenceWorkerCompletion<H, L, S>>,
    collect_storage_telemetry: bool,
    wake: impl Fn(),
) where
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
{
    let channels = Channels {
        receive: Some(receive),
        send: Some(send),
        wake,
    };
    telemetry::set_enabled(collect_storage_telemetry);
    while let Some(work) = channels.receive.as_ref().and_then(|rx| rx.recv().ok()) {
        let completion = work.persist();
        let storage_telemetry = if collect_storage_telemetry {
            telemetry::snapshot()
        } else {
            Vec::new()
        };
        let Some(send) = &channels.send else {
            break;
        };
        if send
            .try_send(PersistenceWorkerCompletion {
                completion,
                storage_telemetry,
            })
            .is_err()
        {
            break;
        }
        (channels.wake)();
    }
}
