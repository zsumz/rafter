//! Bounded off-thread destruction for compacted in-memory WAL entries.

use std::{
    fmt,
    sync::mpsc::{self, SyncSender, TrySendError},
    thread::{self, JoinHandle},
};

pub(super) struct EntryDropper<T: Send + 'static> {
    worker: Option<Worker<T>>,
}

impl<T: Send + 'static> EntryDropper<T> {
    pub(super) fn new() -> Self {
        Self { worker: None }
    }

    pub(super) fn retire(&mut self, entries: Vec<T>) {
        if entries.is_empty() {
            return;
        }
        let Some(worker) = self.worker.as_mut() else {
            match Worker::spawn(entries) {
                Ok(worker) => self.worker = Some(worker),
                Err(entries) => drop(entries),
            }
            return;
        };
        let Some(sender) = worker.sender.as_ref() else {
            drop(entries);
            return;
        };
        match sender.try_send(entries) {
            Ok(()) => {}
            Err(TrySendError::Full(entries) | TrySendError::Disconnected(entries)) => drop(entries),
        }
    }

    #[cfg(test)]
    pub(super) fn worker_started(&self) -> bool {
        self.worker.is_some()
    }
}

impl<T: Send + 'static> fmt::Debug for EntryDropper<T> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EntryDropper")
            .field("worker_started", &self.worker.is_some())
            .finish()
    }
}

struct Worker<T: Send + 'static> {
    sender: Option<SyncSender<Vec<T>>>,
    handle: Option<JoinHandle<()>>,
}

impl<T: Send + 'static> Worker<T> {
    fn spawn(entries: Vec<T>) -> Result<Self, Vec<T>> {
        let (sender, receiver) = mpsc::sync_channel::<Vec<T>>(0);
        let handle = match thread::Builder::new()
            .name("rafter-wal-entry-drop".to_owned())
            .spawn(move || {
                while let Ok(entries) = receiver.recv() {
                    drop(entries);
                }
            }) {
            Ok(handle) => handle,
            Err(_) => return Err(entries),
        };
        if let Err(error) = sender.send(entries) {
            let _ = handle.join();
            return Err(error.0);
        }
        Ok(Self {
            sender: Some(sender),
            handle: Some(handle),
        })
    }
}

impl<T: Send + 'static> Drop for Worker<T> {
    fn drop(&mut self) {
        drop(self.sender.take());
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
#[path = "retirement_test.rs"]
mod tests;
