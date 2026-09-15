//! Drain-only-ready application batching and failure ownership return.

use super::{Shared, Work, WorkerEvent};
use crate::application::{
    ApplicationCompletion, ApplicationEntry, ApplicationEvent, ApplicationFailure,
    ApplicationFailureKind, ApplicationWorkerOptions, DurableApplication,
};
use std::{
    collections::VecDeque,
    panic::{catch_unwind, AssertUnwindSafe},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{Receiver, Sender},
        Arc, Mutex,
    },
};

pub(super) struct State<T, S, F>
where
    T: ApplicationEntry,
    S: DurableApplication<T>,
    F: Fn(),
{
    pub store: S,
    pub receive: Receiver<Work<T>>,
    pub send: Sender<WorkerEvent<T, S>>,
    pub shared: Arc<Mutex<Shared>>,
    pub durable_through: Arc<AtomicU64>,
    pub applying_through: Arc<AtomicU64>,
    pub options: ApplicationWorkerOptions,
    pub wake: F,
}

struct TerminationGuard<E, F: Fn()> {
    send: Option<Sender<E>>,
    shared: Arc<Mutex<Shared>>,
    applying_through: Arc<AtomicU64>,
    wake: F,
}

impl<E, F: Fn()> TerminationGuard<E, F> {
    fn send(&self, event: E) -> Result<(), E> {
        let Some(send) = &self.send else {
            return Err(event);
        };
        send.send(event).map_err(|error| error.0)
    }

    fn notify(&self) {
        (self.wake)();
    }
}

impl<E, F: Fn()> Drop for TerminationGuard<E, F> {
    fn drop(&mut self) {
        self.applying_through.store(0, Ordering::Release);
        self.shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .accepting = false;
        self.send.take();
        if std::thread::panicking() {
            let _ = catch_unwind(AssertUnwindSafe(|| (self.wake)()));
        } else {
            (self.wake)();
        }
    }
}

impl<T, S, F> State<T, S, F>
where
    T: ApplicationEntry,
    S: DurableApplication<T>,
    F: Fn(),
{
    pub(super) fn run(self) -> S {
        let Self {
            mut store,
            receive,
            send,
            shared,
            durable_through,
            applying_through,
            options,
            wake,
        } = self;
        let terminal = TerminationGuard {
            send: Some(send),
            shared: Arc::clone(&shared),
            applying_through: Arc::clone(&applying_through),
            wake,
        };
        let mut deferred = VecDeque::new();
        while let Some(mut work) = deferred.pop_front().or_else(|| receive.recv().ok()) {
            Self::drain_ready(&receive, options, &mut work, &mut deferred);
            let final_index = work.last_index;
            applying_through.store(final_index.0, Ordering::Release);
            let expected_outcomes = work.entries.len();
            let event = match store.apply(&work.entries) {
                Err(error) => Self::failure(
                    &shared,
                    &receive,
                    work,
                    ApplicationFailureKind::Store(error),
                    &mut deferred,
                ),
                Ok(outcomes) if outcomes.len() != work.entries.len() => {
                    let actual = outcomes.len();
                    Self::failure(
                        &shared,
                        &receive,
                        work,
                        ApplicationFailureKind::OutcomeCount {
                            expected: expected_outcomes,
                            actual,
                        },
                        &mut deferred,
                    )
                }
                Ok(outcomes) => {
                    let actual = store.applied_through();
                    if actual == final_index {
                        durable_through.store(final_index.0, Ordering::Release);
                        ApplicationEvent::Applied(ApplicationCompletion {
                            entries: work.entries,
                            outcomes,
                            retained_bytes: work.retained_bytes,
                        })
                    } else {
                        Self::failure(
                            &shared,
                            &receive,
                            work,
                            ApplicationFailureKind::DurableFloor {
                                expected: final_index,
                                actual,
                            },
                            &mut deferred,
                        )
                    }
                }
            };
            applying_through.store(0, Ordering::Release);
            let failed = matches!(event, ApplicationEvent::Failed(_));
            if terminal.send(event).is_err() {
                break;
            }
            terminal.notify();
            if failed {
                break;
            }
        }
        store
    }

    fn drain_ready(
        receive: &Receiver<Work<T>>,
        options: ApplicationWorkerOptions,
        work: &mut Work<T>,
        deferred: &mut VecDeque<Work<T>>,
    ) {
        loop {
            let Ok(next) = receive.try_recv() else {
                return;
            };
            let combined_entries = work.entries.len().saturating_add(next.entries.len());
            let Some(combined_batch_bytes) = work.batch_bytes.checked_add(next.batch_bytes) else {
                deferred.push_back(next);
                return;
            };
            if combined_entries > options.batch_entries
                || combined_batch_bytes > options.batch_bytes
            {
                deferred.push_back(next);
                return;
            }
            work.entries.extend(next.entries);
            work.retained_bytes = work.retained_bytes.saturating_add(next.retained_bytes);
            work.batch_bytes = combined_batch_bytes;
            work.last_index = next.last_index;
        }
    }

    fn failure(
        shared: &Arc<Mutex<Shared>>,
        receive: &Receiver<Work<T>>,
        work: Work<T>,
        kind: ApplicationFailureKind<S::Error>,
        deferred: &mut VecDeque<Work<T>>,
    ) -> ApplicationEvent<T, S::Outcome, S::Error> {
        let mut shared = shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        shared.accepting = false;

        let mut unattempted = Vec::new();
        let mut retained_bytes = work.retained_bytes;
        for queued in deferred.drain(..).chain(receive.try_iter()) {
            retained_bytes = retained_bytes.saturating_add(queued.retained_bytes);
            unattempted.extend(queued.entries);
        }
        drop(shared);
        ApplicationEvent::Failed(ApplicationFailure {
            attempted: work.entries,
            unattempted,
            kind,
            retained_bytes,
        })
    }
}
