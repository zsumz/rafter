//! Drain-only-ready application batching and failure ownership return.

use super::{Shared, Work};
use crate::application::{
    ApplicationCompletion, ApplicationEntry, ApplicationEvent, ApplicationFailure,
    ApplicationFailureKind, ApplicationWorkerOptions, DurableApplication,
};
use std::{
    collections::VecDeque,
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
    pub send: Sender<ApplicationEvent<T, S::Outcome, S::Error>>,
    pub shared: Arc<Mutex<Shared>>,
    pub durable_through: Arc<AtomicU64>,
    pub applying_through: Arc<AtomicU64>,
    pub options: ApplicationWorkerOptions,
    pub wake: F,
}

struct AdmissionGuard {
    shared: Arc<Mutex<Shared>>,
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .accepting = false;
    }
}

impl<T, S, F> State<T, S, F>
where
    T: ApplicationEntry,
    S: DurableApplication<T>,
    F: Fn(),
{
    pub fn run(mut self) {
        let _admission = AdmissionGuard {
            shared: Arc::clone(&self.shared),
        };
        let mut deferred = VecDeque::new();
        while let Some(mut work) = deferred.pop_front().or_else(|| self.receive.recv().ok()) {
            self.drain_ready(&mut work, &mut deferred);
            let final_index = work.last_index;
            self.applying_through
                .store(final_index.0, Ordering::Release);
            let expected_outcomes = work.entries.len();
            let event = match self.store.apply(&work.entries) {
                Err(error) => {
                    self.failure(work, ApplicationFailureKind::Store(error), &mut deferred)
                }
                Ok(outcomes) if outcomes.len() != work.entries.len() => {
                    let actual = outcomes.len();
                    self.failure(
                        work,
                        ApplicationFailureKind::OutcomeCount {
                            expected: expected_outcomes,
                            actual,
                        },
                        &mut deferred,
                    )
                }
                Ok(outcomes) => {
                    let actual = self.store.applied_through();
                    if actual == final_index {
                        self.durable_through.store(final_index.0, Ordering::Release);
                        ApplicationEvent::Applied(ApplicationCompletion {
                            entries: work.entries,
                            outcomes,
                            retained_bytes: work.retained_bytes,
                        })
                    } else {
                        self.failure(
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
            self.applying_through.store(0, Ordering::Release);
            let failed = matches!(event, ApplicationEvent::Failed(_));
            if self.send.send(event).is_err() {
                return;
            }
            (self.wake)();
            if failed {
                return;
            }
        }
    }

    fn drain_ready(&self, work: &mut Work<T>, deferred: &mut VecDeque<Work<T>>) {
        loop {
            let Ok(next) = self.receive.try_recv() else {
                return;
            };
            let combined_entries = work.entries.len().saturating_add(next.entries.len());
            let Some(combined_batch_bytes) = work.batch_bytes.checked_add(next.batch_bytes) else {
                deferred.push_back(next);
                return;
            };
            if combined_entries > self.options.batch_entries
                || combined_batch_bytes > self.options.batch_bytes
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
        &self,
        work: Work<T>,
        kind: ApplicationFailureKind<S::Error>,
        deferred: &mut VecDeque<Work<T>>,
    ) -> ApplicationEvent<T, S::Outcome, S::Error> {
        let mut shared = self
            .shared
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        shared.accepting = false;

        let mut unattempted = Vec::new();
        let mut retained_bytes = work.retained_bytes;
        for queued in deferred.drain(..).chain(self.receive.try_iter()) {
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
