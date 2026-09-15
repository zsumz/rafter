//! Contiguous nonblocking application admission.

use super::{ApplicationWorker, Work};
use crate::application::{
    ApplicationEntry, ApplicationSubmitError, ApplicationSubmitRejection, DurableApplication,
};
use rafter::LogIndex;

#[derive(Clone, Copy, Debug)]
struct PreparedSubmission {
    first_index: LogIndex,
    last_index: LogIndex,
    retained_bytes: usize,
    retained_bytes_overflowed: bool,
    batch_bytes: usize,
}

impl<T, S> ApplicationWorker<T, S>
where
    T: ApplicationEntry,
    S: DurableApplication<T>,
{
    /// Transfers one nonempty contiguous batch without waiting for queue room.
    ///
    /// The last accepted index reserves the next submission boundary even
    /// while earlier batches are still running. Every refusal returns the exact
    /// submitted entries to the caller.
    ///
    /// # Errors
    ///
    /// Returns an ownership-preserving refusal for empty, noncontiguous,
    /// exhausted-index, oversized, over-capacity, or stopped submissions.
    pub fn try_submit(&self, entries: Vec<T>) -> Result<(), ApplicationSubmitError<T>> {
        let reject = |entries, rejection| ApplicationSubmitError { entries, rejection };
        let prepared = self.prepare_submission(&entries);
        let mut shared = self.lock_shared();
        if !shared.accepting {
            return Err(reject(entries, ApplicationSubmitRejection::Stopped));
        }
        let prepared = match prepared {
            Ok(prepared) => prepared,
            Err(rejection) => return Err(reject(entries, rejection)),
        };
        let Some(expected) = checked_next(shared.accepted_through) else {
            return Err(reject(
                entries,
                ApplicationSubmitRejection::IndexExhausted {
                    after: shared.accepted_through,
                },
            ));
        };
        if prepared.first_index != expected {
            return Err(reject(
                entries,
                ApplicationSubmitRejection::NonContiguous {
                    expected,
                    actual: prepared.first_index,
                },
            ));
        }
        let available_entries = self
            .options
            .inflight_entries
            .saturating_sub(shared.inflight_entries);
        let available_bytes = self
            .options
            .inflight_bytes
            .saturating_sub(shared.inflight_bytes);
        if entries.len() > available_entries
            || prepared.retained_bytes_overflowed
            || prepared.retained_bytes > available_bytes
        {
            return Err(reject(
                entries,
                ApplicationSubmitRejection::Full {
                    available_entries,
                    available_bytes,
                },
            ));
        }

        let previous = shared.accepted_through;
        shared.accepted_through = prepared.last_index;
        shared.inflight_entries += entries.len();
        shared.inflight_bytes += prepared.retained_bytes;
        let work = Work {
            entries,
            retained_bytes: prepared.retained_bytes,
            batch_bytes: prepared.batch_bytes,
            last_index: prepared.last_index,
        };
        let Some(requests) = &self.requests else {
            rollback(&mut shared, previous, &work);
            return Err(reject(work.entries, ApplicationSubmitRejection::Stopped));
        };
        if let Err(error) = requests.send(work) {
            let work = error.0;
            rollback(&mut shared, previous, &work);
            return Err(reject(work.entries, ApplicationSubmitRejection::Stopped));
        }
        Ok(())
    }

    fn prepare_submission(
        &self,
        entries: &[T],
    ) -> Result<PreparedSubmission, ApplicationSubmitRejection> {
        let Some(first) = entries.first() else {
            return Err(ApplicationSubmitRejection::Empty);
        };
        let first_index = first.log_index();
        let mut last_index = first_index;
        let mut retained_bytes = first.retained_bytes();
        let mut retained_bytes_overflowed = false;
        let mut batch_bytes = first.batch_bytes();
        let mut batch_bytes_overflowed = false;
        for entry in entries.iter().skip(1) {
            let actual = entry.log_index();
            let Some(expected) = checked_next(last_index) else {
                return Err(ApplicationSubmitRejection::IndexExhausted { after: last_index });
            };
            if actual != expected {
                return Err(ApplicationSubmitRejection::NonContiguous { expected, actual });
            }
            last_index = actual;
            if let Some(combined) = retained_bytes.checked_add(entry.retained_bytes()) {
                retained_bytes = combined;
            } else {
                retained_bytes = usize::MAX;
                retained_bytes_overflowed = true;
            }
            let Some(combined) = batch_bytes.checked_add(entry.batch_bytes()) else {
                batch_bytes = usize::MAX;
                batch_bytes_overflowed = true;
                continue;
            };
            batch_bytes = combined;
        }
        if batch_bytes_overflowed
            || entries.len() > self.options.batch_entries
            || batch_bytes > self.options.batch_bytes
        {
            return Err(ApplicationSubmitRejection::BatchTooLarge {
                entries: entries.len(),
                bytes: batch_bytes,
                max_entries: self.options.batch_entries,
                max_bytes: self.options.batch_bytes,
            });
        }
        Ok(PreparedSubmission {
            first_index,
            last_index,
            retained_bytes,
            retained_bytes_overflowed,
            batch_bytes,
        })
    }
}

fn rollback<T>(shared: &mut super::Shared, previous: LogIndex, work: &Work<T>) {
    shared.accepted_through = previous;
    shared.inflight_entries -= work.entries.len();
    shared.inflight_bytes -= work.retained_bytes;
    shared.accepting = false;
}

fn checked_next(index: LogIndex) -> Option<LogIndex> {
    index.0.checked_add(1).map(LogIndex)
}
