//! Bounded prefix dispatch preserves the owner's unsubmitted application work.

use super::ApplicationWorker;
use crate::application::{ApplicationEntry, ApplicationSubmitRejection, DurableApplication};
use std::collections::VecDeque;

impl<T, S> ApplicationWorker<T, S>
where
    T: ApplicationEntry,
    S: DurableApplication<T>,
{
    /// Transfers a prefix sized to current entry and byte budgets.
    ///
    /// One call submits at most one application batch and never waits for
    /// credits. It removes only the successfully submitted items, preserving
    /// the order and ownership of the remainder. An empty queue returns zero
    /// while the worker is accepting. The caller still owns its queue limit
    /// and must retain committed work until it is submitted or recovered.
    ///
    /// On `Full`, consume or poll completion events before retrying. A single
    /// item larger than the worker's total retained-byte capacity can never
    /// fit; the embedding must align command admission with worker limits.
    /// This method does not acknowledge clients: only verified application
    /// completion crosses that fence.
    ///
    /// # Errors
    ///
    /// Returns the same refusals as [`Self::try_submit`] for the selected
    /// prefix. Every error leaves the entire caller queue unchanged, including
    /// when the worker stops between prefix selection and ownership transfer.
    /// An invalid or oversized item beyond the selected prefix is examined on
    /// a later call; earlier successful prefixes remain accepted.
    pub fn try_submit_prefix(
        &self,
        entries: &mut VecDeque<T>,
    ) -> Result<usize, ApplicationSubmitRejection> {
        if !self.is_accepting() {
            return Err(ApplicationSubmitRejection::Stopped);
        }
        if entries.is_empty() {
            return Ok(0);
        }
        let (available_entries, available_bytes) = self.available();
        let limit = available_entries.min(self.options.batch_entries);
        let mut retained_bytes = 0_usize;
        let mut batch_bytes = 0_usize;
        let mut count = 0;
        for entry in entries.iter().take(limit) {
            let Some(retained) = retained_bytes.checked_add(entry.retained_bytes()) else {
                break;
            };
            let Some(batch) = batch_bytes.checked_add(entry.batch_bytes()) else {
                break;
            };
            if retained > available_bytes || batch > self.options.batch_bytes {
                break;
            }
            retained_bytes = retained;
            batch_bytes = batch;
            count += 1;
        }
        // A zero-sized fit still checks one item through ordinary admission:
        // it returns the precise Full, size, range, or stopped refusal. The
        // temporary vector remains bounded to one item even with no credits.
        let count = count.max(1);
        let prefix = entries.drain(..count).collect();
        match self.try_submit(prefix) {
            Ok(()) => Ok(count),
            Err(error) => {
                let rejection = error.rejection();
                for entry in error.into_entries().into_iter().rev() {
                    entries.push_front(entry);
                }
                Err(rejection)
            }
        }
    }
}
