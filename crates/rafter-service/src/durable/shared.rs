//! Admission and retained client state, separate from the owned backend.

use super::{budget::Budget, lock, reply::Reply};
use crate::{ErrorCause, UnknownOutcomeReason, WriteError, WriteFate, WriteOptions, WriteReceipt};
use rafter::{LocalProposalId, LogIndex, Term};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    num::NonZeroUsize,
    sync::{atomic::Ordering, Arc, Mutex},
};

/// Bounds spanning queued writes, protocol ownership, and uncollected results.
#[derive(Clone, Copy, Debug)]
pub struct DurableWriteLimits {
    /// Maximum retained writes, including canceled writes still owned by Raft.
    pub writes: NonZeroUsize,
    /// Maximum sum of admitted encoded `Vec` capacities, held until collection.
    /// Does not bound arbitrary application outcomes or the kernel log.
    pub payload_bytes: NonZeroUsize,
}

/// Current retained credits, including completed outcomes awaiting collection.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DurableWriteStats {
    /// Number of retained writes.
    pub writes: usize,
    /// Encoded payload capacities charged to those writes.
    pub payload_bytes: usize,
}

type WriteResult<O> = Result<WriteReceipt<O>, WriteError>;

pub(super) struct Entry<O> {
    // Payload destruction must precede releasing the reply's credit.
    pub payload: Option<Vec<u8>>,
    pub options: WriteOptions,
    pub appended: Option<(LogIndex, Term)>,
    pub reply: Arc<Reply<WriteResult<O>>>,
}
pub(super) struct State<O> {
    pub open: bool,
    pub next: Option<u64>,
    pub queue: VecDeque<LocalProposalId>,
    pub entries: BTreeMap<LocalProposalId, Entry<O>>,
    pub indexes: BTreeMap<LogIndex, LocalProposalId>,
}
pub(super) struct Shared<O> {
    pub state: Mutex<State<O>>,
    budget: Arc<Budget>,
    limits: DurableWriteLimits,
    max_payload: usize,
    pub wake: Arc<dyn Fn() + Send + Sync>,
}

impl<O> Shared<O> {
    pub(super) fn new(
        limits: DurableWriteLimits,
        max_payload: usize,
        wake: Arc<dyn Fn() + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                open: true,
                next: Some(1),
                queue: VecDeque::new(),
                entries: BTreeMap::new(),
                indexes: BTreeMap::new(),
            }),
            budget: Arc::new(Budget::default()),
            limits,
            max_payload,
            wake,
        })
    }

    pub(super) fn admit(
        &self,
        payload: Vec<u8>,
        options: WriteOptions,
    ) -> Result<Arc<Reply<WriteResult<O>>>, WriteError> {
        let mut state = lock(&self.state);
        if !state.open {
            return Err(WriteError::ShuttingDown);
        }
        if payload.len() > self.max_payload {
            return Err(WriteError::PayloadTooLarge {
                max: self.max_payload,
                actual: payload.len(),
            });
        }
        let credit = self
            .budget
            .reserve(
                payload.capacity(),
                self.limits.writes.get(),
                self.limits.payload_bytes.get(),
            )
            .ok_or_else(|| WriteError::Transport {
                fate: WriteFate::NotAppended,
                cause: ErrorCause::new(AdmissionFull),
            })?;
        let next = state.next.ok_or(WriteError::LocalProposalIdExhausted)?;
        state.next = next.checked_add(1);
        let reply = Reply::new(credit, Arc::clone(&self.wake));
        let id = LocalProposalId(next);
        state.entries.insert(
            id,
            Entry {
                payload: Some(payload),
                options,
                appended: None,
                reply: Arc::clone(&reply),
            },
        );
        state.queue.push_back(id);
        Ok(reply)
    }

    pub(super) fn stats(&self) -> DurableWriteStats {
        let (writes, payload_bytes) = self.budget.stats();
        DurableWriteStats {
            writes,
            payload_bytes,
        }
    }

    pub(super) fn close(&self) {
        lock(&self.state).open = false;
    }

    pub(super) fn discard_canceled_queued(&self) {
        let mut discarded = Vec::new();
        {
            let mut state = lock(&self.state);
            let State { queue, entries, .. } = &mut *state;
            queue.retain(|id| {
                if entries
                    .get(id)
                    .is_some_and(|entry| entry.reply.canceled.load(Ordering::Acquire))
                {
                    discarded.extend(entries.remove(id));
                    false
                } else {
                    true
                }
            });
        }
        drop(discarded);
    }

    pub(super) fn finish(&self, id: LocalProposalId, outcome: Result<WriteReceipt<O>, WriteError>) {
        let entry = {
            let mut state = lock(&self.state);
            let entry = state.entries.remove(&id);
            if let Some((index, _)) = entry.as_ref().and_then(|e| e.appended) {
                state.indexes.remove(&index);
            }
            entry
        };
        if let Some(mut entry) = entry {
            drop(entry.payload.take());
            entry.reply.finish(outcome);
        }
    }

    pub(super) fn terminate(&self) -> Vec<std::task::Waker> {
        let entries = {
            let mut state = lock(&self.state);
            state.open = false;
            state.queue.clear();
            state.indexes.clear();
            std::mem::take(&mut state.entries)
        };
        // Publish every terminal answer before invoking any caller code. A
        // panicking waker must not strand other waiters after backend release.
        let mut wakers = Vec::new();
        for (id, mut entry) in entries {
            let error = if entry.payload.is_some() {
                WriteError::ShuttingDown
            } else {
                unknown(id, entry.options, UnknownOutcomeReason::DriverReleased)
            };
            drop(entry.payload.take());
            if let Some(waker) = entry.reply.complete(Err(error)) {
                wakers.push(waker);
            }
        }
        wakers
    }
}

pub(super) fn unknown(
    id: LocalProposalId,
    options: WriteOptions,
    reason: UnknownOutcomeReason,
) -> WriteError {
    WriteError::UnknownOutcome {
        local_proposal_id: id,
        client_request_id: options.client_request_id,
        reason,
    }
}

#[derive(Debug)]
struct AdmissionFull;
impl fmt::Display for AdmissionFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("durable client admission is full")
    }
}
impl std::error::Error for AdmissionFull {}
