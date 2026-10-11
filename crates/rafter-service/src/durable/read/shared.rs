//! Query bytes and credits outlive cancellation while a quorum still owns them.

use super::{DurableReadLimits, DurableReadStats};
use crate::{
    durable::{budget::Budget, lock, reply::Reply},
    ErrorCause, QueryReceipt, ReadError, ReadOptions,
};
use rafter::ReadId;
use rafter_app::read::{ReadConsistency, ReadProof};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    task::Waker,
};

mod observe;

type ResultOf<R> = Result<QueryReceipt<(), R>, ReadError>;

#[derive(Clone)]
pub(in crate::durable) enum Phase {
    Queued,
    Barrier,
    Ready(Option<ReadProof<()>>),
}
struct Entry<R> {
    payload: Arc<Vec<u8>>,
    options: ReadOptions,
    phase: Phase,
    reply: Arc<Reply<ResultOf<R>>>,
}
struct State<R> {
    open: bool,
    next: Option<u64>,
    queue: VecDeque<ReadId>,
    entries: BTreeMap<ReadId, Entry<R>>,
}
pub(in crate::durable) struct ReadShared<R> {
    state: Mutex<State<R>>,
    budget: Arc<Budget>,
    completed_proofs: Arc<AtomicUsize>,
    limits: DurableReadLimits,
    pub(in crate::durable) wake: Arc<dyn Fn() + Send + Sync>,
}
pub(in crate::durable) struct Work {
    pub id: ReadId,
    pub payload: Arc<Vec<u8>>,
    pub options: ReadOptions,
    pub phase: Phase,
}
impl<R> ReadShared<R> {
    pub(in crate::durable) fn new(
        limits: DurableReadLimits,
        wake: Arc<dyn Fn() + Send + Sync>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                open: true,
                next: Some(1),
                queue: VecDeque::new(),
                entries: BTreeMap::new(),
            }),
            budget: Arc::new(Budget::default()),
            completed_proofs: Arc::new(AtomicUsize::new(0)),
            limits,
            wake,
        })
    }

    pub(super) fn admit(
        &self,
        query: Vec<u8>,
        consistency: ReadConsistency,
        options: ReadOptions,
    ) -> Result<Arc<Reply<ResultOf<R>>>, ReadError> {
        let mut state = lock(&self.state);
        if !state.open {
            return Err(ReadError::ShuttingDown);
        }
        let phase = match consistency {
            ReadConsistency::Linearizable => Phase::Queued,
            ReadConsistency::Local => Phase::Ready(None),
            _ => return Err(ReadError::UnsupportedConsistency { consistency }),
        };
        let credit = self
            .budget
            .reserve(
                query.capacity(),
                self.limits.reads.get(),
                self.limits.payload_bytes.get(),
            )
            .ok_or_else(|| ReadError::Transport {
                cause: ErrorCause::new(AdmissionFull),
            })?;
        let next = state.next.ok_or(ReadError::ReadIdExhausted)?;
        state.next = next.checked_add(1);
        let id = ReadId(next);
        let reply = Reply::new(credit, Arc::clone(&self.wake));
        state.entries.insert(
            id,
            Entry {
                payload: Arc::new(query),
                options,
                phase,
                reply: Arc::clone(&reply),
            },
        );
        state.queue.push_back(id);
        Ok(reply)
    }

    pub(in crate::durable) fn stats(&self) -> DurableReadStats {
        let (reads, payload_bytes) = self.budget.stats();
        DurableReadStats {
            reads,
            payload_bytes,
        }
    }

    pub(in crate::durable) fn close(&self) {
        lock(&self.state).open = false;
    }

    pub(in crate::durable) fn discard_canceled(&self) {
        let mut discarded = Vec::new();
        {
            let mut state = lock(&self.state);
            let State { queue, entries, .. } = &mut *state;
            queue.retain(|id| {
                if entries.get(id).is_some_and(|e| {
                    !matches!(e.phase, Phase::Barrier) && e.reply.canceled.load(Ordering::Acquire)
                }) {
                    discarded.extend(entries.remove(id));
                    false
                } else {
                    true
                }
            });
        }
        drop(discarded);
    }

    pub(in crate::durable) fn next_work(&self) -> Option<Work> {
        let mut state = lock(&self.state);
        for _ in 0..state.queue.len() {
            let id = state.queue.pop_front()?;
            state.queue.push_back(id);
            let entry = state.entries.get(&id)?;
            if !matches!(entry.phase, Phase::Barrier) {
                return Some(Work {
                    id,
                    payload: Arc::clone(&entry.payload),
                    options: entry.options,
                    phase: entry.phase.clone(),
                });
            }
        }
        None
    }

    pub(in crate::durable) fn phase(&self, id: ReadId, phase: Phase) {
        if let Some(entry) = lock(&self.state).entries.get_mut(&id) {
            entry.phase = phase;
        }
    }

    pub(in crate::durable) fn finish(&self, id: ReadId, outcome: ResultOf<R>) {
        let entry = {
            let mut state = lock(&self.state);
            state.queue.retain(|queued| *queued != id);
            state.entries.remove(&id)
        };
        if let Some(entry) = entry {
            drop(entry.payload);
            let count = outcome
                .as_ref()
                .ok()
                .filter(|receipt| receipt.proof.is_some())
                .map(|_| Arc::clone(&self.completed_proofs));
            entry.reply.finish_counted(outcome, count);
        }
    }

    pub(in crate::durable) fn terminate(&self) -> Vec<Waker> {
        let entries = {
            let mut state = lock(&self.state);
            state.open = false;
            state.queue.clear();
            std::mem::take(&mut state.entries)
        };
        let mut wakers = Vec::new();
        for (_, entry) in entries {
            drop(entry.payload);
            if let Some(waker) = entry.reply.complete(Err(ReadError::ShuttingDown)) {
                wakers.push(waker);
            }
        }
        wakers
    }
}

#[derive(Debug)]
struct AdmissionFull;
impl fmt::Display for AdmissionFull {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("durable read admission is full")
    }
}
impl std::error::Error for AdmissionFull {}
