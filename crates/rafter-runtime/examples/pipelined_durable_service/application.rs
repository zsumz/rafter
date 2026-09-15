//! Durable application adapter for the public ordered worker.

use std::{
    io,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, PoisonError},
};

use rafter::{LocalProposalId, LogIndex, NodeId};
use rafter_runtime::application::{
    ApplicationEntry, ApplicationWorker, ApplicationWorkerOptions, DurableApplication,
};

use super::{
    app_state::{load_app_state, try_persist_app_state, AppState},
    codec::apply_set,
    storage::node_dir,
};

#[derive(Debug)]
pub(crate) struct AppliedCommand {
    pub index: LogIndex,
    pub payload: Option<Vec<u8>>,
    pub proposal_id: Option<LocalProposalId>,
}

impl ApplicationEntry for AppliedCommand {
    fn log_index(&self) -> LogIndex {
        self.index
    }

    fn retained_bytes(&self) -> usize {
        128_usize.saturating_add(
            self.payload
                .as_ref()
                .map_or(0, |payload| payload.len().saturating_mul(2)),
        )
    }

    fn batch_bytes(&self) -> usize {
        64_usize.saturating_add(self.payload.as_ref().map_or(0, Vec::len))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct AppliedOutcome {
    pub proposal_id: Option<LocalProposalId>,
}

#[derive(Debug)]
pub(crate) struct Store {
    directory: PathBuf,
    state: SharedState,
}

pub(crate) type SharedState = Arc<Mutex<AppState>>;
pub(crate) type Worker = ApplicationWorker<AppliedCommand, Store>;

impl DurableApplication<AppliedCommand> for Store {
    type Outcome = AppliedOutcome;
    type Error = io::Error;

    fn applied_through(&self) -> LogIndex {
        lock(&self.state).applied
    }

    fn apply(&mut self, entries: &[AppliedCommand]) -> Result<Vec<Self::Outcome>, Self::Error> {
        let mut state = lock(&self.state);
        let mut outcomes = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(payload) = &entry.payload {
                let command = std::str::from_utf8(payload).map_err(io::Error::other)?;
                apply_set(command, &mut state.kv);
            }
            state.applied = entry.index;
            outcomes.push(AppliedOutcome {
                proposal_id: entry.proposal_id,
            });
        }
        try_persist_app_state(&self.directory, &state.kv, state.applied)?;
        Ok(outcomes)
    }
}

impl Store {
    fn install_snapshot(
        &mut self,
        kv: std::collections::BTreeMap<String, String>,
        applied: LogIndex,
    ) {
        try_persist_app_state(&self.directory, &kv, applied)
            .expect("persist installed application snapshot");
        *lock(&self.state) = AppState { kv, applied };
    }
}

pub(crate) fn open(root: &Path, node_id: NodeId) -> (SharedState, Worker) {
    let state = Arc::new(Mutex::new(load_app_state(root, node_id)));
    let store = Store {
        directory: node_dir(root, node_id),
        state: Arc::clone(&state),
    };
    let worker = start(store);
    (state, worker)
}

pub(crate) fn install_snapshot(
    worker: &mut Worker,
    kv: std::collections::BTreeMap<String, String>,
    applied: LogIndex,
) -> SharedState {
    let mut store = worker
        .shutdown_into_store()
        .expect("idle application worker returns its store");
    store.install_snapshot(kv, applied);
    let state = Arc::clone(&store.state);
    *worker = start(store);
    state
}

pub(crate) fn snapshot(state: &SharedState) -> AppState {
    lock(state).clone()
}

fn lock(state: &SharedState) -> std::sync::MutexGuard<'_, AppState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

fn start(store: Store) -> Worker {
    ApplicationWorker::start(store, ApplicationWorkerOptions::new(), || {})
        .expect("start bounded application worker")
}
