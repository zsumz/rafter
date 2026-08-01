//! Bounded live receiver ownership and continuous finished-worker reaping.

use std::{
    net::{Shutdown, TcpStream},
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

#[derive(Debug, Default)]
pub(crate) struct ReceiverRegistry {
    state: Mutex<RegistryState>,
}

#[derive(Debug, Default)]
struct RegistryState {
    active: Vec<ReceiverWorker>,
    panicked: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct ReceiverWorker {
    pub(crate) name: String,
    pub(crate) handle: JoinHandle<()>,
    pub(crate) shutdown_socket: Arc<TcpStream>,
}

impl ReceiverWorker {
    pub(crate) fn shut_down(&self) {
        let _ = self.shutdown_socket.shutdown(Shutdown::Both);
    }

    pub(crate) fn join(self) -> Option<String> {
        self.handle.join().is_err().then_some(self.name)
    }
}

impl ReceiverRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn push(&self, worker: ReceiverWorker) -> Result<(), ReceiverWorker> {
        let Ok(mut state) = self.state.lock() else {
            return Err(worker);
        };
        state.active.push(worker);
        Ok(())
    }

    pub(crate) fn reap_finished(&self) -> Result<(), ()> {
        let finished = {
            let mut state = self.state.lock().map_err(|_| ())?;
            let mut finished = Vec::new();
            let mut index = 0;
            while index < state.active.len() {
                if state.active[index].handle.is_finished() {
                    finished.push(state.active.swap_remove(index));
                } else {
                    index += 1;
                }
            }
            finished
        };
        self.join_workers(finished)
    }

    pub(crate) fn shutdown_all(&self) -> Result<(), ()> {
        let state = self.state.lock().map_err(|_| ())?;
        for worker in &state.active {
            worker.shut_down();
        }
        Ok(())
    }

    pub(crate) fn join_all(&self) -> Result<Vec<String>, ()> {
        let active = {
            let mut state = self.state.lock().map_err(|_| ())?;
            std::mem::take(&mut state.active)
        };
        self.join_workers(active)?;
        self.state
            .lock()
            .map(|mut state| std::mem::take(&mut state.panicked))
            .map_err(|_| ())
    }

    fn join_workers(&self, workers: Vec<ReceiverWorker>) -> Result<(), ()> {
        let mut panicked = Vec::new();
        for worker in workers {
            if let Some(name) = worker.join() {
                panicked.push(name);
            }
        }
        if !panicked.is_empty() {
            self.state.lock().map_err(|_| ())?.panicked.extend(panicked);
        }
        Ok(())
    }
}
