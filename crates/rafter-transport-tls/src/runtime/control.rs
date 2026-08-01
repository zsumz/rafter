//! Shared lifecycle, terminal failure, and degraded-peer state.

use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, AtomicU8, Ordering},
        Mutex,
    },
    time::{Duration, Instant},
};

use crate::{PeerId, TlsTransportStartError, TransportHealth};

const STARTING: u8 = 0;
const RUNNING: u8 = 1;
const STOPPING: u8 = 2;
const FAILED: u8 = 3;
const STOPPED: u8 = 4;
const STOPPING_PAUSED: u8 = 5;

#[derive(Debug)]
pub(crate) struct RuntimeControl {
    lifecycle: AtomicU8,
    shutdown: AtomicBool,
    shutdown_started: Mutex<Option<Instant>>,
    shutdown_grace: Duration,
    degraded_peers: Mutex<BTreeSet<PeerId>>,
    terminal_failure: Mutex<Option<String>>,
}

impl RuntimeControl {
    pub(crate) fn new(shutdown_grace: Duration) -> Self {
        Self {
            lifecycle: AtomicU8::new(STARTING),
            shutdown: AtomicBool::new(false),
            shutdown_started: Mutex::new(None),
            shutdown_grace,
            degraded_peers: Mutex::new(BTreeSet::new()),
            terminal_failure: Mutex::new(None),
        }
    }

    pub(crate) fn start(&self) -> Result<(), TlsTransportStartError> {
        loop {
            match self.lifecycle.load(Ordering::Acquire) {
                STARTING => {
                    if self
                        .lifecycle
                        .compare_exchange(STARTING, RUNNING, Ordering::AcqRel, Ordering::Acquire)
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
                RUNNING => return Ok(()),
                STOPPING | STOPPING_PAUSED => {
                    return Err(TlsTransportStartError::Stopping);
                }
                FAILED => {
                    return Err(TlsTransportStartError::Failed {
                        message: self.terminal_failure(),
                    });
                }
                STOPPED => return Err(TlsTransportStartError::Stopped),
                _ => {
                    return Err(TlsTransportStartError::Failed {
                        message: Some("transport lifecycle state is invalid".to_owned()),
                    });
                }
            }
        }
    }

    pub(crate) fn starting(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == STARTING
    }

    pub(crate) fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        loop {
            let lifecycle = self.lifecycle.load(Ordering::Acquire);
            let next = match lifecycle {
                STARTING => STOPPING_PAUSED,
                RUNNING => STOPPING,
                STOPPING | STOPPING_PAUSED | FAILED | STOPPED => break,
                _ => {
                    self.fail("transport lifecycle state is invalid");
                    break;
                }
            };
            if self
                .lifecycle
                .compare_exchange(lifecycle, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                break;
            }
        }
        match self.shutdown_started.lock() {
            Ok(mut started) => {
                if started.is_none() {
                    *started = Some(Instant::now());
                }
            }
            Err(_) => self.fail("transport shutdown timer is poisoned"),
        }
    }

    pub(crate) fn fail(&self, message: impl Into<String>) {
        if let Ok(mut failure) = self.terminal_failure.lock() {
            if failure.is_none() {
                *failure = Some(message.into());
            }
        }
        self.shutdown.store(true, Ordering::Release);
        self.lifecycle.store(FAILED, Ordering::Release);
    }

    pub(crate) fn mark_stopped(&self) {
        if self.lifecycle.load(Ordering::Acquire) != FAILED {
            self.lifecycle.store(STOPPED, Ordering::Release);
        }
    }

    pub(crate) fn shutdown_requested(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    pub(crate) fn shutdown_grace_expired(&self) -> bool {
        if !self.shutdown_requested() {
            return false;
        }
        match self.shutdown_started.lock() {
            Ok(started) => started
                .as_ref()
                .is_some_and(|started| started.elapsed() >= self.shutdown_grace),
            Err(_) => true,
        }
    }

    pub(crate) fn accepts_send(&self) -> bool {
        matches!(self.lifecycle.load(Ordering::Acquire), STARTING | RUNNING)
    }

    pub(crate) fn stopping_while_paused(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == STOPPING_PAUSED
    }

    pub(crate) fn terminal(&self) -> bool {
        self.lifecycle.load(Ordering::Acquire) == FAILED
    }

    pub(crate) fn mark_degraded(&self, peer: &PeerId) {
        match self.degraded_peers.lock() {
            Ok(mut peers) => {
                peers.insert(peer.clone());
            }
            Err(_) => self.fail("transport degraded-peer state is poisoned"),
        }
    }

    pub(crate) fn mark_connected(&self, peer: &PeerId) {
        match self.degraded_peers.lock() {
            Ok(mut peers) => {
                peers.remove(peer);
            }
            Err(_) => self.fail("transport degraded-peer state is poisoned"),
        }
    }

    pub(crate) fn health(&self) -> TransportHealth {
        match self.lifecycle.load(Ordering::Acquire) {
            STARTING => TransportHealth::Starting,
            RUNNING => match self.degraded_peers.lock() {
                Ok(peers) if peers.is_empty() => TransportHealth::Ready,
                Ok(_) => TransportHealth::Degraded,
                Err(_) => TransportHealth::Failed,
            },
            STOPPING | STOPPING_PAUSED => TransportHealth::Stopping,
            STOPPED => TransportHealth::Stopped,
            _ => TransportHealth::Failed,
        }
    }

    pub(crate) fn terminal_failure(&self) -> Option<String> {
        self.terminal_failure
            .lock()
            .ok()
            .and_then(|failure| failure.clone())
    }
}
