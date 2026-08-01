//! Caller-owned endpoint discovery lifecycle around the paused public runtime.

use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use rafter::NodeId;
use rafter_transport_tls::{EndpointBook, PeerId, TlsServerName};

use super::endpoints::EndpointSupervisor;

/// Discovery resources retained while transport workers are paused.
#[derive(Debug)]
pub(super) struct EndpointLifecycle {
    cluster_dir: PathBuf,
    peers: BTreeMap<NodeId, PeerId>,
    endpoints: EndpointBook,
    server_name: TlsServerName,
    supervisor: Mutex<Option<EndpointSupervisor>>,
}

impl EndpointLifecycle {
    pub(super) fn new(
        cluster_dir: &Path,
        peers: BTreeMap<NodeId, PeerId>,
        endpoints: EndpointBook,
        server_name: TlsServerName,
    ) -> Self {
        Self {
            cluster_dir: cluster_dir.to_path_buf(),
            peers,
            endpoints,
            server_name,
            supervisor: Mutex::new(None),
        }
    }

    pub(super) fn start(&self, failure: Arc<Mutex<Option<String>>>) -> Result<(), String> {
        let mut supervisor = lock(&self.supervisor);
        if supervisor.is_some() {
            return Ok(());
        }
        *supervisor = Some(EndpointSupervisor::start(
            &self.cluster_dir,
            self.peers.clone(),
            self.endpoints.clone(),
            self.server_name.clone(),
            failure,
        )?);
        Ok(())
    }

    pub(super) fn stop(&self) -> Result<(), String> {
        let Some(supervisor) = lock(&self.supervisor).take() else {
            return Ok(());
        };
        supervisor.stop()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
