//! Stable host principals and strict durable transport-state provisioning.

use std::path::Path;

use rafter::NodeId;
use rafter_transport_tls::{ClusterId, FileTransportSessionStore, PeerId, SessionStoreLimits};

const TRANSPORT_SESSION_FILE: &str = "transport.state";

pub(super) fn transport_peer_id(node_id: NodeId) -> PeerId {
    PeerId::new(&format!("counter-host-{}", node_id.0))
        .expect("the bounded counter host identity is valid")
}

pub(super) fn transport_cluster_id() -> ClusterId {
    ClusterId::new("rafter-reference-sharded-counter")
        .expect("the fixed counter cluster identity is valid")
}

pub(super) fn open_transport_state(
    host_dir: &Path,
    cluster_id: &ClusterId,
    local_peer: &PeerId,
    limits: SessionStoreLimits,
) -> Result<FileTransportSessionStore, String> {
    let path = host_dir.join(TRANSPORT_SESSION_FILE);
    if path
        .try_exists()
        .map_err(|error| format!("could not inspect {}: {error}", path.display()))?
    {
        return FileTransportSessionStore::open_existing(&path, cluster_id, local_peer)
            .map_err(|error| error.to_string());
    }
    if durable_group_identity_exists(host_dir)? {
        return Err(format!(
            "transport session state is missing at {}; restore it or provision a new PeerId",
            path.display()
        ));
    }
    FileTransportSessionStore::create_new(&path, cluster_id.clone(), local_peer.clone(), limits)
        .map_err(|error| error.to_string())
}

fn durable_group_identity_exists(host_dir: &Path) -> Result<bool, String> {
    let groups = host_dir.join("groups");
    if !groups
        .try_exists()
        .map_err(|error| format!("could not inspect {}: {error}", groups.display()))?
    {
        return Ok(false);
    }
    let mut entries = std::fs::read_dir(&groups)
        .map_err(|error| format!("could not inspect {}: {error}", groups.display()))?;
    entries
        .next()
        .transpose()
        .map(|entry| entry.is_some())
        .map_err(|error| format!("could not inspect {}: {error}", groups.display()))
}
