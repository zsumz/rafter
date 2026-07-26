use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use rafter::{Input, NodeId, Output};
use serde_json::json;

use crate::{
    app::{load_app_state, persist_snapshot_application_state, AppState},
    membership::{membership_plan_from_env, membership_target_for_plan},
    protocol::{body_type, node_id_map, required_array, required_str, required_u64, Envelope},
    raft::snapshots::validate_application_snapshot_metadata,
    raft_node::{node_root, open_node, read_snapshot_payload, snapshot_every_from_env, FileNode},
    InitializedNode, MaelstromNode,
};

const DEFAULT_TICK_INTERVAL_MS: u64 = 50;

pub(crate) struct OpenedApplicationNode {
    pub(crate) node: FileNode,
    pub(crate) app: AppState,
    pub(crate) recovery_outputs: Vec<Output>,
}

pub(crate) fn open_application_node(
    root: &Path,
    node_id: NodeId,
    peers: Vec<NodeId>,
) -> Result<OpenedApplicationNode, Box<dyn Error>> {
    let mut app = load_app_state(root)?;
    let opened = open_node(root, node_id, peers, app.applied)?;
    if let Some(snapshot) = opened.node.snapshot().cloned() {
        validate_application_snapshot_metadata(&snapshot.metadata)
            .map_err(|error| format!("refusing recovered application snapshot: {error}"))?;
        if app.applied < snapshot.metadata.last_included_index {
            let payload = read_snapshot_payload(&opened.node, &snapshot).map_err(|error| {
                format!("failed to read recovered application snapshot: {error}")
            })?;
            persist_snapshot_application_state(
                root,
                &mut app,
                snapshot.metadata.last_included_index,
                &payload,
            )?;
        }
    }
    Ok(OpenedApplicationNode {
        node: opened.node,
        app,
        recovery_outputs: opened.recovery_outputs,
    })
}

pub(crate) fn run() -> Result<(), Box<dyn Error>> {
    let tick_interval = tick_interval_from_env()?;
    let stdin_rx = spawn_stdin_reader();
    let mut node = MaelstromNode::default();
    let mut last_tick = Instant::now();

    loop {
        match stdin_rx.recv_timeout(tick_interval / 5) {
            Ok(line) => node.handle_line(&line)?,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        }

        if node.is_initialized() && last_tick.elapsed() >= tick_interval {
            node.tick();
            last_tick = Instant::now();
        }
    }
}

fn tick_interval_from_env() -> Result<Duration, Box<dyn Error>> {
    tick_interval(
        std::env::var("RAFTER_MAELSTROM_TICK_INTERVAL_MS")
            .ok()
            .as_deref(),
    )
    .map_err(std::convert::Into::into)
}

fn tick_interval(value: Option<&str>) -> Result<Duration, String> {
    let milliseconds = match value {
        Some(value) => value
            .parse::<u64>()
            .ok()
            .filter(|value| *value >= 5)
            .ok_or_else(|| "RAFTER_MAELSTROM_TICK_INTERVAL_MS must be at least 5".to_owned())?,
        None => DEFAULT_TICK_INTERVAL_MS,
    };
    Ok(Duration::from_millis(milliseconds))
}

fn spawn_stdin_reader() -> mpsc::Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else {
                break;
            };
            if tx.send(line).is_err() {
                break;
            }
        }
    });
    rx
}

impl MaelstromNode {
    fn is_initialized(&self) -> bool {
        self.initialized.is_some()
    }

    /// Drives one tick of Raft time and re-examines the reads waiting on it.
    ///
    /// Without the flush a stalled read is only ever reconsidered by an apply
    /// or a snapshot install, so a read that becomes answerable through any
    /// other path — including one that lands between the grant and this node's
    /// next committed command — waits for unrelated traffic to arrive and
    /// trigger a pass. The tick is the one event this node always produces.
    pub(crate) fn tick(&mut self) {
        if let Some(node) = self.initialized.as_mut() {
            node.step(Input::Tick);
            node.drive_membership();
            node.flush_reads();
        }
    }

    fn handle_line(&mut self, line: &str) -> Result<(), Box<dyn Error>> {
        if line.trim().is_empty() {
            return Ok(());
        }
        let envelope: Envelope = serde_json::from_str(line)?;
        if body_type(&envelope.body) == Some("init") {
            self.initialize(&envelope)?;
            return Ok(());
        }
        let Some(node) = self.initialized.as_mut() else {
            return Ok(());
        };
        node.handle_envelope(envelope);
        node.drive_membership();
        Ok(())
    }

    fn initialize(&mut self, envelope: &Envelope) -> Result<(), Box<dyn Error>> {
        let node_name = required_str(&envelope.body, "node_id")?;
        self.initialize_at_root(envelope, node_root(node_name))
    }

    pub(crate) fn initialize_at_root(
        &mut self,
        envelope: &Envelope,
        root: PathBuf,
    ) -> Result<(), Box<dyn Error>> {
        let node_name = required_str(&envelope.body, "node_id")?.to_string();
        let node_names = required_array(&envelope.body, "node_ids")?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(ToString::to_string)
                    .ok_or("node_ids must contain strings")
            })
            .collect::<Result<Vec<_>, _>>()?;
        let name_to_id = node_id_map(&node_names);
        let id_to_name = name_to_id
            .iter()
            .map(|(name, node_id)| (*node_id, name.clone()))
            .collect::<BTreeMap<_, _>>();
        let node_id = *name_to_id.get(&node_name).ok_or("node_id must be listed")?;
        let peers = name_to_id
            .values()
            .copied()
            .filter(|peer| *peer != node_id)
            .collect();
        let membership_plan = membership_plan_from_env()?;
        let membership_target = membership_target_for_plan(membership_plan, &name_to_id)?;
        std::fs::create_dir_all(&root)?;
        let opened = open_application_node(&root, node_id, peers)?;
        let node = opened.node;
        let app = opened.app;
        let recovery_outputs = opened.recovery_outputs;
        let last_reported_role = node.role();
        let last_reported_lease_active = node.read_lease_active();
        let last_snapshot_index = node.snapshot_index();
        let snapshot_every = snapshot_every_from_env()?;

        let mut initialized = InitializedNode {
            name: node_name,
            node,
            root,
            app,
            name_to_id,
            id_to_name,
            membership_plan,
            membership_target,
            membership_reported_complete: false,
            known_leader: None,
            pending_reads: BTreeMap::new(),
            completed_replies: BTreeSet::new(),
            next_msg_id: 1,
            next_read_id: 1,
            snapshot_every,
            last_snapshot_index,
            last_reported_role,
            last_reported_lease_active,
            #[cfg(test)]
            emitted: Vec::new(),
        };
        initialized.emit(
            &envelope.src,
            json!({
                "type": "init_ok",
                "in_reply_to": required_u64(&envelope.body, "msg_id")?,
            }),
        );
        dispatch_recovery_outputs(&mut initialized, recovery_outputs);
        self.initialized = Some(initialized);
        Ok(())
    }
}

pub(crate) fn dispatch_recovery_outputs(node: &mut InitializedNode, outputs: Vec<Output>) {
    node.handle_outputs(outputs);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::tick_interval;

    #[test]
    fn evidence_tick_interval_is_explicit_and_nonzero() {
        assert_eq!(tick_interval(None), Ok(Duration::from_millis(50)));
        assert_eq!(tick_interval(Some("25")), Ok(Duration::from_millis(25)));
        assert!(tick_interval(Some("0")).is_err());
        assert!(tick_interval(Some("bad")).is_err());
    }
}
