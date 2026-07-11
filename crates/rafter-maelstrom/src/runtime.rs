use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::io::BufRead;
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use rafter::Input;
use serde_json::json;

use crate::{
    app::load_app_state,
    membership::{membership_plan_from_env, membership_target_for_plan},
    protocol::{body_type, node_id_map, required_array, required_str, required_u64, Envelope},
    raft_node::{node_root, open_node, snapshot_every_from_env},
    InitializedNode, MaelstromNode,
};

const TICK_INTERVAL: Duration = Duration::from_millis(50);

pub(crate) fn run() -> Result<(), Box<dyn Error>> {
    let stdin_rx = spawn_stdin_reader();
    let mut node = MaelstromNode::default();
    let mut last_tick = Instant::now();

    loop {
        match stdin_rx.recv_timeout(TICK_INTERVAL / 5) {
            Ok(line) => node.handle_line(&line)?,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        }

        if node.is_initialized() && last_tick.elapsed() >= TICK_INTERVAL {
            node.tick();
            last_tick = Instant::now();
        }
    }
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

    fn tick(&mut self) {
        if let Some(node) = self.initialized.as_mut() {
            node.step(Input::Tick);
            node.drive_membership();
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
        let root = node_root(&node_name);
        std::fs::create_dir_all(&root)?;
        let app = load_app_state(&root)?;
        let node = open_node(&root, node_id, peers, app.applied)?;
        let last_reported_role = node.role();
        let last_snapshot_index = node.snapshot_index();
        let snapshot_every = snapshot_every_from_env()?;

        let initialized = InitializedNode {
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
        };
        initialized.emit(
            &envelope.src,
            json!({
                "type": "init_ok",
                "in_reply_to": required_u64(&envelope.body, "msg_id")?,
            }),
        );
        self.initialized = Some(initialized);
        Ok(())
    }
}
