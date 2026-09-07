//! Parent-side process commands and bounded event waits.

use std::{
    io::Write,
    path::PathBuf,
    sync::mpsc,
    time::{Duration, Instant},
};

use super::{NodeId, ProcessCluster, ProcessLine, PROCESS_DRIVER_INTERVAL, PROCESS_PENDING_LIMIT};

impl ProcessCluster {
    pub(super) fn tick_node(&mut self, node_id: NodeId) {
        if self.processes.contains_key(&node_id) {
            self.send_command(node_id, "TICK");
        }
    }

    pub(super) fn shutdown(mut self) -> PathBuf {
        let live: Vec<_> = self.processes.keys().copied().collect();
        for node_id in &live {
            self.send_command(*node_id, "STOP");
        }
        for node_id in live {
            if let Some(mut process) = self.processes.remove(&node_id) {
                drop(process.stdin);
                process.child.wait().ok();
            }
        }
        self.root
    }

    pub(super) fn send_command(&mut self, node_id: NodeId, command: &str) {
        let process = self.processes.get_mut(&node_id).expect("process exists");
        writeln!(process.stdin, "{command}").expect("send process command");
        process.stdin.flush().expect("flush process command");
    }

    pub(super) fn wait_for_line<F, D>(
        &mut self,
        timeout: Duration,
        driver: D,
        predicate: F,
    ) -> ProcessLine
    where
        F: FnMut(&ProcessLine) -> bool,
        D: FnMut(&mut Self),
    {
        self.wait_for_event(timeout, driver, predicate)
    }

    fn push_pending(&mut self, event: ProcessLine) {
        if self.pending.len() == PROCESS_PENDING_LIMIT {
            self.pending.pop_front();
        }
        self.pending.push_back(event);
    }

    pub(super) fn wait_for_event<F, D>(
        &mut self,
        timeout: Duration,
        mut driver: D,
        mut predicate: F,
    ) -> ProcessLine
    where
        F: FnMut(&ProcessLine) -> bool,
        D: FnMut(&mut Self),
    {
        let deadline = Instant::now() + timeout;
        let mut next_drive = Instant::now();
        loop {
            let pending_len = self.pending.len();
            for _ in 0..pending_len {
                let event = self.pending.pop_front().expect("pending event exists");
                if predicate(&event) {
                    return event;
                }
                self.pending.push_back(event);
            }

            let now = Instant::now();
            if now >= next_drive {
                driver(self);
                next_drive = now + PROCESS_DRIVER_INTERVAL;
            }
            let now = Instant::now();
            assert!(
                now < deadline,
                "timed out waiting for process event; pending={:?}",
                self.pending
            );
            let remaining = deadline.saturating_duration_since(now);
            match self
                .stdout_rx
                .recv_timeout(remaining.min(PROCESS_DRIVER_INTERVAL))
            {
                Ok(event) if predicate(&event) => return event,
                Ok(event) => self.push_pending(event),
                Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("process event channel disconnected")
                }
            }
        }
    }
}
