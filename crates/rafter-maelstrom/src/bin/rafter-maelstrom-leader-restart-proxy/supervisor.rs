use std::error::Error;
use std::fs::{self, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use super::config::ProxyMode;
use super::config::{
    child_path, env_down_time, env_proxy_mode, env_restart_count, env_restart_delay,
};
use super::lease_isolation::{Action as LeaseAction, EvidenceEvent, LeaseIsolation};
use super::protocol::{
    body_type, client_request, client_response, init_node_id, lease_read, lease_state,
    node_restart_stagger, reports_leader, role_state,
};

mod io_threads;

use io_threads::{spawn_line_reader, spawn_stdin_reader, Event};

struct ChildProcess {
    child: Child,
    stdin: ChildStdin,
}

struct Supervisor {
    child_path: PathBuf,
    child: Option<ChildProcess>,
    init_line: Option<String>,
    init_ok_forwarded: bool,
    node_id: Option<String>,
    proxy_mode: ProxyMode,
    lease_isolation: LeaseIsolation,
    buffered_read_line: Option<String>,
    lease_event_sequence: u64,
    restarts_done: u64,
    max_restarts: u64,
    restart_delay: Duration,
    down_time: Duration,
    leader_seen_at: Option<Instant>,
    scheduled_restart_at: Option<Instant>,
    restart_at: Option<Instant>,
}

pub(super) fn run() -> Result<(), Box<dyn Error>> {
    let (tx, rx) = mpsc::channel();
    spawn_stdin_reader(tx.clone());

    let mut supervisor = Supervisor::new(child_path()?);
    supervisor.spawn_child(tx.clone())?;

    loop {
        match rx.recv_timeout(Duration::from_millis(20)) {
            Ok(Event::Stdin(line)) => supervisor.handle_stdin(&line)?,
            Ok(Event::ChildStdout(line)) => supervisor.handle_child_stdout(&line)?,
            Ok(Event::ChildStderr(line)) => supervisor.handle_child_stderr(&line)?,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(mpsc::RecvTimeoutError::Disconnected) => return Ok(()),
        }
        supervisor.check_child_exit()?;
        supervisor.maybe_restart(tx.clone())?;
    }
}

impl Supervisor {
    fn new(child_path: PathBuf) -> Self {
        Self {
            child_path,
            child: None,
            init_line: None,
            init_ok_forwarded: false,
            node_id: None,
            proxy_mode: env_proxy_mode(),
            lease_isolation: LeaseIsolation::default(),
            buffered_read_line: None,
            lease_event_sequence: 0,
            restarts_done: 0,
            max_restarts: env_restart_count(),
            restart_delay: env_restart_delay(),
            down_time: env_down_time(),
            leader_seen_at: None,
            scheduled_restart_at: None,
            restart_at: None,
        }
    }

    fn spawn_child(&mut self, tx: mpsc::Sender<Event>) -> Result<(), Box<dyn Error>> {
        let mut child = Command::new(&self.child_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let mut stdin = child.stdin.take().ok_or("child stdin unavailable")?;
        let stdout = child.stdout.take().ok_or("child stdout unavailable")?;
        let stderr = child.stderr.take().ok_or("child stderr unavailable")?;

        spawn_line_reader(stdout, tx.clone(), Event::ChildStdout);
        spawn_line_reader(stderr, tx, Event::ChildStderr);

        if let Some(init_line) = &self.init_line {
            writeln!(stdin, "{init_line}")?;
            stdin.flush()?;
        }

        self.child = Some(ChildProcess { child, stdin });
        self.schedule_staggered_restart();
        Ok(())
    }

    fn handle_stdin(&mut self, line: &str) -> Result<(), Box<dyn Error>> {
        if body_type(line).as_deref() == Some("init") && self.init_line.is_none() {
            self.init_line = Some(line.to_string());
            self.node_id = init_node_id(line);
            self.schedule_staggered_restart();
        }
        let message_type = body_type(line);
        if self.proxy_mode == ProxyMode::LeaseIsolation {
            if message_type.as_deref() == Some("raft") && self.lease_isolation.drops_raft() {
                return Ok(());
            }
            if let Some((request, direct)) = client_request(line) {
                let disposition = self.lease_isolation.observe_read_request(&request, direct);
                if disposition.hold && self.buffered_read_line.replace(line.to_owned()).is_some() {
                    return Err("lease-isolation attempted to buffer more than one read".into());
                }
                self.handle_lease_actions(disposition.actions)?;
                if disposition.hold {
                    return Ok(());
                }
            }
        }
        let Some(child) = self.child.as_mut() else {
            eprintln!("dropping Maelstrom input while child is down");
            return Ok(());
        };
        if let Err(error) = writeln!(child.stdin, "{line}").and_then(|()| child.stdin.flush()) {
            if error.kind() == ErrorKind::BrokenPipe {
                eprintln!("rafter-maelstrom child stdin closed; scheduling restart");
                self.child = None;
                if self.restart_at.is_none() {
                    self.restart_at = Some(Instant::now() + self.down_time);
                }
                return Ok(());
            }
            return Err(error.into());
        }
        Ok(())
    }

    fn handle_child_stdout(&mut self, line: &str) -> Result<(), Box<dyn Error>> {
        if body_type(line).as_deref() == Some("init_ok") {
            if self.init_ok_forwarded {
                return Ok(());
            }
            self.init_ok_forwarded = true;
        }
        if self.proxy_mode == ProxyMode::LeaseIsolation {
            if body_type(line).as_deref() == Some("raft") && self.lease_isolation.drops_raft() {
                return Ok(());
            }
            let actions = client_response(line).map_or_else(Vec::new, |(request, response)| {
                self.lease_isolation.observe_response(&request, response)
            });
            println!("{line}");
            std::io::stdout().flush()?;
            self.handle_lease_actions(actions)?;
            return Ok(());
        }
        println!("{line}");
        std::io::stdout().flush()?;
        Ok(())
    }

    fn handle_child_stderr(&mut self, line: &str) -> Result<(), Box<dyn Error>> {
        eprintln!("{line}");
        if self.proxy_mode == ProxyMode::LeaseIsolation {
            if let Some(state) = lease_state(line) {
                let actions = self.lease_isolation.observe_lease_state(
                    state.active,
                    state.leader,
                    state.term,
                );
                self.handle_lease_actions(actions)?;
            }
            if let Some(role) = role_state(line) {
                let actions = self.lease_isolation.observe_role(role.leader, role.term);
                self.handle_lease_actions(actions)?;
            }
            if let Some(read) = lease_read(line) {
                let actions = self.lease_isolation.observe_read_handler(
                    &read.request,
                    read.active,
                    read.leader,
                    read.term,
                );
                self.handle_lease_actions(actions)?;
            }
        }
        if self.proxy_mode == ProxyMode::Leader
            && reports_leader(line)
            && self.leader_seen_at.is_none()
            && self.restarts_done < self.max_restarts
        {
            self.leader_seen_at = Some(Instant::now());
        }
        if let Some(leader_seen_at) = self.leader_seen_at {
            if leader_seen_at.elapsed() >= self.restart_delay {
                self.kill_child_for_restart("leader-observed")?;
            }
        }
        Ok(())
    }

    fn check_child_exit(&mut self) -> Result<(), Box<dyn Error>> {
        let Some(child) = self.child.as_mut() else {
            return Ok(());
        };
        if let Some(status) = child.child.try_wait()? {
            eprintln!("rafter-maelstrom child exited with {status}");
            self.child = None;
            self.scheduled_restart_at = None;
            if self.restart_at.is_none() {
                self.restart_at = Some(Instant::now() + self.down_time);
            }
        }
        Ok(())
    }

    fn maybe_restart(&mut self, tx: mpsc::Sender<Event>) -> Result<(), Box<dyn Error>> {
        let now = Instant::now();
        if let Some(restart_at) = self.restart_at {
            if now < restart_at {
                return Ok(());
            }
            self.leader_seen_at = None;
            self.restart_at = None;
            self.spawn_child(tx)?;
            return Ok(());
        }

        if self.proxy_mode == ProxyMode::Scheduled
            && self
                .scheduled_restart_at
                .is_some_and(|restart_at| now >= restart_at)
        {
            self.kill_child_for_restart("scheduled")?;
        }
        Ok(())
    }

    fn schedule_staggered_restart(&mut self) {
        if self.proxy_mode != ProxyMode::Scheduled
            || self.child.is_none()
            || self.init_line.is_none()
            || self.scheduled_restart_at.is_some()
            || self.restarts_done >= self.max_restarts
        {
            return;
        }
        let stagger = self
            .node_id
            .as_deref()
            .map(node_restart_stagger)
            .unwrap_or_default();
        self.scheduled_restart_at = Some(Instant::now() + self.restart_delay + stagger);
    }

    fn kill_child_for_restart(&mut self, reason: &str) -> Result<(), Box<dyn Error>> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        self.restarts_done += 1;
        eprintln!(
            "rafter-maelstrom proxy restarting child reason={} node={} attempt={} down_ms={}",
            reason,
            self.node_id.as_deref().unwrap_or("unknown"),
            self.restarts_done,
            self.down_time.as_millis()
        );
        child.child.kill()?;
        let _ = child.child.wait();
        self.leader_seen_at = None;
        self.scheduled_restart_at = None;
        self.restart_at = Some(Instant::now() + self.down_time);
        Ok(())
    }

    fn handle_lease_actions(&mut self, actions: Vec<LeaseAction>) -> Result<(), Box<dyn Error>> {
        for action in actions {
            self.handle_lease_action(action)?;
        }
        Ok(())
    }

    fn handle_lease_action(&mut self, action: LeaseAction) -> Result<(), Box<dyn Error>> {
        match action {
            LeaseAction::Claim => {
                let node = self
                    .node_id
                    .as_deref()
                    .ok_or("lease-isolation node unavailable")?;
                let claimed = claim_lease_isolation(node)?;
                let actions = self.lease_isolation.claim_result(claimed);
                self.handle_lease_actions(actions)?;
            }
            LeaseAction::FastPathReadOk(event) => {
                self.emit_lease_marker("fast-path-read-ok", &event, None)?;
            }
            LeaseAction::ReadBuffered(event) => {
                self.emit_lease_marker("read-buffered", &event, None)?;
            }
            LeaseAction::LeaseExpired(event) => {
                self.emit_lease_marker("lease-expired", &event, None)?;
            }
            LeaseAction::ReleaseBuffered(event) => {
                let line = self
                    .buffered_read_line
                    .take()
                    .ok_or("lease-isolation buffered read line unavailable")?;
                let child = self
                    .child
                    .as_mut()
                    .ok_or("lease-isolation child unavailable")?;
                writeln!(child.stdin, "{line}")?;
                child.stdin.flush()?;
                self.emit_lease_marker("post-expiry-released", &event, None)?;
            }
            LeaseAction::PostExpiryHandler(event) => {
                self.emit_lease_marker("post-expiry-handler", &event, None)?;
            }
            LeaseAction::ProbeUnavailable(event) => {
                self.emit_lease_marker("post-expiry-unavailable", &event, None)?;
            }
            LeaseAction::PostExpiryReadServed(event) => {
                self.emit_lease_marker("post-expiry-read-served-violation", &event, None)?;
            }
            LeaseAction::PostExpiryLeaseRenewed(event) => {
                self.emit_lease_marker("post-expiry-renewed-violation", &event, None)?;
            }
            LeaseAction::PostExpiryUnexpectedError { event, code } => {
                self.emit_lease_marker(
                    "post-expiry-unexpected-error",
                    &event,
                    Some(&format!("code={code}")),
                )?;
            }
            LeaseAction::DuplicateTerminal(event) => {
                self.emit_lease_marker("post-expiry-duplicate-terminal", &event, None)?;
            }
            LeaseAction::CoverageLost { event, reason } => {
                if let Some(line) = self.buffered_read_line.take() {
                    let child = self
                        .child
                        .as_mut()
                        .ok_or("lease-isolation child unavailable")?;
                    writeln!(child.stdin, "{line}")?;
                    child.stdin.flush()?;
                }
                self.emit_lease_marker("coverage-lost", &event, Some(&format!("reason={reason}")))?;
            }
        }
        Ok(())
    }

    fn emit_lease_marker(
        &mut self,
        phase: &str,
        event: &EvidenceEvent,
        extra: Option<&str>,
    ) -> Result<(), Box<dyn Error>> {
        let node = self
            .node_id
            .as_deref()
            .ok_or("lease-isolation node ID unavailable")?;
        self.lease_event_sequence += 1;
        eprint!(
            "rafter-maelstrom lease-isolation seq={} node={node} term={} phase={phase} client={} msg_id={}",
            self.lease_event_sequence,
            event.term,
            event.request.client(),
            event.request.msg_id()
        );
        if let Some(extra) = extra {
            eprint!(" {extra}");
        }
        eprintln!();
        Ok(())
    }
}

fn claim_lease_isolation(node_id: &str) -> Result<bool, Box<dyn Error>> {
    let root = std::env::var_os("RAFTER_MAELSTROM_ROOT")
        .ok_or("lease-isolation proxy requires RAFTER_MAELSTROM_ROOT")?;
    let path = PathBuf::from(root).join("lease-isolation-owner");
    match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(mut file) => {
            writeln!(file, "{node_id}")?;
            file.flush()?;
            Ok(true)
        }
        Err(error) if error.kind() == ErrorKind::AlreadyExists => {
            Ok(fs::read_to_string(path)?.trim() == node_id)
        }
        Err(error) => Err(error.into()),
    }
}
