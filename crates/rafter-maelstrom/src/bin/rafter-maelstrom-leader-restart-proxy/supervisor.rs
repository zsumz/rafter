use std::error::Error;
use std::io::{ErrorKind, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use super::config::RestartMode;
use super::config::{
    child_path, env_down_time, env_restart_count, env_restart_delay, env_restart_mode,
};
use super::protocol::{body_type, init_node_id, node_restart_stagger, reports_leader};

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
    restart_mode: RestartMode,
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
            restart_mode: env_restart_mode(),
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
        println!("{line}");
        std::io::stdout().flush()?;
        Ok(())
    }

    fn handle_child_stderr(&mut self, line: &str) -> Result<(), Box<dyn Error>> {
        eprintln!("{line}");
        if self.restart_mode == RestartMode::Leader
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

        if self.restart_mode == RestartMode::Scheduled
            && self
                .scheduled_restart_at
                .is_some_and(|restart_at| now >= restart_at)
        {
            self.kill_child_for_restart("scheduled")?;
        }
        Ok(())
    }

    fn schedule_staggered_restart(&mut self) {
        if self.restart_mode != RestartMode::Scheduled
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
}
