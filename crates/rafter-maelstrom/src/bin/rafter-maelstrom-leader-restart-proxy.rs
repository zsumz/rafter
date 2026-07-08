use std::env;
use std::error::Error;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

const DEFAULT_LEADER_RESTARTS: u64 = 1;
const DEFAULT_RESTART_DELAY_MS: u64 = 250;
const DEFAULT_DOWN_MS: u64 = 500;

enum Event {
    Stdin(String),
    ChildStdout(String),
    ChildStderr(String),
}

struct ChildProcess {
    child: Child,
    stdin: ChildStdin,
}

struct Supervisor {
    child_path: PathBuf,
    child: Option<ChildProcess>,
    init_line: Option<String>,
    init_ok_forwarded: bool,
    restarts_done: u64,
    max_restarts: u64,
    restart_delay: Duration,
    down_time: Duration,
    leader_seen_at: Option<Instant>,
    restart_at: Option<Instant>,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("rafter-maelstrom leader restart proxy failed: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
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

fn spawn_stdin_reader(tx: mpsc::Sender<Event>) {
    thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else {
                break;
            };
            if tx.send(Event::Stdin(line)).is_err() {
                break;
            }
        }
    });
}

impl Supervisor {
    fn new(child_path: PathBuf) -> Self {
        Self {
            child_path,
            child: None,
            init_line: None,
            init_ok_forwarded: false,
            restarts_done: 0,
            max_restarts: env_u64("RAFTER_MAELSTROM_LEADER_RESTARTS", DEFAULT_LEADER_RESTARTS),
            restart_delay: env_duration_ms(
                "RAFTER_MAELSTROM_LEADER_RESTART_DELAY_MS",
                DEFAULT_RESTART_DELAY_MS,
            ),
            down_time: env_duration_ms("RAFTER_MAELSTROM_LEADER_DOWN_MS", DEFAULT_DOWN_MS),
            leader_seen_at: None,
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
        Ok(())
    }

    fn handle_stdin(&mut self, line: &str) -> Result<(), Box<dyn Error>> {
        if body_type(line).as_deref() == Some("init") && self.init_line.is_none() {
            self.init_line = Some(line.to_string());
        }
        let Some(child) = self.child.as_mut() else {
            eprintln!("dropping Maelstrom input while child is down");
            return Ok(());
        };
        writeln!(child.stdin, "{line}")?;
        child.stdin.flush()?;
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
        if reports_leader(line)
            && self.leader_seen_at.is_none()
            && self.restarts_done < self.max_restarts
        {
            self.leader_seen_at = Some(Instant::now());
        }
        if let Some(leader_seen_at) = self.leader_seen_at {
            if leader_seen_at.elapsed() >= self.restart_delay {
                self.kill_child_for_restart()?;
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
            if self.restart_at.is_none() {
                self.restart_at = Some(Instant::now() + self.down_time);
            }
        }
        Ok(())
    }

    fn maybe_restart(&mut self, tx: mpsc::Sender<Event>) -> Result<(), Box<dyn Error>> {
        let Some(restart_at) = self.restart_at else {
            return Ok(());
        };
        if Instant::now() < restart_at {
            return Ok(());
        }
        self.leader_seen_at = None;
        self.restart_at = None;
        self.spawn_child(tx)
    }

    fn kill_child_for_restart(&mut self) -> Result<(), Box<dyn Error>> {
        let Some(mut child) = self.child.take() else {
            return Ok(());
        };
        self.restarts_done += 1;
        eprintln!(
            "rafter-maelstrom proxy restarting leader child attempt={} down_ms={}",
            self.restarts_done,
            self.down_time.as_millis()
        );
        child.child.kill()?;
        let _ = child.child.wait();
        self.leader_seen_at = None;
        self.restart_at = Some(Instant::now() + self.down_time);
        Ok(())
    }
}

fn spawn_line_reader<R, F>(reader: R, tx: mpsc::Sender<Event>, wrap: F)
where
    R: std::io::Read + Send + 'static,
    F: Fn(String) -> Event + Send + Copy + 'static,
{
    thread::spawn(move || {
        for line in BufReader::new(reader).lines() {
            let Ok(line) = line else {
                break;
            };
            if tx.send(wrap(line)).is_err() {
                break;
            }
        }
    });
}

fn child_path() -> Result<PathBuf, Box<dyn Error>> {
    if let Some(path) = env::var_os("RAFTER_MAELSTROM_CHILD") {
        return Ok(path.into());
    }
    Ok(env::current_exe()?.with_file_name("rafter-maelstrom"))
}

fn env_duration_ms(name: &str, default_ms: u64) -> Duration {
    Duration::from_millis(env_u64(name, default_ms))
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn reports_leader(line: &str) -> bool {
    line.contains("rafter-maelstrom role ") && line.contains(" role=leader ")
}

fn body_type(line: &str) -> Option<String> {
    let value: Value = serde_json::from_str(line).ok()?;
    value.get("body")?.get("type")?.as_str().map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_init_and_init_ok_body_types() {
        assert_eq!(
            body_type(r#"{"src":"c0","dest":"n1","body":{"type":"init"}}"#),
            Some("init".to_string())
        );
        assert_eq!(
            body_type(r#"{"src":"n1","dest":"c0","body":{"type":"init_ok"}}"#),
            Some("init_ok".to_string())
        );
        assert_eq!(body_type("not json"), None);
    }

    #[test]
    fn detects_structured_leader_marker() {
        assert!(reports_leader(
            "rafter-maelstrom role node=n1 role=leader term=3"
        ));
        assert!(!reports_leader(
            "rafter-maelstrom role node=n1 role=follower term=3"
        ));
    }
}
