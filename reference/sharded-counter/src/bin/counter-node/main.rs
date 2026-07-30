//! Three-host, many-group durable process fixture for the sharded counter.
//!
//! This is integration evidence. Its peer transport is explicitly
//! unauthenticated and its filesystem address discovery is test-only.

mod app_store;
mod engine;
mod group;
mod host_registry;
mod peer_link;
mod protocol;

use std::{env, fs, io, num::NonZeroUsize, path::PathBuf, process::ExitCode, time::Duration};

use rafter::NodeId;
use rafter_reference_sharded_counter::WorkQuota;

fn main() -> ExitCode {
    match Config::parse().and_then(|config| engine::run(&config)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            emit(&format!("FATAL {error}"));
            ExitCode::FAILURE
        }
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    pub node_id: NodeId,
    pub members: Vec<NodeId>,
    pub cluster_dir: PathBuf,
    pub group_count: u32,
    pub election_timeout_ticks: u64,
    pub tick_interval: Duration,
    pub request_timeout: Duration,
    pub max_sessions: usize,
    pub quota: WorkQuota,
    pub workers: NonZeroUsize,
    pub max_group_queue: NonZeroUsize,
    pub max_global_queue: NonZeroUsize,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let arguments = env::args().skip(1).collect::<Vec<_>>();
        let value = |name: &str| -> Result<&str, String> {
            arguments
                .windows(2)
                .find(|pair| pair[0] == name)
                .map(|pair| pair[1].as_str())
                .ok_or_else(|| format!("missing required argument {name}"))
        };
        let node_id = NodeId(parse(value("--id")?, "node id")?);
        let members = value("--members")?
            .split(',')
            .map(|field| parse(field, "member id").map(NodeId))
            .collect::<Result<Vec<_>, _>>()?;
        if members.len() != 3 || !members.contains(&node_id) {
            return Err("members must name exactly three nodes including this node".to_string());
        }
        let group_count = parse(value("--groups")?, "group count")?;
        if group_count == 0 || group_count > 4096 {
            return Err("group count must be in 1..=4096".to_string());
        }
        let max_sessions = parse::<usize>(value("--max-sessions")?, "session bound")?;
        if max_sessions == 0 {
            return Err("session bound must be nonzero".to_string());
        }
        let quota = WorkQuota::new(parse(value("--quota")?, "quota")?)
            .ok_or_else(|| "quota must be nonzero".to_string())?;
        Ok(Self {
            node_id,
            members,
            cluster_dir: PathBuf::from(value("--cluster-dir")?),
            group_count,
            election_timeout_ticks: parse(value("--election-timeout-ticks")?, "election timeout")?,
            tick_interval: nonzero_duration(value("--tick-interval-ms")?, "tick interval")?,
            request_timeout: nonzero_duration(value("--request-timeout-ms")?, "request timeout")?,
            max_sessions,
            quota,
            workers: nonzero(value("--workers")?, "worker count")?,
            max_group_queue: nonzero(value("--max-group-queue")?, "group queue bound")?,
            max_global_queue: nonzero(value("--max-global-queue")?, "global queue bound")?,
        })
    }

    #[must_use]
    pub fn host_dir(&self) -> PathBuf {
        self.cluster_dir.join(format!("host-{}", self.node_id.0))
    }
}

fn parse<T>(value: &str, label: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value.parse().map_err(|_| format!("{label} is invalid"))
}

fn nonzero(value: &str, label: &str) -> Result<NonZeroUsize, String> {
    NonZeroUsize::new(parse(value, label)?).ok_or_else(|| format!("{label} must be nonzero"))
}

fn nonzero_duration(value: &str, label: &str) -> Result<Duration, String> {
    let milliseconds = parse(value, label)?;
    if milliseconds == 0 {
        return Err(format!("{label} must be nonzero"));
    }
    Ok(Duration::from_millis(milliseconds))
}

pub fn emit(line: &str) {
    println!("{line}");
}

pub fn directed_failpoint(name: &str) {
    if directed_failpoint_armed(name) {
        emit(&format!("FAILPOINT {name}"));
        std::process::abort();
    }
}

/// Injects a configured I/O failure without aborting the process.
///
/// # Errors
///
/// Returns the directed error when `name` is armed.
pub fn directed_io_failure(name: &str) -> io::Result<()> {
    if directed_failpoint_armed(name) {
        emit(&format!("FAILPOINT {name}"));
        return Err(io::Error::other(format!("directed I/O failure at {name}")));
    }
    Ok(())
}

fn directed_failpoint_armed(name: &str) -> bool {
    let environment_match = env::var("RAFTER_COUNTER_FAILPOINT").as_deref() == Ok(name);
    let file_match = env::var_os("RAFTER_COUNTER_FAILPOINT_FILE")
        .and_then(|path| fs::read_to_string(path).ok())
        .is_some_and(|armed| armed.trim() == name);
    environment_match || file_match
}
