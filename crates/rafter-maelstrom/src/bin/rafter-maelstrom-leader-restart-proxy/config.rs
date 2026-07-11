use std::{env, error::Error, path::PathBuf, time::Duration};

const DEFAULT_LEADER_RESTARTS: u64 = 1;
const DEFAULT_RESTART_DELAY_MS: u64 = 250;
const DEFAULT_DOWN_MS: u64 = 500;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum RestartMode {
    Leader,
    Scheduled,
}

pub(super) fn child_path() -> Result<PathBuf, Box<dyn Error>> {
    if let Some(path) = env::var_os("RAFTER_MAELSTROM_CHILD") {
        return Ok(path.into());
    }
    Ok(env::current_exe()?.with_file_name("rafter-maelstrom"))
}

pub(super) fn env_restart_mode() -> RestartMode {
    restart_mode_from_value(env::var("RAFTER_MAELSTROM_RESTART_MODE").ok().as_deref())
}

pub(super) fn restart_mode_from_value(value: Option<&str>) -> RestartMode {
    match value {
        Some("scheduled" | "staggered" | "any-node") => RestartMode::Scheduled,
        _ => RestartMode::Leader,
    }
}

pub(super) fn env_restart_count() -> u64 {
    env_u64(
        "RAFTER_MAELSTROM_RESTARTS",
        env_u64("RAFTER_MAELSTROM_LEADER_RESTARTS", DEFAULT_LEADER_RESTARTS),
    )
}

pub(super) fn env_restart_delay() -> Duration {
    env_duration_ms(
        "RAFTER_MAELSTROM_RESTART_DELAY_MS",
        env_u64(
            "RAFTER_MAELSTROM_LEADER_RESTART_DELAY_MS",
            DEFAULT_RESTART_DELAY_MS,
        ),
    )
}

pub(super) fn env_down_time() -> Duration {
    env_duration_ms(
        "RAFTER_MAELSTROM_DOWN_MS",
        env_u64("RAFTER_MAELSTROM_LEADER_DOWN_MS", DEFAULT_DOWN_MS),
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_restart_modes() {
        // The parser accepts aliases used by scripts but keeps the proxy's
        // historical leader-triggered behavior as the fallback.
        assert_eq!(
            restart_mode_from_value(Some("scheduled")),
            RestartMode::Scheduled
        );
        assert_eq!(
            restart_mode_from_value(Some("staggered")),
            RestartMode::Scheduled
        );
        assert_eq!(restart_mode_from_value(Some("leader")), RestartMode::Leader);
        assert_eq!(restart_mode_from_value(None), RestartMode::Leader);
    }
}
