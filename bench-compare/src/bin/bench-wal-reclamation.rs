//! WAL reclamation under repeated durable writes with a fixed live suffix.
//!
//! This is a Rafter component qualification, not a service or competitor
//! comparison. Each synchronous reclamation is a write-stop interval because
//! the WAL coordinator serializes mutation while it publishes a checkpoint,
//! manifest, and cleanup fence. The storage concurrency test proves an
//! overlapping writer resumes durably; this binary measures that interval and
//! verifies physical/recovery bounds without adding a timing threshold.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    NodeId, RaftSnapshotMetadata, SnapshotGroupId, Term,
};
use rafter_storage::{
    durable_batch::{PersistenceDomain, RaftPersistenceBatch, WalRaftNodeStores},
    telemetry::{self, Metric},
    BorrowedPersistedRaftLogEntry, PersistedRaftLogEntry, PersistedRaftSnapshot, RaftHardState,
    RaftHardStateStore, RaftLogSegment, RaftSnapshotStore,
};
use std::{
    collections::BTreeMap,
    env, fs, io,
    path::{Path, PathBuf},
    process::ExitCode,
    time::{Duration, Instant},
};

const HELP: &str = "usage: bench-wal-reclamation RETAINED ROUNDS BATCHES_PER_ROUND BATCH_SIZE PAYLOAD_BYTES SOURCE_SHA SCRATCH_PARENT";
const MAX_LIVE_PAYLOAD_BYTES: u64 = 512 * 1024 * 1024;
const PHASES: [&str; 4] = [
    "wal_reclamation",
    "wal_checkpoint_prepare",
    "wal_manifest_publish",
    "wal_cleanup",
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct Config {
    retained: u64,
    rounds: u64,
    batches_per_round: u64,
    batch_size: u64,
    payload_bytes: u64,
    source_sha: String,
    scratch_parent: PathBuf,
}

impl Config {
    fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let args: Vec<_> = args.into_iter().collect();
        if args.len() != 7 {
            return Err(format!("expected seven arguments; {HELP}"));
        }
        let config = Self {
            retained: bounded(&args[0], "RETAINED", 1, 1_000_000)?,
            rounds: bounded(&args[1], "ROUNDS", 1, 1_000)?,
            batches_per_round: bounded(&args[2], "BATCHES_PER_ROUND", 1, 1_000_000)?,
            batch_size: bounded(&args[3], "BATCH_SIZE", 1, 4_096)?,
            payload_bytes: bounded(&args[4], "PAYLOAD_BYTES", 1, 1024 * 1024)?,
            source_sha: args[5].clone(),
            scratch_parent: PathBuf::from(&args[6]),
        };
        if config.source_sha.len() != 40
            || !config
                .source_sha
                .bytes()
                .all(|byte| byte.is_ascii_hexdigit())
        {
            return Err("SOURCE_SHA must be exactly 40 hexadecimal characters".into());
        }
        if !config.scratch_parent.is_dir() {
            return Err("SCRATCH_PARENT must be an existing directory".into());
        }
        let live_payload = config
            .retained
            .checked_mul(config.payload_bytes)
            .ok_or("retained payload size overflows")?;
        if live_payload > MAX_LIVE_PAYLOAD_BYTES {
            return Err(format!(
                "retained payload exceeds the {MAX_LIVE_PAYLOAD_BYTES}-byte memory bound"
            ));
        }
        let appended = config
            .rounds
            .checked_mul(config.batches_per_round)
            .and_then(|value| value.checked_mul(config.batch_size))
            .ok_or("appended entry count overflows")?;
        config
            .retained
            .checked_add(appended)
            .filter(|total| *total < u64::MAX)
            .ok_or("final log index is not representable")?;
        Ok(config)
    }
}

#[derive(Clone, Debug)]
struct LatencySummary {
    count: usize,
    p50_ns: u64,
    p99_ns: u64,
    max_ns: u64,
}

impl LatencySummary {
    fn from_samples(samples: &[u64]) -> Self {
        assert!(!samples.is_empty(), "latency population is nonempty");
        let mut sorted = samples.to_vec();
        sorted.sort_unstable();
        Self {
            count: sorted.len(),
            p50_ns: nearest_rank(&sorted, 50),
            p99_ns: nearest_rank(&sorted, 99),
            max_ns: *sorted.last().expect("checked nonempty"),
        }
    }

    fn json(&self) -> String {
        format!(
            "{{\"count\":{},\"p50_ns\":{},\"p99_ns\":{},\"max_ns\":{}}}",
            self.count, self.p50_ns, self.p99_ns, self.max_ns
        )
    }
}

#[derive(Clone, Debug)]
struct WalInventory {
    names: Vec<String>,
    bytes: u64,
}

impl WalInventory {
    fn read(directory: &Path) -> io::Result<Self> {
        let mut names = Vec::new();
        let mut bytes = 0_u64;
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name
                .to_str()
                .ok_or_else(|| io::Error::other("non-UTF-8 replica filename"))?;
            if !is_managed_wal_name(name) {
                continue;
            }
            let metadata = entry.metadata()?;
            if !metadata.is_file() {
                return Err(io::Error::other(format!(
                    "managed WAL path is not a file: {name}"
                )));
            }
            names.push(name.to_owned());
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or_else(|| io::Error::other("managed WAL byte count overflows"))?;
        }
        names.sort();
        Ok(Self { names, bytes })
    }

    fn validate_published_generation(&self) -> Result<(), String> {
        let manifests = self
            .names
            .iter()
            .filter(|name| name.as_str() == "raft-wal-current")
            .count();
        let checkpoints = self
            .names
            .iter()
            .filter(|name| generation_name(name, "raft-wal-checkpoint-", ".rfwc"))
            .count();
        let segments = self
            .names
            .iter()
            .filter(|name| generation_name(name, "raft-wal-segment-", ".rfwb"))
            .count();
        if self.names.len() != 3 || manifests != 1 || checkpoints != 1 || segments != 1 {
            return Err(format!(
                "expected exactly one manifest, checkpoint, and segment; found {:?}",
                self.names
            ));
        }
        Ok(())
    }

    fn json(&self) -> String {
        let names = self
            .names
            .iter()
            .map(|name| format!("\"{name}\""))
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"file_count\":{},\"bytes\":{},\"names\":[{}]}}",
            self.names.len(),
            self.bytes,
            names
        )
    }
}

#[derive(Clone, Debug)]
struct Round {
    number: u64,
    appended_entries: u64,
    compacted_through: u64,
    snapshot_ns: u64,
    reclamation_pause_ns: u64,
    write_latency: LatencySummary,
    inventory: WalInventory,
}

impl Round {
    fn json(&self) -> String {
        format!(
            "{{\"round\":{},\"appended_entries\":{},\"compacted_through\":{},\"snapshot_ns\":{},\"reclamation_pause_ns\":{},\"write_latency\":{},\"managed_wal\":{}}}",
            self.number,
            self.appended_entries,
            self.compacted_through,
            self.snapshot_ns,
            self.reclamation_pause_ns,
            self.write_latency.json(),
            self.inventory.json()
        )
    }
}

#[derive(Clone, Debug)]
struct PhaseMetric {
    name: &'static str,
    metric: Metric,
}

impl PhaseMetric {
    fn json(&self) -> String {
        let buckets = self
            .metric
            .buckets
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        format!(
            "{{\"name\":\"{}\",\"calls\":{},\"total_ns\":{},\"max_ns\":{},\"buckets\":[{}]}}",
            self.name, self.metric.calls, self.metric.total_ns, self.metric.max_ns, buckets
        )
    }
}

fn main() -> ExitCode {
    let config = match Config::parse(env::args().skip(1)) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{error}");
            return ExitCode::from(2);
        }
    };
    match run(&config) {
        Ok(report) => {
            println!("{report}");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("WAL reclamation qualification failed: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(config: &Config) -> Result<String, Box<dyn std::error::Error>> {
    let directory = config
        .scratch_parent
        .join(format!("wal-reclamation-bench-{}", std::process::id()));
    fs::create_dir(&directory)?;
    let cleanup = Cleanup(directory.clone());
    let (hard, mut log, mut snapshots) = WalRaftNodeStores::open(&directory)?.into_parts();
    let domain = hard
        .persistence_domain()
        .ok_or("WAL hard state must expose its persistence domain")?;
    let payload = vec![0xa5; usize::try_from(config.payload_bytes)?];
    let seed = entries(1, config.retained, &payload);
    persist(&hard, &mut log, &domain, &seed, config.retained)?;

    let baseline = selected_phase_metrics();
    if baseline.iter().any(|metric| metric.metric.calls != 0) {
        return Err("reclamation telemetry was not initially empty".into());
    }

    let mut rounds = Vec::new();
    let mut all_write_latencies = Vec::new();
    let mut reclamation_pauses = Vec::new();
    let mut total = config.retained;
    for number in 1..=config.rounds {
        let mut write_latencies = Vec::new();
        for _ in 0..config.batches_per_round {
            let first = total.checked_add(1).ok_or("log index overflows")?;
            let batch = entries(first, config.batch_size, &payload);
            total = total
                .checked_add(config.batch_size)
                .ok_or("log index overflows")?;
            let started = Instant::now();
            persist(&hard, &mut log, &domain, &batch, total)?;
            let elapsed = elapsed_ns(started.elapsed());
            write_latencies.push(elapsed);
            all_write_latencies.push(elapsed);
        }

        let compacted = total
            .checked_sub(config.retained)
            .ok_or("retained suffix exceeds durable log")?;
        let snapshot_started = Instant::now();
        snapshots.write_snapshot(snapshot(compacted))?;
        let snapshot_ns = elapsed_ns(snapshot_started.elapsed());

        telemetry::set_enabled(true);
        let reclaim_started = Instant::now();
        let reclaim_result = log.compact_prefix_through(LogIndex(compacted));
        let reclamation_pause_ns = elapsed_ns(reclaim_started.elapsed());
        telemetry::set_enabled(false);
        reclaim_result?;
        reclamation_pauses.push(reclamation_pause_ns);

        if hard.current() != hard_state(total)
            || log.compacted_through() != LogIndex(compacted)
            || log.next_index() != LogIndex(total + 1)
        {
            return Err(format!("round {number} live state disagrees after reclamation").into());
        }
        verify_suffix(&log.replay_entries(), compacted + 1, total, config.retained)?;
        let inventory = WalInventory::read(&directory)?;
        inventory.validate_published_generation()?;
        rounds.push(Round {
            number,
            appended_entries: config.batches_per_round * config.batch_size,
            compacted_through: compacted,
            snapshot_ns,
            reclamation_pause_ns,
            write_latency: LatencySummary::from_samples(&write_latencies),
            inventory,
        });
    }

    let phases = selected_phase_metrics();
    validate_phases(&phases, config.rounds, &reclamation_pauses)?;
    let min_bytes = rounds
        .iter()
        .map(|round| round.inventory.bytes)
        .min()
        .ok_or("missing rounds")?;
    let max_bytes = rounds
        .iter()
        .map(|round| round.inventory.bytes)
        .max()
        .ok_or("missing rounds")?;
    if min_bytes != max_bytes {
        return Err(format!(
            "fixed live suffix did not produce a fixed managed WAL size: {min_bytes}..={max_bytes}"
        )
        .into());
    }

    drop((hard, log, snapshots));
    let reopen_started = Instant::now();
    let (hard, log, snapshots) = WalRaftNodeStores::open(&directory)?.into_parts();
    let reopen_ns = elapsed_ns(reopen_started.elapsed());
    let final_compacted = total - config.retained;
    if hard.current() != hard_state(total)
        || log.compacted_through() != LogIndex(final_compacted)
        || log.next_index() != LogIndex(total + 1)
    {
        return Err("reopened hard state or log boundary disagrees".into());
    }
    verify_suffix(
        &log.replay_entries(),
        final_compacted + 1,
        total,
        config.retained,
    )?;
    let reopened_inventory = WalInventory::read(&directory)?;
    reopened_inventory.validate_published_generation()?;
    if reopened_inventory.bytes != max_bytes {
        return Err("reopen changed the authoritative managed WAL size".into());
    }
    drop((hard, log, snapshots));

    let rounds_json = rounds.iter().map(Round::json).collect::<Vec<_>>().join(",");
    let phases_json = phases
        .iter()
        .map(PhaseMetric::json)
        .collect::<Vec<_>>()
        .join(",");
    let report = format!(
        "{{\"schema\":1,\"layer\":\"durable_replication\",\"comparison_kind\":\"rafter_component_qualification\",\"source_sha\":\"{}\",\"completion_boundary\":\"durable WAL batch receipt; reclamation pause ends after manifest publication and cleanup fence\",\"config\":{{\"retained_entries\":{},\"rounds\":{},\"batches_per_round\":{},\"batch_size\":{},\"payload_bytes\":{}}},\"rounds\":[{}],\"aggregate\":{{\"write_latency\":{},\"reclamation_pause\":{},\"reopen_ns\":{},\"managed_wal_bytes\":{},\"managed_wal_files\":3}},\"phase_metrics\":[{}],\"final\":{{\"hard_commit\":{},\"compacted_through\":{},\"retained_entries\":{},\"next_index\":{},\"recovery_verified\":true,\"physical_bound_verified\":true,\"managed_wal\":{}}}}}",
        config.source_sha.to_ascii_lowercase(),
        config.retained,
        config.rounds,
        config.batches_per_round,
        config.batch_size,
        config.payload_bytes,
        rounds_json,
        LatencySummary::from_samples(&all_write_latencies).json(),
        LatencySummary::from_samples(&reclamation_pauses).json(),
        reopen_ns,
        max_bytes,
        phases_json,
        total,
        final_compacted,
        config.retained,
        total + 1,
        reopened_inventory.json()
    );
    drop(cleanup);
    Ok(report)
}

fn bounded(value: &str, name: &str, minimum: u64, maximum: u64) -> Result<u64, String> {
    let parsed = value
        .parse::<u64>()
        .map_err(|_| format!("{name} must be an integer"))?;
    if !(minimum..=maximum).contains(&parsed) {
        return Err(format!("{name} must be in {minimum}..={maximum}"));
    }
    Ok(parsed)
}

fn entries(first: u64, count: u64, payload: &[u8]) -> Vec<PersistedRaftLogEntry> {
    (first..first + count)
        .map(|index| PersistedRaftLogEntry::application(LogIndex(index), Term(1), payload.to_vec()))
        .collect()
}

fn hard_state(commit: u64) -> RaftHardState {
    RaftHardState {
        current_term: Term(1),
        commit_index: LogIndex(commit),
        ..RaftHardState::default()
    }
}

fn persist(
    hard: &impl RaftHardStateStore,
    log: &mut impl RaftLogSegment,
    domain: &PersistenceDomain,
    entries: &[PersistedRaftLogEntry],
    commit: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let borrowed: Vec<_> = entries
        .iter()
        .map(|entry| BorrowedPersistedRaftLogEntry::new(entry.index, entry.term, &entry.kind))
        .collect();
    let receipt = log
        .persist_batch(
            domain,
            RaftPersistenceBatch {
                truncate_from: None,
                entries: &borrowed,
                hard_state: Some(hard_state(commit)),
            },
        )?
        .ok_or("nonempty WAL batch did not return a durable receipt")?;
    if receipt.hard_state != hard_state(commit)
        || receipt.next_index != LogIndex(commit + 1)
        || hard.current() != hard_state(commit)
    {
        return Err("durable receipt disagrees with acknowledged state".into());
    }
    Ok(())
}

fn snapshot(index: u64) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: RaftSnapshotMetadata::new(
            SnapshotGroupId::new("wal-reclamation-benchmark").expect("valid constant group"),
            NodeId(1),
            LogIndex(index),
            Term(1),
            Term(1),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("benchmark-state").expect("valid constant kind"),
                ApplicationSnapshotVersion::new(1).expect("nonzero constant version"),
            ),
        )
        .expect("valid benchmark snapshot metadata"),
        application_payload: format!("state-through-{index}").into_bytes(),
    }
}

fn verify_suffix(
    entries: &[PersistedRaftLogEntry],
    first: u64,
    last: u64,
    expected_len: u64,
) -> Result<(), String> {
    if u64::try_from(entries.len()).ok() != Some(expected_len)
        || entries.first().map(|entry| entry.index) != Some(LogIndex(first))
        || entries.last().map(|entry| entry.index) != Some(LogIndex(last))
        || entries
            .windows(2)
            .any(|pair| pair[1].index != pair[0].index.next())
    {
        return Err(format!(
            "retained suffix is not the complete ordered interval {first}..={last}"
        ));
    }
    Ok(())
}

fn selected_phase_metrics() -> Vec<PhaseMetric> {
    let metrics: BTreeMap<_, _> = telemetry::snapshot().into_iter().collect();
    PHASES
        .into_iter()
        .map(|name| PhaseMetric {
            name,
            metric: metrics
                .get(name)
                .unwrap_or_else(|| panic!("missing telemetry stage {name}"))
                .clone(),
        })
        .collect()
}

fn validate_phases(phases: &[PhaseMetric], rounds: u64, pauses: &[u64]) -> Result<(), String> {
    let overall = phases
        .iter()
        .find(|phase| phase.name == "wal_reclamation")
        .ok_or("missing overall reclamation metric")?;
    for phase in phases {
        if phase.metric.calls != rounds
            || phase.metric.max_ns == 0
            || phase.metric.max_ns > phase.metric.total_ns
            || phase.metric.buckets.iter().sum::<u64>() != rounds
        {
            return Err(format!(
                "incoherent {} telemetry: {:?}",
                phase.name, phase.metric
            ));
        }
        if phase.metric.total_ns > overall.metric.total_ns {
            return Err(format!("{} exceeds enclosing reclamation time", phase.name));
        }
    }
    let measured_pause_total = pauses.iter().try_fold(0_u64, |total, value| {
        total.checked_add(*value).ok_or("pause time overflows")
    })?;
    if overall.metric.total_ns > measured_pause_total {
        return Err("reclamation telemetry exceeds measured synchronous pauses".into());
    }
    Ok(())
}

fn nearest_rank(sorted: &[u64], percentile: usize) -> u64 {
    assert!(!sorted.is_empty());
    assert!((1..=100).contains(&percentile));
    let rank = sorted.len().saturating_mul(percentile).div_ceil(100);
    sorted[rank.saturating_sub(1)]
}

fn elapsed_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn is_managed_wal_name(name: &str) -> bool {
    name == "hard-state"
        || name == "raft-wal-current"
        || name.starts_with(".raft-wal-current-")
        || name.starts_with("raft-wal-checkpoint-")
        || name.starts_with("raft-wal-segment-")
}

fn generation_name(name: &str, prefix: &str, suffix: &str) -> bool {
    name.strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(suffix))
        .is_some_and(|generation| {
            generation.len() == 20 && generation.bytes().all(|byte| byte.is_ascii_digit())
        })
}

struct Cleanup(PathBuf);

impl Drop for Cleanup {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sha() -> String {
        "0123456789abcdef0123456789abcdef01234567".into()
    }

    #[test]
    fn config_rejects_identity_bounds_and_missing_parent() {
        let temp = env::temp_dir();
        let good = vec![
            "10000".into(),
            "3".into(),
            "16".into(),
            "4".into(),
            "64".into(),
            sha(),
            temp.display().to_string(),
        ];
        assert!(Config::parse(good.clone()).is_ok());
        let mut bad = good.clone();
        bad[5] = "HEAD".into();
        assert!(Config::parse(bad).unwrap_err().contains("SOURCE_SHA"));
        let mut bad = good.clone();
        bad[0] = "0".into();
        assert!(Config::parse(bad).unwrap_err().contains("RETAINED"));
        let mut bad = good;
        bad[6] = temp
            .join("rafter-definitely-missing-parent")
            .display()
            .to_string();
        assert!(Config::parse(bad)
            .unwrap_err()
            .contains("existing directory"));
    }

    #[test]
    fn nearest_rank_uses_the_declared_sample_population() {
        let samples: Vec<_> = (1..=100).collect();
        assert_eq!(nearest_rank(&samples, 50), 50);
        assert_eq!(nearest_rank(&samples, 99), 99);
        assert_eq!(LatencySummary::from_samples(&samples).max_ns, 100);
    }

    #[test]
    fn inventory_accepts_only_one_complete_published_generation() {
        let inventory = WalInventory {
            names: vec![
                "raft-wal-checkpoint-00000000000000000001.rfwc".into(),
                "raft-wal-current".into(),
                "raft-wal-segment-00000000000000000001.rfwb".into(),
            ],
            bytes: 123,
        };
        assert!(inventory.validate_published_generation().is_ok());
        let mut stale = inventory.clone();
        stale.names.push("hard-state".into());
        assert!(stale.validate_published_generation().is_err());
        assert!(!generation_name(
            "raft-wal-segment-1.rfwb",
            "raft-wal-segment-",
            ".rfwb"
        ));
    }
}
