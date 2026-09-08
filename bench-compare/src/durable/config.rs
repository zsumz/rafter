//! Validated, bounded workload selection for repeatable durable measurements.

use std::path::PathBuf;

pub(super) const HELP: &str = "bench-rafter-durable [OPTIONS]
  --workload all|proposals|snapshot  Select measured workloads (default: all)
  --proposals N                    Fixed proposals per workload (default: 512)
  --batch-sizes N[,N...]            Proposal batch sizes (default: 1,32)
  --payload-bytes N                Proposal payload bytes (default: 256)
  --snapshot-bytes N               Snapshot payload bytes (default: 33554432)
  --directory PATH                 Existing scratch parent (default: system temp)
  --help                           Print this help
Use at least 1,000 batches for sustained evidence. Nodes share one filesystem.";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Workload {
    All,
    Proposals,
    Snapshot,
}

#[derive(Debug)]
pub(super) struct Config {
    pub(super) workload: Workload,
    pub(super) proposals: usize,
    pub(super) batch_sizes: Vec<usize>,
    pub(super) payload_bytes: usize,
    pub(super) snapshot_bytes: usize,
    pub(super) directory: PathBuf,
}

impl Config {
    pub(super) fn parse(args: impl IntoIterator<Item = String>) -> Result<Option<Self>, String> {
        let mut config = Self {
            workload: Workload::All,
            proposals: 512,
            batch_sizes: vec![1, 32],
            payload_bytes: 256,
            snapshot_bytes: 32 * 1024 * 1024,
            directory: std::env::temp_dir(),
        };
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            if flag == "--help" {
                return Ok(None);
            }
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {flag}"))?;
            match flag.as_str() {
                "--workload" => {
                    config.workload = match value.as_str() {
                        "all" => Workload::All,
                        "proposals" => Workload::Proposals,
                        "snapshot" => Workload::Snapshot,
                        _ => return Err(format!("invalid workload: {value}")),
                    };
                }
                "--proposals" => config.proposals = number(&flag, &value, 1_048_576)?,
                "--payload-bytes" => config.payload_bytes = number(&flag, &value, 524_224)?,
                "--snapshot-bytes" => {
                    config.snapshot_bytes = number(&flag, &value, 1024 * 1024 * 1024)?;
                }
                "--batch-sizes" => {
                    config.batch_sizes = value
                        .split(',')
                        .map(|value| number(&flag, value, 1024))
                        .collect::<Result<_, _>>()?;
                }
                "--directory" => config.directory = PathBuf::from(value),
                _ => return Err(format!("unknown option: {flag}")),
            }
        }
        if config.workload != Workload::Snapshot {
            if config.proposals.saturating_mul(config.payload_bytes) > 512 * 1024 * 1024 {
                return Err("proposal payload total exceeds 512 MiB per node".into());
            }
            for (index, batch) in config.batch_sizes.iter().enumerate() {
                if *batch > config.proposals || config.batch_sizes[..index].contains(batch) {
                    return Err("batch sizes must be unique and no greater than proposals".into());
                }
            }
        }
        if !config.directory.is_dir() {
            return Err("--directory must name an existing directory".into());
        }
        Ok(Some(config))
    }
}

fn number(flag: &str, value: &str, max: usize) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|number| (1..=max).contains(number))
        .ok_or_else(|| format!("{flag} must be an integer in 1..={max}"))
}

#[cfg(test)]
#[path = "config_test.rs"]
mod tests;
