//! Three-node file-backed throughput and latency benchmark.
//!
//! Select workloads, fixed proposal counts, batch sizes, payload sizes, and a
//! scratch parent with `--help`. The default invocation retains the original
//! smoke workloads; sustained evidence should use at least 1,000 batches.
//! Messages run synchronously, and all three nodes share the selected filesystem.

#[path = "../durable/cluster.rs"]
mod cluster;
#[path = "../durable/config.rs"]
mod config;
#[path = "../durable/report.rs"]
mod report;
#[path = "../durable/stores.rs"]
mod stores;
#[path = "../durable/workloads.rs"]
mod workloads;

use config::{Config, Workload};

fn main() -> std::process::ExitCode {
    let config = match Config::parse(std::env::args().skip(1)) {
        Ok(Some(config)) => config,
        Ok(None) => {
            println!("{}", config::HELP);
            return std::process::ExitCode::SUCCESS;
        }
        Err(error) => {
            eprintln!("{error}\n{}", config::HELP);
            return std::process::ExitCode::from(2);
        }
    };
    let scratch = config
        .directory
        .join(format!("rafter-bench-{}", std::process::id()));
    std::fs::create_dir(&scratch).expect("fresh benchmark scratch directory");
    let mut reports = Vec::new();
    if config.workload != Workload::Snapshot {
        for batch in &config.batch_sizes {
            let directory = scratch.join(format!("proposals-{batch}"));
            let metrics = workloads::proposal_workload(&directory, *batch, &config);
            reports.push(report::proposal_json(&metrics));
        }
    }
    if config.workload != Workload::Proposals {
        let metrics = workloads::snapshot_workload(
            &scratch.join("snapshot"),
            config.snapshot_bytes,
            config.hard_state,
        );
        reports.push(report::snapshot_json(&metrics));
    }
    std::fs::remove_dir_all(&scratch).expect("benchmark scratch directory is removable");
    println!(
        "{}",
        report::report_json(&reports, config.hard_state.label())
    );
    std::process::ExitCode::SUCCESS
}
