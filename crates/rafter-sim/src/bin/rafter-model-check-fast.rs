//! The model-check driver CI runs: one profile per invocation.
//!
//! Every check this binary runs must either exhaust its frontier or fail the
//! process, and each verdict must leave a machine-readable `RAFTER_EVENT` line
//! behind, because CI grades a run from those lines rather than the exit code
//! alone. Profile budgets are data, not policy; they live in `profile`.

use std::{env, error::Error};

#[path = "rafter_model_check_fast/profile.rs"]
mod profile;
#[path = "rafter_model_check_fast/raft_config.rs"]
mod raft_config;
#[path = "rafter_model_check_fast/reporting.rs"]
mod reporting;
#[path = "rafter_model_check_fast/runner.rs"]
mod runner;

use profile::{parse_profile, print_profiles, ProfileSelection};
use runner::run_profile;

#[cfg(test)]
pub(crate) use profile::{Profile, ProfileRun};
#[cfg(test)]
pub(crate) use reporting::{failure_timeline_lines, raft_summary_line_for_counts};

fn main() -> Result<(), Box<dyn Error>> {
    match parse_profile(env::args().skip(1))? {
        ProfileSelection::List => {
            print_profiles();
            Ok(())
        }
        ProfileSelection::Run(run) => run_profile(run),
    }
}

#[cfg(test)]
#[path = "rafter_model_check_fast/tests.rs"]
mod tests;
