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
