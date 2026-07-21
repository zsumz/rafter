//! Binary entry point: parse user intent and delegate to the CLI domain.

use std::process::ExitCode;

use clap::Parser;

mod cli;

fn main() -> ExitCode {
    match cli::run(cli::Cli::parse()) {
        Ok(green) if green => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("rafter-invariants: {error}");
            ExitCode::from(2)
        }
    }
}
