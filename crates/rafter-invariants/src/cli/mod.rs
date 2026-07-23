//! Binary command vocabulary and adaptation facade.

mod command;
mod dispatch;

pub(crate) use command::Cli;
pub(crate) use dispatch::run;
