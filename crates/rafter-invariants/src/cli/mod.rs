//! Binary command vocabulary and adaptation facade.

mod check;
mod command;
mod dispatch;
mod document;
mod publication;
mod report;

pub(crate) use command::Cli;
pub(crate) use dispatch::run;
