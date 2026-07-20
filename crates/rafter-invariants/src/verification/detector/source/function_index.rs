//! Function identity, declaration indexes, and local call resolution.

mod catalog;
mod model;
mod path_syntax;
mod resolver;
mod target;

pub(super) use catalog::FunctionIndex;
pub(super) use model::{CallTarget, FunctionId};
pub(super) use resolver::LocalCallResolver;
