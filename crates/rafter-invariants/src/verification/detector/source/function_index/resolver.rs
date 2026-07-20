//! Local resolver state and its focused indexing and resolution policies.

mod calls;
mod imports;
mod inventory;
mod methods;
mod model;
mod types;

pub(in crate::verification::detector::source) use model::LocalCallResolver;
