//! Serialized simulator evidence fixture vocabulary and builders.

use super::super::simulator_compiler_artifact_executable;

#[path = "fixtures/checkout.rs"]
mod checkout;
#[path = "fixtures/compile.rs"]
mod compile;
#[path = "fixtures/io.rs"]
mod io;
#[path = "fixtures/materialize.rs"]
mod materialize;
#[path = "fixtures/model.rs"]
mod model;
#[path = "fixtures/runtime.rs"]
mod runtime;
#[path = "fixtures/substitution.rs"]
mod substitution;

pub(super) use materialize::{materialize_cross_root_fixture, materialize_fixture};
pub(super) use model::{ProvenanceSubstitution, RuntimeDefect, SimulatorFixture};
