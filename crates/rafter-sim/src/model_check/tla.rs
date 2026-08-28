//! Projection of simulator traces into a TLA+ trace specification.
//!
//! A projection must be total or refused: a trace the abstraction cannot
//! represent is reported as an explicit abstraction gap rather than quietly
//! rendered into a weaker spec. Rendering produces input for an external
//! checker; nothing here decides whether the trace is correct.

mod errors;
mod projection;
mod render;
mod types;

pub use errors::{TlaProjectionFailure, TlaTraceRenderError};
pub use projection::{project_raft_trace_to_tla, require_tla_projectable_raft_trace};
pub use render::render_tla_trace_spec;
pub use types::{TlaAbstractionGap, TlaAction, TlaProjection, TlaTraceSpec, TlaTraceStep};
