mod errors;
mod projection;
mod render;
mod types;

pub use errors::{TlaProjectionFailure, TlaTraceRenderError};
pub use projection::{project_raft_trace_to_tla, require_tla_projectable_raft_trace};
pub use render::render_tla_trace_spec;
pub use types::{TlaAbstractionGap, TlaAction, TlaProjection, TlaTraceSpec, TlaTraceStep};
