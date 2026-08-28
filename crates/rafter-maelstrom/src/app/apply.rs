//! Applying one committed command, and the crash point that fires between the
//! durable checkpoint and the client's reply.
//!
//! The outcome type is the contract this file holds: the mutation has already
//! landed by the time a checkpoint can fail, so every arm still names an answer
//! the caller owes.

use std::path::Path;

use rafter::LogIndex;

use super::{
    apply_mutation, claim_app_persist_crash_point_once, persist_app_state, AppState, ClientResult,
    Command, APP_PERSIST_CRASH_EXIT_CODE, CRASH_AFTER_APP_PERSIST_ONCE_ENV,
};

/// What a crash point decided after the checkpoint reached the medium.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AfterAppPersist {
    /// No crash point armed; apply the command and answer normally.
    Continue,
    /// A crash point fired. The checkpoint is durable and the client was never
    /// answered — the window a restart must survive without losing the write.
    Interrupt,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum CommandApplyOutcome {
    /// The command applied and the application checkpoint was written.
    Applied(ClientResult),
    /// The command applied into the in-memory state machine, but the
    /// application checkpoint could not be written.
    ///
    /// The answer is owed exactly as it is on the `Applied` path, and the
    /// result is carried out here rather than discarded so the caller can pay
    /// it. The mutation is real: `app.kv` carries it and `app.applied` has
    /// moved past `index`, so every later read on this node — and the
    /// already-applied check on every later command — sees it. And it is
    /// durable, because durability was never this checkpoint's job: the Raft
    /// log holds the committed entry, the checkpoint only lets recovery skip
    /// replaying it. A restart therefore replays the entry and applies it
    /// again, reaching the same state the answer described.
    AppliedWithoutCheckpoint {
        result: ClientResult,
        error: String,
    },
    AlreadyApplied,
    Interrupted,
}

/// Applies one committed command into the state machine and reports what the
/// caller owes for it.
///
/// Infallible by construction. The only fallible step is the application
/// checkpoint, and it runs *after* the mutation has already landed in `app`, so
/// there is no outcome in which the command did not apply. Reporting a failed
/// checkpoint as an error would hand the caller a value that carries no result
/// to answer with while the mutation it answers for has already happened — the
/// shape that stranded the write. `AppliedWithoutCheckpoint` carries both.
pub(crate) fn apply_committed_command(
    root: &Path,
    app: &mut AppState,
    index: LogIndex,
    command: &Command,
    after_persist: impl FnOnce(&Path) -> AfterAppPersist,
) -> CommandApplyOutcome {
    if index <= app.applied {
        return CommandApplyOutcome::AlreadyApplied;
    }

    let result = apply_mutation(&mut app.kv, &command.request);
    app.applied = index;
    if let Err(error) = persist_app_state(root, app) {
        return CommandApplyOutcome::AppliedWithoutCheckpoint {
            result,
            error: error.to_string(),
        };
    }

    if after_persist(root) == AfterAppPersist::Interrupt {
        return CommandApplyOutcome::Interrupted;
    }
    CommandApplyOutcome::Applied(result)
}

pub(crate) fn maybe_crash_after_app_persist_before_reply(root: &Path) {
    if std::env::var_os(CRASH_AFTER_APP_PERSIST_ONCE_ENV).is_none() {
        return;
    }
    if claim_app_persist_crash_point_once(root) {
        eprintln!(
            "rafter-maelstrom crashpoint={CRASH_AFTER_APP_PERSIST_ONCE_ENV} fired after app persist before reply"
        );
        std::process::exit(APP_PERSIST_CRASH_EXIT_CODE);
    }
}
