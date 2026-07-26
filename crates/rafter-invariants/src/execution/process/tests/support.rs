//! Descriptor-bound shell requests and collision-resistant fixture paths.

use std::{
    collections::BTreeMap,
    error::Error,
    ffi::OsString,
    fs::File,
    os::fd::AsFd,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

#[cfg(unix)]
use command_fds::{CommandFdExt, FdMapping};

use super::super::{
    run, CleanupFailures, FinalizationPolicy, ManagedProcess, NoSignalReaper, PendingProcessOutput,
    ProcessArtifactPaths, ProcessDeadlines, ProcessGroupAnchor, ProcessObserver, ProcessOutput,
    ProcessRequest, ProcessRuntime, RuntimeExecutable, TerminationPolicy,
};

static TEST_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// How long this machine needs, right now, to run one trivial process through
/// the real launcher chain.
///
/// Fixtures that have to let the launcher finish before their own deadlines
/// bite used to allow a constant picked on an idle machine. A constant is the
/// wrong shape for that allowance: it says "the launcher is done" when it means
/// "some milliseconds passed". Measuring says the same thing in terms the
/// machine can honour, so a contended host widens every derived window in
/// proportion instead of failing.
pub(super) fn measured_launch_cost() -> Duration {
    run_shell(
        "exit 0",
        &super::super::base_environment(),
        Path::new("."),
        Duration::from_secs(30),
        Duration::from_millis(20),
    )
    .expect("measure the launcher chain with a trivial process")
    .duration
}

pub(super) fn run_shell(
    script: &str,
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    grace: Duration,
) -> Result<ProcessOutput, Box<dyn Error>> {
    run_shell_with_finalization(
        script,
        environment,
        current_dir,
        timeout,
        grace,
        FinalizationPolicy::bounded(Duration::from_secs(5)),
    )
}

pub(super) fn run_shell_with_finalization(
    script: &str,
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    grace: Duration,
    finalization: FinalizationPolicy,
) -> Result<ProcessOutput, Box<dyn Error>> {
    let deadlines = test_process_deadlines(timeout, grace, finalization)?;
    run_shell_with_deadlines(
        script,
        environment,
        current_dir,
        deadlines,
        grace,
        finalization,
    )
}

pub(super) fn run_shell_with_artifact_paths(
    script: &str,
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    timeout: Duration,
    grace: Duration,
) -> Result<(ProcessOutput, ProcessArtifactPaths), Box<dyn Error>> {
    let finalization = FinalizationPolicy::bounded(Duration::from_secs(5));
    let deadlines = test_process_deadlines(timeout, grace, finalization)?;
    let pending = run_shell_pending_with_deadlines(
        script,
        environment,
        current_dir,
        deadlines,
        grace,
        finalization,
    )?;
    let artifacts = pending.artifact_paths();
    Ok((pending.finalize()?, artifacts))
}

pub(super) fn run_shell_with_deadlines(
    script: &str,
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    deadlines: ProcessDeadlines,
    grace: Duration,
    finalization: FinalizationPolicy,
) -> Result<ProcessOutput, Box<dyn Error>> {
    run_shell_pending_with_deadlines(
        script,
        environment,
        current_dir,
        deadlines,
        grace,
        finalization,
    )?
    .finalize()
}

fn run_shell_pending_with_deadlines(
    script: &str,
    environment: &BTreeMap<String, String>,
    current_dir: &Path,
    deadlines: ProcessDeadlines,
    grace: Duration,
    finalization: FinalizationPolicy,
) -> Result<PendingProcessOutput, Box<dyn Error>> {
    let executable_path = Path::new("/bin/sh");
    let executable = File::open(executable_path)?;
    let perl = File::open("/usr/bin/perl")?;
    let time = File::open("/usr/bin/time")?;
    #[cfg(target_os = "macos")]
    let ps_path = Path::new("/bin/ps");
    #[cfg(not(target_os = "macos"))]
    let ps_path = Path::new("/usr/bin/ps");
    let ps = File::open(ps_path)?;
    let working_directory = File::open(current_dir)?;
    let arguments = [OsString::from("-c"), OsString::from(script)];
    run(&ProcessRequest {
        program: "sh",
        executable_path,
        arguments: &arguments,
        environment,
        deadlines,
        termination: TerminationPolicy {
            grace,
            publication_timeout: Duration::from_secs(5),
            kill_confirmation_timeout: Duration::from_secs(5),
        },
        finalization,
        runtime: ProcessRuntime {
            perl: RuntimeExecutable {
                path: Path::new("/usr/bin/perl"),
                descriptor: perl.as_fd(),
            },
            time: RuntimeExecutable {
                path: Path::new("/usr/bin/time"),
                descriptor: time.as_fd(),
            },
            observer: RuntimeExecutable {
                path: ps_path,
                descriptor: ps.as_fd(),
            },
        },
        executable_descriptor: executable.as_fd(),
        target_descriptor: executable.as_fd(),
        working_directory_descriptor: working_directory.as_fd(),
        inherited_descriptors: &[],
    })
}

fn test_process_deadlines(
    timeout: Duration,
    grace: Duration,
    finalization: FinalizationPolicy,
) -> Result<ProcessDeadlines, Box<dyn Error>> {
    let now = std::time::Instant::now();
    let execution_window = now
        .checked_add(Duration::from_secs(5))
        .and_then(|deadline| deadline.checked_add(timeout))
        .ok_or("test execution window overflow")?;
    let lifecycle = execution_window
        .checked_add(grace)
        .and_then(|deadline| deadline.checked_add(Duration::from_secs(15)))
        .ok_or("test lifecycle deadline overflow")?;
    let finalization_start = lifecycle
        .checked_sub(finalization.timeout)
        .ok_or("test finalization boundary underflow")?;
    let cleanup_start = finalization_start
        .checked_sub(Duration::from_secs(5))
        .ok_or("test cleanup boundary underflow")?;
    Ok(ProcessDeadlines::new(
        timeout,
        execution_window,
        cleanup_start,
        finalization_start,
        lifecycle,
    )?)
}

pub(super) fn unique_test_path(label: &str) -> PathBuf {
    let sequence = TEST_SEQUENCE.fetch_add(1, Ordering::Relaxed);
    PathBuf::from("target/rafter-invariants/process-tests").join(format!(
        "rafter-invariants-{label}-{}-{sequence}",
        std::process::id()
    ))
}

pub(super) fn process_observer() -> ProcessObserver {
    #[cfg(target_os = "macos")]
    let path = Path::new("/bin/ps");
    #[cfg(not(target_os = "macos"))]
    let path = Path::new("/usr/bin/ps");
    let file = File::open(path).expect("open process observer");
    ProcessObserver::capture(
        RuntimeExecutable {
            path,
            descriptor: file.as_fd(),
        },
        NoSignalReaper::start().expect("start process-observer reaper"),
    )
    .expect("capture process observer")
}

#[cfg(unix)]
pub(super) fn managed_process_fixture(
    wrapper_script: &str,
    cleanup_window: Duration,
    confirmation_timeout: Duration,
    cleanup_failures: CleanupFailures,
    observer: Option<ProcessObserver>,
) -> (ManagedProcess, u32, u32, NoSignalReaper, std::time::Instant) {
    use std::os::unix::process::CommandExt;

    let reaper = NoSignalReaper::start().expect("start fixture process reaper");
    let perl_path = Path::new("/usr/bin/perl");
    let perl = File::open(perl_path).expect("open fixture Perl runtime");
    let now = std::time::Instant::now();
    let anchor = ProcessGroupAnchor::spawn(
        RuntimeExecutable {
            path: perl_path,
            descriptor: perl.as_fd(),
        },
        now + Duration::from_secs(2),
        reaper.clone(),
        std::process::Stdio::null(),
    )
    .expect("spawn fixture process-group anchor");
    let target_group = anchor.id();
    let (target_lifetime, target_lifetime_writer) =
        super::super::TargetLifetimeLease::create().expect("create fixture target lifetime lease");
    let mut command = std::process::Command::new("sh");
    command.args(["-c", wrapper_script]).process_group(0);
    command
        .fd_mappings(vec![FdMapping {
            parent_fd: target_lifetime_writer
                .as_fd()
                .try_clone_to_owned()
                .expect("clone fixture target lifetime writer"),
            child_fd: std::os::fd::AsRawFd::as_raw_fd(&target_lifetime_writer),
        }])
        .expect("inherit fixture target lifetime writer");
    let child = command.spawn().expect("spawn fixture resource wrapper");
    drop(target_lifetime_writer);
    let wrapper_group = child.id();
    let cleanup_deadline = std::time::Instant::now() + cleanup_window;
    let process = ManagedProcess::new(
        child,
        anchor,
        cleanup_deadline,
        confirmation_timeout,
        cleanup_failures,
        observer,
        reaper.clone(),
        target_lifetime,
    );
    (
        process,
        wrapper_group,
        target_group,
        reaper,
        cleanup_deadline,
    )
}
