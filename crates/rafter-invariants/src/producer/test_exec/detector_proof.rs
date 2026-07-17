use std::{collections::BTreeMap, error::Error, ffi::OsString, path::Path};

#[cfg(all(test, unix))]
use std::time::Instant;

#[cfg(unix)]
use std::{
    fs::{self, File},
    io::{Read, Write},
    net::Shutdown,
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::{Duration, SystemTime},
};

use super::super::process;

pub(super) struct Execution {
    pub output: process::ProcessOutput,
    pub challenge: String,
    pub channel_error: Option<String>,
}

#[cfg(unix)]
pub(super) fn execute(
    program: &str,
    arguments: &[OsString],
    environment: &mut BTreeMap<String, String>,
) -> Result<Execution, Box<dyn Error>> {
    let (socket, responder, challenge) = challenge_listener()?;
    environment.insert(
        crate::detector_proof::PROOF_SOCKET_ENV.to_owned(),
        socket.to_string_lossy().into_owned(),
    );
    let output = process::timed_for(
        process::ProcessKind::TestExecution,
        program,
        arguments,
        environment,
        Path::new("."),
    );
    complete_execution(output, responder.finish(), &challenge)
}

#[cfg(not(unix))]
pub(super) fn execute(
    _program: &str,
    _arguments: &[OsString],
    _environment: &mut BTreeMap<String, String>,
) -> Result<Execution, Box<dyn Error>> {
    Err("detector proof requires Unix domain sockets".into())
}

#[cfg(all(test, unix))]
pub(super) fn execute_for_test(
    program: &str,
    arguments: &[OsString],
    environment: &mut BTreeMap<String, String>,
) -> Result<Execution, Box<dyn Error>> {
    let (socket, responder, challenge) = challenge_listener()?;
    environment.insert(
        crate::detector_proof::PROOF_SOCKET_ENV.to_owned(),
        socket.to_string_lossy().into_owned(),
    );
    let invocation = process::expected_invocation(program, arguments, environment, Path::new("."))?;
    let started = Instant::now();
    let mut command = std::process::Command::new(program);
    command
        .args(arguments)
        .env_clear()
        .envs(&*environment)
        .current_dir(".");
    let captured = command.output()?;
    let output = process::ProcessOutput {
        invocation,
        status: captured.status,
        stdout: captured.stdout,
        stderr: captured.stderr,
        duration: started.elapsed(),
        peak_rss_kib: 1,
        timed_out: false,
        termination: None,
    };
    complete_execution(Ok(output), responder.finish(), &challenge)
}

#[cfg(all(test, not(unix)))]
pub(super) fn execute_for_test(
    _program: &str,
    _arguments: &[OsString],
    _environment: &mut BTreeMap<String, String>,
) -> Result<Execution, Box<dyn Error>> {
    Err("detector proof requires Unix domain sockets".into())
}

#[cfg(unix)]
fn complete_execution(
    output: Result<process::ProcessOutput, Box<dyn Error>>,
    response: Result<bool, Box<dyn Error>>,
    challenge: &[u8; crate::detector_proof::CHALLENGE_BYTES],
) -> Result<Execution, Box<dyn Error>> {
    match output {
        Ok(output) => Ok(Execution {
            output,
            challenge: crate::detector_proof::encode_challenge(challenge),
            channel_error: response.err().map(|error| error.to_string()),
        }),
        Err(process_error) => match response {
            Ok(_) => Err(process_error),
            Err(channel_error) => Err(format!(
                "{process_error}; detector proof channel also failed: {channel_error}"
            )
            .into()),
        },
    }
}

#[cfg(unix)]
fn challenge_listener() -> Result<
    (
        PathBuf,
        ChallengeResponder,
        [u8; crate::detector_proof::CHALLENGE_BYTES],
    ),
    Box<dyn Error>,
> {
    static NEXT_SOCKET: AtomicU64 = AtomicU64::new(0);
    let mut challenge = [0_u8; crate::detector_proof::CHALLENGE_BYTES];
    let mut socket_nonce = [0_u8; crate::detector_proof::SOCKET_NONCE_BYTES];
    let mut random = File::open("/dev/urandom")?;
    random.read_exact(&mut challenge)?;
    random.read_exact(&mut socket_nonce)?;
    let directory = Path::new(crate::detector_proof::PROOF_SOCKET_DIRECTORY);
    prepare_socket_directory(directory)?;
    let socket_nonce = crate::detector_proof::encode_socket_nonce(&socket_nonce);
    let socket = directory.join(format!(
        "{}-{}-{}.sock",
        std::process::id(),
        NEXT_SOCKET.fetch_add(1, Ordering::Relaxed),
        socket_nonce,
    ));
    if !crate::detector_proof::managed_socket_path(&socket) {
        return Err("constructed detector proof socket has an invalid path".into());
    }
    let listener = UnixListener::bind(&socket)?;
    if let Err(error) = fs::set_permissions(&socket, fs::Permissions::from_mode(0o600)) {
        let _ = fs::remove_file(&socket);
        return Err(error.into());
    }
    let responder = match ChallengeResponder::start(listener, socket.clone(), challenge) {
        Ok(responder) => responder,
        Err(error) => {
            let _ = fs::remove_file(&socket);
            return Err(error);
        }
    };
    Ok((socket, responder, challenge))
}

#[cfg(unix)]
fn prepare_socket_directory(directory: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(directory)?;
    let mut current = PathBuf::new();
    for component in directory.components() {
        current.push(component);
        let metadata = fs::symlink_metadata(&current)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(format!(
                "detector proof directory component {} is not a normal directory",
                current.display()
            )
            .into());
        }
    }
    fs::set_permissions(directory, fs::Permissions::from_mode(0o700))?;
    prune_stale_sockets(directory)?;
    Ok(())
}

#[cfg(unix)]
fn prune_stale_sockets(directory: &Path) -> Result<(), Box<dyn Error>> {
    const STALE_AFTER: Duration = Duration::from_secs(24 * 60 * 60);
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if !entry.file_type()?.is_socket()
            || !crate::detector_proof::managed_socket_path(&entry.path())
        {
            continue;
        }
        let Ok(modified) = entry.metadata()?.modified() else {
            continue;
        };
        if SystemTime::now()
            .duration_since(modified)
            .is_ok_and(|age| age >= STALE_AFTER)
        {
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

#[cfg(unix)]
struct ChallengeResponder {
    cancel: Arc<AtomicBool>,
    handle: Option<JoinHandle<Result<bool, String>>>,
    socket: PathBuf,
}

#[cfg(unix)]
impl ChallengeResponder {
    fn start(
        listener: UnixListener,
        socket: PathBuf,
        challenge: [u8; crate::detector_proof::CHALLENGE_BYTES],
    ) -> Result<Self, Box<dyn Error>> {
        listener.set_nonblocking(true)?;
        let cancel = Arc::new(AtomicBool::new(false));
        let thread_cancel = Arc::clone(&cancel);
        let handle = std::thread::Builder::new()
            .name("rafter-detector-proof".to_owned())
            .spawn(move || loop {
                if thread_cancel.load(Ordering::Acquire) {
                    return Ok(false);
                }
                match listener.accept() {
                    Ok((stream, _)) => {
                        return answer_challenge(stream, &challenge, &thread_cancel);
                    }
                    Err(error)
                        if matches!(
                            error.kind(),
                            std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                        ) =>
                    {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    Err(error) => return Err(format!("accept detector proof channel: {error}")),
                }
            })?;
        Ok(Self {
            cancel,
            handle: Some(handle),
            socket,
        })
    }

    fn finish(mut self) -> Result<bool, Box<dyn Error>> {
        self.cancel.store(true, Ordering::Release);
        let result = self
            .handle
            .take()
            .ok_or("detector challenge responder was already joined")?
            .join()
            .map_err(|_| "detector challenge responder panicked")?
            .map_err(Into::into);
        self.remove_socket()?;
        result
    }

    fn remove_socket(&self) -> Result<(), Box<dyn Error>> {
        match fs::remove_file(&self.socket) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }
}

#[cfg(unix)]
impl Drop for ChallengeResponder {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        let _ = fs::remove_file(&self.socket);
    }
}

#[cfg(unix)]
fn answer_challenge(
    mut stream: UnixStream,
    challenge: &[u8; crate::detector_proof::CHALLENGE_BYTES],
    cancel: &AtomicBool,
) -> Result<bool, String> {
    stream
        .set_nonblocking(true)
        .map_err(|error| format!("configure detector proof channel: {error}"))?;
    loop {
        if cancel.load(Ordering::Acquire) {
            return Ok(false);
        }
        let mut request = [0_u8; 1];
        match stream.read_exact(&mut request) {
            Ok(()) if request[0] == crate::detector_proof::PROOF_REQUEST => {
                stream
                    .write_all(challenge)
                    .map_err(|error| format!("write detector challenge: {error}"))?;
                stream
                    .flush()
                    .map_err(|error| format!("flush detector challenge: {error}"))?;
                match stream.shutdown(Shutdown::Write) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotConnected => {}
                    Err(error) => return Err(format!("close detector challenge stream: {error}")),
                }
                return Ok(true);
            }
            Ok(()) => return Err("detector proof request is malformed".to_owned()),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(error) => return Err(format!("read detector proof request: {error}")),
        }
    }
}

#[cfg(all(test, unix))]
#[path = "detector_proof_tests.rs"]
mod tests;
