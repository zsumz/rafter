//! Bounded one-shot responder lifecycle for detector challenge requests.

use std::{
    fs,
    io::{Read, Write},
    net::Shutdown,
    os::unix::net::{UnixListener, UnixStream},
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::JoinHandle,
    time::Duration,
};

use super::{wire, ChallengeExchange, DetectorChallenge, TransportError};

pub(super) struct ChallengeResponder {
    cancel: Arc<AtomicBool>,
    handle: Option<JoinHandle<ChallengeExchange>>,
    socket: std::path::PathBuf,
}

impl ChallengeResponder {
    pub(super) fn start(
        listener: UnixListener,
        socket: std::path::PathBuf,
        challenge: DetectorChallenge,
    ) -> Result<Self, TransportError> {
        listener
            .set_nonblocking(true)
            .map_err(|error| TransportError::context("configure detector proof listener", error))?;
        let cancel = Arc::new(AtomicBool::new(false));
        let thread_cancel = Arc::clone(&cancel);
        let handle = std::thread::Builder::new()
            .name("rafter-detector-proof".to_owned())
            .spawn(move || accept_request(&listener, &challenge, &thread_cancel))
            .map_err(|error| {
                TransportError::context("start detector challenge responder", error)
            })?;
        Ok(Self {
            cancel,
            handle: Some(handle),
            socket,
        })
    }

    pub(super) fn finish(mut self) -> ChallengeExchange {
        self.cancel.store(true, Ordering::Release);
        let exchange = match self.handle.take() {
            Some(handle) => match handle.join() {
                Ok(exchange) => exchange,
                Err(_) => ChallengeExchange::TransportError(TransportError::new(
                    "detector challenge responder panicked",
                )),
            },
            None => ChallengeExchange::TransportError(TransportError::new(
                "detector challenge responder was already joined",
            )),
        };
        match remove_socket(&self.socket) {
            Ok(()) => exchange,
            Err(error) => ChallengeExchange::TransportError(error),
        }
    }
}

impl Drop for ChallengeResponder {
    fn drop(&mut self) {
        self.cancel.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        let _ = fs::remove_file(&self.socket);
    }
}

fn accept_request(
    listener: &UnixListener,
    challenge: &DetectorChallenge,
    cancel: &AtomicBool,
) -> ChallengeExchange {
    loop {
        if cancel.load(Ordering::Acquire) {
            return ChallengeExchange::Disconnected;
        }
        match listener.accept() {
            Ok((stream, _)) => return answer_challenge(stream, challenge, cancel),
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) => {
                return ChallengeExchange::TransportError(TransportError::context(
                    "accept detector proof channel",
                    error,
                ));
            }
        }
    }
}

fn answer_challenge(
    mut stream: UnixStream,
    challenge: &DetectorChallenge,
    cancel: &AtomicBool,
) -> ChallengeExchange {
    if let Err(error) = stream.set_nonblocking(true) {
        return ChallengeExchange::TransportError(TransportError::context(
            "configure detector proof channel",
            error,
        ));
    }
    loop {
        if cancel.load(Ordering::Acquire) {
            return ChallengeExchange::Disconnected;
        }
        let mut request = [0_u8; 1];
        match stream.read_exact(&mut request) {
            Ok(()) if request[0] == wire::PROOF_REQUEST => {
                return release_challenge(&mut stream, challenge);
            }
            Ok(()) => return ChallengeExchange::MalformedRequest,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                return ChallengeExchange::Disconnected;
            }
            Err(error) => {
                return ChallengeExchange::TransportError(TransportError::context(
                    "read detector proof request",
                    error,
                ));
            }
        }
    }
}

fn release_challenge(stream: &mut UnixStream, challenge: &DetectorChallenge) -> ChallengeExchange {
    if let Err(error) = stream.write_all(challenge.as_bytes()) {
        return ChallengeExchange::TransportError(TransportError::context(
            "write detector challenge",
            error,
        ));
    }
    if let Err(error) = stream.flush() {
        return ChallengeExchange::TransportError(TransportError::context(
            "flush detector challenge",
            error,
        ));
    }
    match stream.shutdown(Shutdown::Write) {
        Ok(()) => ChallengeExchange::Completed,
        Err(error) if error.kind() == std::io::ErrorKind::NotConnected => {
            ChallengeExchange::Completed
        }
        Err(error) => ChallengeExchange::TransportError(TransportError::context(
            "close detector challenge stream",
            error,
        )),
    }
}

fn remove_socket(socket: &Path) -> Result<(), TransportError> {
    match fs::remove_file(socket) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(TransportError::context(
            "remove detector proof socket",
            error,
        )),
    }
}

#[cfg(test)]
mod tests;
