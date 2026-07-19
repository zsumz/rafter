//! Detector-session fail-closed lifecycle stories.

use std::process::{ExitCode, Termination};

#[test]
fn detector_sessions_fail_closed_when_lifecycle_proof_is_incomplete() {
    rafter_invariant_test::__begin_detector_test();
    rafter_invariant_test::__oracle_observed();
    let outcome = rafter_invariant_test::__detector_test_outcome();
    assert_eq!(outcome.report(), ExitCode::FAILURE);

    #[cfg(unix)]
    unix_failures::assert_changed_token_is_rejected();
    #[cfg(unix)]
    unix_failures::assert_unreachable_proof_socket_is_rejected();
    #[cfg(unix)]
    unix_failures::assert_truncated_challenge_is_rejected();
    #[cfg(unix)]
    unix_failures::assert_non_unicode_token_is_rejected();
}

#[cfg(unix)]
mod unix_failures {
    use std::{
        ffi::{OsStr, OsString},
        io::{Read, Write},
        os::{unix::ffi::OsStringExt, unix::net::UnixListener},
        path::{Path, PathBuf},
        process::{ExitCode, Termination},
        sync::atomic::{AtomicU64, Ordering},
    };

    const TOKEN_ENV: &str = "RAFTER_INVARIANT_ORACLE_TOKEN";
    const SOCKET_ENV: &str = "RAFTER_INVARIANT_DETECTOR_PROOF_SOCKET";
    static NEXT_SOCKET: AtomicU64 = AtomicU64::new(0);

    pub(super) fn assert_changed_token_is_rejected() {
        let _environment = EnvironmentGuard::new();
        let socket = SocketFixture::bind();
        EnvironmentGuard::bind("original", socket.path());

        rafter_invariant_test::__begin_detector_test();
        reject_once();
        std::env::set_var(TOKEN_ENV, "replacement");
        assert_eq!(
            rafter_invariant_test::__detector_test_outcome().report(),
            ExitCode::FAILURE
        );
    }

    pub(super) fn assert_unreachable_proof_socket_is_rejected() {
        let _environment = EnvironmentGuard::new();
        let missing = socket_path();
        EnvironmentGuard::bind("token", &missing);

        rafter_invariant_test::__begin_detector_test();
        reject_once();
        assert_eq!(
            rafter_invariant_test::__detector_test_outcome().report(),
            ExitCode::FAILURE
        );
    }

    pub(super) fn assert_truncated_challenge_is_rejected() {
        let _environment = EnvironmentGuard::new();
        let socket = SocketFixture::bind();
        EnvironmentGuard::bind("token", socket.path());

        rafter_invariant_test::__begin_detector_test();
        let responder = std::thread::spawn(move || {
            let (mut stream, _) = socket.listener.accept().unwrap();
            let mut request = [0_u8; 1];
            stream.read_exact(&mut request).unwrap();
            assert_eq!(request, [0xa7]);
            stream.write_all(&[0_u8; 4]).unwrap();
        });
        reject_once();
        assert_eq!(
            rafter_invariant_test::__detector_test_outcome().report(),
            ExitCode::FAILURE
        );
        responder.join().unwrap();
    }

    pub(super) fn assert_non_unicode_token_is_rejected() {
        let _environment = EnvironmentGuard::new();
        std::env::set_var(TOKEN_ENV, OsString::from_vec(vec![0xff]));

        rafter_invariant_test::__begin_detector_test();
        reject_once();
        assert_eq!(
            rafter_invariant_test::__detector_test_outcome().report(),
            ExitCode::FAILURE
        );
    }

    fn reject_once() {
        fn reject() -> Result<(), ()> {
            Err(())
        }
        rafter_invariant_test::oracle_expect_err!(reject(), "must reject");
    }

    struct EnvironmentGuard {
        token: Option<OsString>,
        socket: Option<OsString>,
    }

    impl EnvironmentGuard {
        fn new() -> Self {
            Self {
                token: std::env::var_os(TOKEN_ENV),
                socket: std::env::var_os(SOCKET_ENV),
            }
        }

        fn bind(token: &str, socket: &Path) {
            std::env::set_var(TOKEN_ENV, token);
            std::env::set_var(SOCKET_ENV, socket);
        }
    }

    impl Drop for EnvironmentGuard {
        fn drop(&mut self) {
            restore_environment(TOKEN_ENV, self.token.as_deref());
            restore_environment(SOCKET_ENV, self.socket.as_deref());
        }
    }

    fn restore_environment(name: &str, value: Option<&OsStr>) {
        if let Some(value) = value {
            std::env::set_var(name, value);
        } else {
            std::env::remove_var(name);
        }
    }

    struct SocketFixture {
        listener: UnixListener,
        path: PathBuf,
    }

    impl SocketFixture {
        fn bind() -> Self {
            let path = socket_path();
            let _ = std::fs::remove_file(&path);
            let listener = UnixListener::bind(&path).unwrap();
            Self { listener, path }
        }

        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for SocketFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }

    fn socket_path() -> PathBuf {
        let sequence = NEXT_SOCKET.fetch_add(1, Ordering::Relaxed);
        Path::new("/tmp").join(format!("rit-{}-{sequence}.s", std::process::id()))
    }
}
