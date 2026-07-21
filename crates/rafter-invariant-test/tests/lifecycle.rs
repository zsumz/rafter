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
    unix_failures::assert_proof_descriptor_is_closed_before_fixture_body();
    #[cfg(unix)]
    unix_failures::assert_missing_proof_descriptor_is_rejected();
    #[cfg(unix)]
    unix_failures::assert_invalid_proof_descriptor_is_rejected();
    #[cfg(unix)]
    unix_failures::assert_noncanonical_proof_descriptor_is_closed();
    #[cfg(unix)]
    unix_failures::assert_proof_descriptor_without_token_is_closed();
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
        os::{fd::IntoRawFd, unix::ffi::OsStringExt, unix::net::UnixStream},
        process::{ExitCode, Termination},
        thread::JoinHandle,
    };

    use nix::{errno::Errno, unistd::close};

    const TOKEN_ENV: &str = "RAFTER_INVARIANT_ORACLE_TOKEN";
    const DESCRIPTOR_ENV: &str = "RAFTER_INVARIANT_DETECTOR_PROOF_FD";

    pub(super) fn assert_changed_token_is_rejected() {
        let _environment = EnvironmentGuard::new();
        let (parent, child) = UnixStream::pair().unwrap();
        EnvironmentGuard::bind("original", Some(child.into_raw_fd()));
        let responder = challenge_responder(parent, 32);

        rafter_invariant_test::__begin_detector_test();
        responder.join().unwrap();
        reject_once();
        std::env::set_var(TOKEN_ENV, "replacement");
        assert_eq!(
            rafter_invariant_test::__detector_test_outcome().report(),
            ExitCode::FAILURE
        );
    }

    pub(super) fn assert_proof_descriptor_is_closed_before_fixture_body() {
        let _environment = EnvironmentGuard::new();
        let (parent, child) = UnixStream::pair().unwrap();
        let descriptor = child.into_raw_fd();
        EnvironmentGuard::bind("token", Some(descriptor));
        let responder = challenge_responder(parent, 32);

        rafter_invariant_test::__begin_detector_test();
        responder.join().unwrap();
        assert_eq!(close(descriptor), Err(Errno::EBADF));
        reject_once();
        assert_eq!(
            rafter_invariant_test::__detector_test_outcome().report(),
            ExitCode::SUCCESS
        );
    }

    pub(super) fn assert_missing_proof_descriptor_is_rejected() {
        let _environment = EnvironmentGuard::new();
        EnvironmentGuard::bind("token", None);

        rafter_invariant_test::__begin_detector_test();
        reject_once();
        assert_eq!(
            rafter_invariant_test::__detector_test_outcome().report(),
            ExitCode::FAILURE
        );
    }

    pub(super) fn assert_invalid_proof_descriptor_is_rejected() {
        let _environment = EnvironmentGuard::new();
        EnvironmentGuard::bind("token", Some(i32::MAX));

        rafter_invariant_test::__begin_detector_test();
        reject_once();
        assert_eq!(
            rafter_invariant_test::__detector_test_outcome().report(),
            ExitCode::FAILURE
        );
    }

    pub(super) fn assert_noncanonical_proof_descriptor_is_closed() {
        let _environment = EnvironmentGuard::new();
        let (_parent, child) = UnixStream::pair().unwrap();
        let descriptor = child.into_raw_fd();
        std::env::set_var(TOKEN_ENV, "token");
        std::env::set_var(DESCRIPTOR_ENV, format!("0{descriptor}"));

        rafter_invariant_test::__begin_detector_test();
        assert_eq!(close(descriptor), Err(Errno::EBADF));
        reject_once();
        assert_eq!(
            rafter_invariant_test::__detector_test_outcome().report(),
            ExitCode::FAILURE
        );
    }

    pub(super) fn assert_proof_descriptor_without_token_is_closed() {
        let _environment = EnvironmentGuard::new();
        let (_parent, child) = UnixStream::pair().unwrap();
        let descriptor = child.into_raw_fd();
        std::env::remove_var(TOKEN_ENV);
        std::env::set_var(DESCRIPTOR_ENV, descriptor.to_string());

        rafter_invariant_test::__begin_detector_test();
        assert_eq!(close(descriptor), Err(Errno::EBADF));
        reject_once();
        assert_eq!(
            rafter_invariant_test::__detector_test_outcome().report(),
            ExitCode::FAILURE
        );
    }

    pub(super) fn assert_truncated_challenge_is_rejected() {
        let _environment = EnvironmentGuard::new();
        let (mut parent, child) = UnixStream::pair().unwrap();
        EnvironmentGuard::bind("token", Some(child.into_raw_fd()));

        let responder = std::thread::spawn(move || {
            let mut request = [0_u8; 1];
            parent.read_exact(&mut request).unwrap();
            assert_eq!(request, [0xa7]);
            parent.write_all(&[0_u8; 4]).unwrap();
        });
        rafter_invariant_test::__begin_detector_test();
        responder.join().unwrap();
        reject_once();
        assert_eq!(
            rafter_invariant_test::__detector_test_outcome().report(),
            ExitCode::FAILURE
        );
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

    fn challenge_responder(mut parent: UnixStream, challenge_bytes: usize) -> JoinHandle<()> {
        std::thread::spawn(move || {
            let mut request = [0_u8; 1];
            parent.read_exact(&mut request).unwrap();
            assert_eq!(request, [0xa7]);
            parent.write_all(&vec![0x5a; challenge_bytes]).unwrap();
        })
    }

    struct EnvironmentGuard {
        token: Option<OsString>,
        descriptor: Option<OsString>,
    }

    impl EnvironmentGuard {
        fn new() -> Self {
            Self {
                token: std::env::var_os(TOKEN_ENV),
                descriptor: std::env::var_os(DESCRIPTOR_ENV),
            }
        }

        fn bind(token: &str, descriptor: Option<i32>) {
            std::env::set_var(TOKEN_ENV, token);
            match descriptor {
                Some(descriptor) => std::env::set_var(DESCRIPTOR_ENV, descriptor.to_string()),
                None => std::env::remove_var(DESCRIPTOR_ENV),
            }
        }
    }

    impl Drop for EnvironmentGuard {
        fn drop(&mut self) {
            restore_environment(TOKEN_ENV, self.token.as_deref());
            restore_environment(DESCRIPTOR_ENV, self.descriptor.as_deref());
        }
    }

    fn restore_environment(name: &str, value: Option<&OsStr>) {
        if let Some(value) = value {
            std::env::set_var(name, value);
        } else {
            std::env::remove_var(name);
        }
    }
}
