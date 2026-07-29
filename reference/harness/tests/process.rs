use std::{
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    process::{Command, Stdio},
    thread,
    time::Duration,
};

use rafter_reference_harness::process::{
    ChildProcess, ConnectionTimeouts, LineConnection, ReconnectingClient, RequestError,
    ScratchSpace, Wait,
};

const QUICK_WAIT: Wait = Wait::new(Duration::from_secs(2), Duration::from_millis(5));
const CONNECTION_TIMEOUTS: ConnectionTimeouts = ConnectionTimeouts::new(
    Duration::from_secs(1),
    Duration::from_secs(1),
    Duration::from_secs(1),
);

#[test]
fn lifecycle_lines_remain_searchable_after_being_observed() {
    let mut command = Command::new("/bin/sh");
    command.arg("-c").arg("printf 'first\\nsecond\\n'");
    let mut child = ChildProcess::spawn("line-retention", &mut command).expect("child spawns");

    assert_eq!(
        child
            .wait_for_stdout_prefix("first", QUICK_WAIT)
            .expect("first line arrives"),
        "first"
    );
    assert_eq!(
        child
            .wait_for_stdout_prefix("second", QUICK_WAIT)
            .expect("second line arrives"),
        "second"
    );
    assert!(child.has_stdout_prefix("first"));
    child.wait_for_exit(QUICK_WAIT).expect("child exits");
}

#[test]
fn child_failure_reports_identity_condition_exit_and_output() {
    let mut command = Command::new("/bin/sh");
    command
        .arg("-c")
        .arg("printf 'stdout evidence\\n'; printf 'stderr evidence\\n' >&2; exit 7");
    let mut child = ChildProcess::spawn("diagnostic-child", &mut command).expect("child spawns");

    let error = child
        .wait_for_stdout_prefix("missing", QUICK_WAIT)
        .expect_err("the requested line never arrives");
    assert_eq!(error.identity(), "diagnostic-child");
    assert!(error.condition().contains("missing"));
    assert!(error.status().is_some_and(|status| !status.success()));
    assert_eq!(error.stdout(), &["stdout evidence"]);
    assert_eq!(error.stderr(), &["stderr evidence"]);
    assert!(error.to_string().contains("2s"));
}

#[test]
fn timeout_names_the_awaited_condition() {
    let error = Wait::new(Duration::from_millis(20), Duration::from_millis(2))
        .until::<()>("a named condition", || None)
        .expect_err("the condition never becomes true");
    assert_eq!(error.condition(), "a named condition");
    assert!(error.to_string().contains("a named condition"));
}

#[test]
fn clean_exit_is_reaped() {
    let mut command = Command::new("/bin/sh");
    command.arg("-c").arg("exit 0");
    let mut child = ChildProcess::spawn("clean-exit", &mut command).expect("child spawns");
    let id = child.id();

    assert!(child
        .wait_for_exit(QUICK_WAIT)
        .expect("child exits")
        .success());
    assert!(!process_exists(id));
}

#[test]
fn forceful_stop_is_reaped() {
    let mut command = Command::new("/bin/sh");
    command.arg("-c").arg("while :; do :; done");
    let mut child = ChildProcess::spawn("forceful-stop", &mut command).expect("child spawns");
    let id = child.id();

    child.kill_and_reap().expect("child is stopped and reaped");
    assert!(!process_exists(id));
}

#[test]
fn dropping_a_child_leaves_no_process() {
    let id = {
        let mut command = Command::new("/bin/sh");
        command.arg("-c").arg("while :; do :; done");
        let child = ChildProcess::spawn("drop-stop", &mut command).expect("child spawns");
        child.id()
    };

    assert!(!process_exists(id));
}

#[test]
fn connection_loss_is_returned_as_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener binds");
    let addr = listener.local_addr().expect("listener has an address");
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().expect("connection arrives");
        drop(stream);
    });

    let mut connection =
        LineConnection::connect(addr, CONNECTION_TIMEOUTS).expect("connection opens");
    let error = connection
        .receive_line()
        .expect_err("closed connection returns an error");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    server.join().expect("server thread exits");
}

#[test]
fn a_failed_exchange_reconnects_once() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener binds");
    let addr = listener.local_addr().expect("listener has an address");
    let server = thread::spawn(move || {
        let (first, _) = listener.accept().expect("first connection arrives");
        let mut first = BufReader::new(first);
        let mut line = String::new();
        first.read_line(&mut line).expect("first request arrives");
        first
            .get_mut()
            .write_all(b"one\n")
            .expect("first response is written");
        first.get_mut().flush().expect("first response is flushed");
        drop(first);

        let (second, _) = listener.accept().expect("replacement connection arrives");
        let mut second = BufReader::new(second);
        line.clear();
        second
            .read_line(&mut line)
            .expect("retried request arrives");
        second
            .get_mut()
            .write_all(b"two\n")
            .expect("second response is written");
        second
            .get_mut()
            .flush()
            .expect("second response is flushed");
    });

    let mut client = ReconnectingClient::new(addr, CONNECTION_TIMEOUTS);
    assert_eq!(
        client.request("first").expect("first exchange works"),
        "one"
    );
    assert_eq!(
        client.request("second").expect("one reconnect works"),
        "two"
    );
    server.join().expect("server thread exits");
}

#[test]
fn an_initial_connection_failure_is_not_a_retry() {
    let listener = TcpListener::bind("127.0.0.1:0").expect("listener binds");
    let addr = listener.local_addr().expect("listener has an address");
    drop(listener);

    let mut client = ReconnectingClient::new(addr, CONNECTION_TIMEOUTS);
    assert!(matches!(
        client.request("unused"),
        Err(RequestError::Connect(_))
    ));
}

#[test]
fn scratch_outlives_a_child_that_uses_it() {
    let scratch = ScratchSpace::create("neutral-process-test", "lifetime")
        .expect("scratch directory is created");
    let path = scratch.path().to_path_buf();
    let mut command = Command::new("/bin/sh");
    command
        .current_dir(&path)
        .arg("-c")
        .arg("while :; do :; done");
    let child =
        ChildProcess::spawn_in("scratch-user", &mut command, &scratch).expect("child spawns");

    drop(scratch);
    assert!(path.is_dir(), "the child keeps its scratch directory alive");
    drop(child);
    assert!(
        !path.exists(),
        "the directory is removed after its final child is gone"
    );
}

fn process_exists(id: u32) -> bool {
    Command::new("/bin/kill")
        .arg("-0")
        .arg(id.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}
