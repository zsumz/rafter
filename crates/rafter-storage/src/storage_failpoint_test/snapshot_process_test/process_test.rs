//! Owned child termination, pre-recovery inputs, and retained failure artifacts.

use std::{
    fmt::Write as _,
    fs::{self, File},
    os::unix::process::ExitStatusExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread,
    time::{Duration, Instant},
};

use crate::{checksum::crc32, storage_failpoint_test::DurabilityPoint};

pub(super) struct Workspace {
    root: PathBuf,
    retain: bool,
}

impl Workspace {
    pub(super) fn new() -> Self {
        let requested = std::env::var_os("RAFTER_SNAPSHOT_PROCESS_ARTIFACTS");
        let retain = requested.is_some();
        let root = requested.map_or_else(
            || std::env::temp_dir().join(format!("rafter-snapshot-process-{}", std::process::id())),
            PathBuf::from,
        );
        fs::create_dir(&root).expect("snapshot process artifacts directory must be new");
        println!("snapshot process artifacts: {}", root.display());
        Self { root, retain }
    }

    pub(super) fn path(&self) -> &Path {
        &self.root
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        if self.retain || thread::panicking() {
            eprintln!(
                "retained snapshot process artifacts: {}",
                self.root.display()
            );
        } else {
            fs::remove_dir_all(&self.root).expect("successful process workspace cleans up");
        }
    }
}

struct OwnedChild(Child);

impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < deadline {
                if self.0.try_wait().ok().flatten().is_some() {
                    return;
                }
                thread::sleep(Duration::from_millis(10));
            }
            eprintln!("failed to reap owned snapshot child {}", self.0.id());
        }
    }
}

pub(super) fn kill_at_boundary(
    root: &Path,
    test: &str,
    child_env: &str,
    point: DurabilityPoint,
) -> u32 {
    let mut child = OwnedChild(
        Command::new(std::env::current_exe().expect("current test binary"))
            .args([
                "--ignored",
                "--exact",
                test,
                "--nocapture",
                "--test-threads=1",
            ])
            .env(child_env, root)
            .stdin(Stdio::null())
            .stdout(File::create(root.join("child.stdout")).expect("child stdout creates"))
            .stderr(File::create(root.join("child.stderr")).expect("child stderr creates"))
            .spawn()
            .expect("snapshot writer child starts"),
    );
    let deadline = Instant::now() + Duration::from_secs(30);
    let expected = format!("{point:?}\n");
    loop {
        assert!(
            child.0.try_wait().expect("poll child").is_none(),
            "child exited before boundary"
        );
        if fs::read_to_string(root.join("boundary.txt"))
            .ok()
            .as_deref()
            == Some(&expected)
        {
            break;
        }
        assert!(Instant::now() < deadline, "child did not reach {point:?}");
        thread::sleep(Duration::from_millis(10));
    }
    let pid = child.0.id();
    child.0.kill().expect("SIGKILL reaches the owned child");
    let status = wait(&mut child.0);
    assert_eq!(
        status.signal(),
        Some(9),
        "writer must exit from SIGKILL: {status}"
    );
    pid
}

fn wait(child: &mut Child) -> ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().expect("poll killed child") {
            return status;
        }
        assert!(Instant::now() < deadline, "killed child did not exit");
        thread::sleep(Duration::from_millis(10));
    }
}

pub(super) fn preserve_inputs(source: &Path, destination: &Path) {
    fs::create_dir(destination).expect("pre-recovery directory must be new");
    let files = regular_files(source);
    assert!(!files.is_empty(), "crash input inventory is empty");
    let mut manifest = String::new();
    for path in &files {
        let name = path.file_name().expect("input filename");
        let bytes = fs::read(path).expect("crash input reads before recovery");
        let copy = destination.join(name);
        fs::write(&copy, &bytes).expect("crash input copy writes");
        assert_eq!(fs::read(&copy).expect("copy reads"), bytes);
        assert_eq!(fs::read(path).expect("source rereads"), bytes);
        writeln!(
            manifest,
            "{}\t{}\t{:08x}",
            name.to_string_lossy(),
            bytes.len(),
            crc32(&bytes)
        )
        .expect("input manifest formats");
    }
    assert_eq!(
        regular_files(source),
        files,
        "crash input inventory changed"
    );
    fs::write(destination.join("inventory.tsv"), manifest).expect("input manifest writes");
}

fn regular_files(root: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(root).expect("crash inputs list") {
        let entry = entry.expect("crash input entry");
        assert!(
            entry.file_type().expect("input type").is_file(),
            "non-regular crash input"
        );
        paths.push(entry.path());
    }
    paths.sort();
    paths
}
