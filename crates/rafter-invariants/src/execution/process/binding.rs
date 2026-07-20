//! Descriptor-bound executable inventory for process launch and observation.

use std::{
    collections::BTreeMap,
    env,
    error::Error,
    fs,
    io::{Read, Seek},
    path::{Path, PathBuf},
};

#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(unix)]
use std::os::fd::{AsFd, BorrowedFd, OwnedFd};

use sha2::{Digest, Sha256};

use crate::execution::filesystem::{self as execution_fs, HeldFile};

mod interpreter;

pub(super) use interpreter::BoundInterpreter;

pub(crate) const BASH_RUNTIME: &str = "bash";
pub(super) const PERL_RUNTIME: &str = "perl";
pub(super) const PS_RUNTIME: &str = "ps";
pub(super) const TIME_RUNTIME: &str = "time";

const PERL_PATH: &str = "/usr/bin/perl";
const TIME_PATH: &str = "/usr/bin/time";
#[cfg(target_os = "macos")]
const PS_PATH: &str = "/bin/ps";
#[cfg(not(target_os = "macos"))]
const PS_PATH: &str = "/usr/bin/ps";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExecutableIdentity {
    pub(crate) path: PathBuf,
    pub(crate) sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct LauncherIdentity {
    pub(crate) role: String,
    pub(crate) runtime: String,
    pub(crate) executable: ExecutableIdentity,
}

pub(super) struct BoundProcessRuntime {
    perl: BoundExecutable,
    time: BoundExecutable,
    ps: BoundExecutable,
}

impl BoundProcessRuntime {
    pub(super) fn bind() -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            perl: BoundExecutable::bind_path(Path::new(PERL_PATH), false)?,
            time: BoundExecutable::bind_path(Path::new(TIME_PATH), false)?,
            ps: BoundExecutable::bind_path(Path::new(PS_PATH), false)?,
        })
    }

    pub(crate) fn identities(&self) -> BTreeMap<String, ExecutableIdentity> {
        [
            (PERL_RUNTIME, self.perl.identity()),
            (TIME_RUNTIME, self.time.identity()),
            (PS_RUNTIME, self.ps.identity()),
        ]
        .into_iter()
        .map(|(name, receipt)| (name.to_owned(), receipt))
        .collect()
    }

    pub(crate) fn launcher_identities(
        &self,
        interpreter: Option<&BoundInterpreter>,
    ) -> Vec<LauncherIdentity> {
        let mut launchers = vec![
            launcher("resource-wrapper", PERL_RUNTIME, &self.perl),
            launcher("resource-sampler", TIME_RUNTIME, &self.time),
            launcher("target-group-launcher", PERL_RUNTIME, &self.perl),
            launcher("process-observer", PS_RUNTIME, &self.ps),
        ];
        if let Some(interpreter) = interpreter {
            launchers.push(launcher(
                "target-interpreter",
                interpreter.runtime(),
                interpreter.executable(),
            ));
        }
        launchers
    }

    pub(super) fn verify_path_bindings(&self) -> Result<(), Box<dyn Error>> {
        self.perl.verify_path_binding()?;
        self.time.verify_path_binding()?;
        self.ps.verify_path_binding()
    }

    pub(super) fn perl(&self) -> &BoundExecutable {
        &self.perl
    }

    pub(super) fn time(&self) -> &BoundExecutable {
        &self.time
    }

    pub(super) fn ps(&self) -> &BoundExecutable {
        &self.ps
    }
}

pub(super) struct BoundExecutable {
    file: fs::File,
    #[cfg(unix)]
    descriptor: OwnedFd,
    execution_path: PathBuf,
    external_path: PathBuf,
    sha256: String,
    held: Option<HeldFile>,
}

impl BoundExecutable {
    pub(super) fn bind_program(
        program: &str,
        environment: &BTreeMap<String, String>,
    ) -> Result<Self, Box<dyn Error>> {
        let path = if Path::new(program).components().count() > 1 {
            PathBuf::from(program)
        } else {
            environment
                .get("PATH")
                .and_then(|path| {
                    env::split_paths(path)
                        .map(|directory| directory.join(program))
                        .find(|candidate| candidate.is_file())
                })
                .ok_or_else(|| format!("subprocess program is not present on PATH: {program}"))?
        };
        let workspace = !path.is_absolute()
            || path.starts_with(
                std::env::current_dir()
                    .map_err(|error| format!("resolve workspace executable: {error}"))?,
            );
        Self::bind_path(&path, workspace)
    }

    fn bind_path(path: &Path, workspace: bool) -> Result<Self, Box<dyn Error>> {
        let (file, held, external_path) = if workspace {
            let held = execution_fs::hold_file(path)?;
            let file = held.try_clone_std()?;
            let external_path = held.external_path();
            (file, Some(held), external_path)
        } else {
            let path = fs::canonicalize(path)?;
            (fs::File::open(&path)?, None, path)
        };
        if !file.metadata()?.is_file() {
            return Err(format!(
                "subprocess program is not a regular file: {}",
                path.display()
            )
            .into());
        }
        let sha256 = sha256_file(&file)?;
        #[cfg(unix)]
        let descriptor = rustix::io::fcntl_dupfd_cloexec(&file, 3)?;
        #[cfg(target_os = "linux")]
        let execution_path = PathBuf::from(format!("/proc/self/fd/{}", descriptor.as_raw_fd()));
        #[cfg(not(target_os = "linux"))]
        let execution_path = external_path.clone();
        Ok(Self {
            file,
            #[cfg(unix)]
            descriptor,
            execution_path,
            external_path,
            sha256,
            held,
        })
    }

    pub(crate) fn identity(&self) -> ExecutableIdentity {
        ExecutableIdentity {
            path: self.external_path.clone(),
            sha256: self.sha256.clone(),
        }
    }

    pub(super) fn execution_path(&self) -> &Path {
        &self.execution_path
    }

    pub(super) fn logical_program(&self) -> &Path {
        &self.external_path
    }

    #[cfg(unix)]
    pub(super) fn descriptor(&self) -> BorrowedFd<'_> {
        self.descriptor.as_fd()
    }

    pub(super) fn verify_path_binding(&self) -> Result<(), Box<dyn Error>> {
        if let Some(held) = &self.held {
            held.verify_path_binding()?;
        }
        let observed = sha256_file(&self.file)?;
        if observed != self.sha256 {
            return Err(format!(
                "bound executable content changed in place: {}",
                self.external_path.display()
            )
            .into());
        }
        Ok(())
    }

    fn shebang(&mut self) -> Result<Option<String>, Box<dyn Error>> {
        let mut file = self.file.try_clone()?;
        file.rewind()?;
        let mut bytes = [0_u8; 512];
        let read = file.read(&mut bytes)?;
        if !bytes[..read].starts_with(b"#!") {
            return Ok(None);
        }
        let line = bytes[2..read]
            .split(|byte| *byte == b'\n')
            .next()
            .ok_or("script shebang is missing its first line")?;
        let line = std::str::from_utf8(line)?.trim_end_matches('\r').trim();
        if line.is_empty() {
            return Err("script shebang has no interpreter".into());
        }
        Ok(Some(line.to_owned()))
    }
}

pub(crate) fn capture_runtime_identities(
    environment: &BTreeMap<String, String>,
    include_bash: bool,
) -> Result<BTreeMap<String, ExecutableIdentity>, Box<dyn Error>> {
    let mut identities = BoundProcessRuntime::bind()?.identities();
    if include_bash {
        identities.insert(
            BASH_RUNTIME.to_owned(),
            BoundExecutable::bind_program(BASH_RUNTIME, environment)?.identity(),
        );
    }
    Ok(identities)
}

fn launcher(role: &str, runtime: &str, executable: &BoundExecutable) -> LauncherIdentity {
    LauncherIdentity {
        role: role.to_owned(),
        runtime: runtime.to_owned(),
        executable: executable.identity(),
    }
}

fn sha256_file(file: &fs::File) -> Result<String, Box<dyn Error>> {
    let mut file = file.try_clone()?;
    file.rewind()?;
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    Ok(format!("{:x}", digest.finalize()))
}
