//! Fail-closed parsing and binding of reviewed script interpreters.

use std::{collections::BTreeMap, error::Error, path::Path};

use super::{BoundExecutable, BASH_RUNTIME};

pub(in crate::producer::process) struct BoundInterpreter {
    runtime: String,
    executable: BoundExecutable,
    arguments: Vec<String>,
}

impl BoundInterpreter {
    pub(in crate::producer::process) fn bind_for_script(
        script: &mut BoundExecutable,
        environment: &BTreeMap<String, String>,
    ) -> Result<Option<Self>, Box<dyn Error>> {
        let Some(line) = script.shebang()? else {
            return Ok(None);
        };
        let fields = line.split_whitespace().collect::<Vec<_>>();
        let (runtime, program, arguments) = match fields.as_slice() {
            ["/usr/bin/env", "bash"] => (BASH_RUNTIME, "bash", Vec::new()),
            ["/usr/bin/env", option, ..] if option.starts_with('-') => {
                return Err(format!("unsupported script shebang option in {line:?}").into());
            }
            ["/usr/bin/env", program, ..] => {
                return Err(format!("unregistered script interpreter {program:?}").into());
            }
            [program, arguments @ ..] if Path::new(program).is_absolute() => {
                let runtime = Path::new(program)
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or("script interpreter name is not UTF-8")?;
                if runtime != BASH_RUNTIME {
                    return Err(format!("unregistered script interpreter {program:?}").into());
                }
                (BASH_RUNTIME, *program, arguments.to_vec())
            }
            _ => return Err(format!("invalid script shebang {line:?}").into()),
        };
        let executable = BoundExecutable::bind_program(program, environment)?;
        if Path::new(program).is_absolute() {
            let source_bound = BoundExecutable::bind_program(BASH_RUNTIME, environment)?;
            if executable.receipt() != source_bound.receipt() {
                return Err(format!(
                    "absolute Bash interpreter {program:?} is not the source-bound PATH runtime"
                )
                .into());
            }
        }
        Ok(Some(Self {
            runtime: runtime.to_owned(),
            executable,
            arguments: arguments.into_iter().map(str::to_owned).collect(),
        }))
    }

    pub(in crate::producer::process) fn runtime(&self) -> &str {
        &self.runtime
    }

    pub(in crate::producer::process) fn executable(&self) -> &BoundExecutable {
        &self.executable
    }

    pub(in crate::producer::process) fn arguments(&self) -> &[String] {
        &self.arguments
    }
}
