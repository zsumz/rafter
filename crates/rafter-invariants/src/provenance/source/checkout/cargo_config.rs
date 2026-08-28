//! Cargo configuration discovery and content hashing for a bound checkout.
//!
//! Owns the release boundary that decides whether Cargo loads configuration
//! includes, the ancestor and Cargo-home precedence order, and the framed
//! digest over every configuration file that participates in a build.

use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

mod includes;
mod policy;

use includes::{cargo_config_in, cargo_config_includes};
use policy::validate_bound_cargo_config;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct CargoRelease {
    major: u64,
    minor: u64,
}

impl CargoRelease {
    // Cargo releases before 1.94 hash the active config but do not load its
    // `include` targets.
    const CONFIG_INCLUDE: Self = Self {
        major: 1,
        minor: 94,
    };

    pub(super) const fn new(major: u64, minor: u64) -> Self {
        Self { major, minor }
    }

    pub(super) fn from_verbose_identity(identity: &str) -> Result<Self, Box<dyn Error>> {
        let mut releases = identity
            .lines()
            .filter_map(|line| line.strip_prefix("release: "));
        let release = releases.next().ok_or("cargo -vV omitted its release")?;
        if releases.next().is_some() {
            return Err("cargo -vV reported more than one release".into());
        }
        let mut components = release.split('.');
        let major = components
            .next()
            .ok_or("cargo -vV release omitted its major version")?
            .parse()?;
        let minor = components
            .next()
            .ok_or("cargo -vV release omitted its minor version")?
            .parse()?;
        Ok(Self::new(major, minor))
    }

    pub(super) fn follows_config_includes(self) -> bool {
        self >= Self::CONFIG_INCLUDE
    }
}

pub(super) fn cargo_config_sha256(
    root: &Path,
    cargo_identity: &str,
) -> Result<String, Box<dyn Error>> {
    let cargo_home = env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")));
    cargo_config_sha256_with_home(
        root,
        cargo_home.as_deref(),
        CargoRelease::from_verbose_identity(cargo_identity)?,
    )
}

pub(super) fn cargo_config_sha256_with_home(
    root: &Path,
    cargo_home: Option<&Path>,
    cargo_release: CargoRelease,
) -> Result<String, Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let cargo_home_config = if let Some(home) = cargo_home {
        let home = if home.is_absolute() {
            home.to_owned()
        } else {
            root.join(home)
        };
        cargo_config_in(&home)?
            .map(|path| fs::canonicalize(&path).map(|canonical| (path, canonical)))
            .transpose()?
    } else {
        None
    };
    let mut ancestor_configs = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for ancestor in root.ancestors() {
        if let Some(path) = cargo_config_in(&ancestor.join(".cargo"))? {
            let canonical = fs::canonicalize(&path)?;
            if cargo_home_config
                .as_ref()
                .is_some_and(|(_, home)| home == &canonical)
            {
                continue;
            }
            if seen.insert(canonical) {
                ancestor_configs.push(path);
            }
        }
    }
    let mut paths = ancestor_configs
        .into_iter()
        .enumerate()
        .map(|(precedence, path)| {
            Ok((
                format!(
                    "ancestor:{precedence}:{}",
                    path.file_name()
                        .and_then(std::ffi::OsStr::to_str)
                        .ok_or("Cargo configuration filename is not valid UTF-8")?
                ),
                path,
            ))
        })
        .collect::<Result<Vec<_>, Box<dyn Error>>>()?;
    if let Some((path, canonical)) = cargo_home_config {
        if seen.insert(canonical) {
            paths.push((
                format!(
                    "cargo-home:{}",
                    path.file_name()
                        .and_then(std::ffi::OsStr::to_str)
                        .ok_or("Cargo configuration filename is not valid UTF-8")?
                ),
                path,
            ));
        }
    }

    let mut hasher = Sha256::new();
    for (identity, path) in paths {
        hash_cargo_config_tree(
            &mut hasher,
            &identity,
            &path,
            cargo_release,
            &mut std::collections::BTreeSet::new(),
        )?;
    }
    Ok(format!("{:x}", hasher.finalize()))
}

#[derive(Debug)]
struct CargoConfigInclude {
    path: PathBuf,
    optional: bool,
}

fn hash_cargo_config_tree(
    hasher: &mut Sha256,
    identity: &str,
    path: &Path,
    cargo_release: CargoRelease,
    active: &mut std::collections::BTreeSet<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let canonical = fs::canonicalize(path)?;
    if !active.insert(canonical.clone()) {
        return Err(format!(
            "Cargo configuration include cycle reaches {}",
            path.display()
        )
        .into());
    }
    let contents = fs::read(path)?;
    let parsed = std::str::from_utf8(&contents)?
        .parse::<toml::Value>()
        .map_err(|error| format!("parse Cargo configuration {}: {error}", path.display()))?;
    validate_bound_cargo_config(&parsed, path)?;
    if cargo_release.follows_config_includes() {
        for (position, include) in cargo_config_includes(&parsed)?.into_iter().enumerate() {
            let include_identity =
                format!("{identity}:include:{position}:{}", include.path.display());
            let include_path = if include.path.is_absolute() {
                include.path
            } else {
                path.parent()
                    .ok_or_else(|| {
                        format!(
                            "Cargo configuration has no parent directory: {}",
                            path.display()
                        )
                    })?
                    .join(include.path)
            };
            match fs::metadata(&include_path) {
                Ok(metadata) if metadata.is_file() => {
                    hash_cargo_config_tree(
                        hasher,
                        &include_identity,
                        &include_path,
                        cargo_release,
                        active,
                    )?;
                }
                Ok(_) => {
                    return Err(format!(
                        "Cargo configuration include is not a file: {}",
                        include_path.display()
                    )
                    .into());
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound && include.optional => {
                    hasher.update(include_identity.as_bytes());
                    hasher.update(b"\0optional-missing\0");
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Err(format!(
                        "required Cargo configuration include is missing: {}",
                        include_path.display()
                    )
                    .into());
                }
                Err(error) => return Err(error.into()),
            }
        }
    }
    hasher.update(identity.as_bytes());
    hasher.update([0]);
    hasher.update(contents);
    hasher.update([0]);
    active.remove(&canonical);
    Ok(())
}
