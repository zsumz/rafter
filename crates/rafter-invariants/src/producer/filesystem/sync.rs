use std::{error::Error, ffi::OsStr, path::Path};

use cap_std::fs::Dir;

#[cfg(unix)]
pub(super) fn sync_directory(
    directory: &Dir,
    published_name: &OsStr,
) -> Result<(), Box<dyn Error>> {
    #[cfg(any(target_os = "android", target_os = "linux"))]
    use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
    #[cfg(any(target_os = "android", target_os = "linux"))]
    use cap_std::fs::OpenOptions;
    use rustix::fs::{fsync, openat, Mode, OFlags};

    let descriptor = openat(
        directory,
        Path::new("."),
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )?;
    let result = fsync(&descriptor);
    #[cfg(any(target_os = "android", target_os = "linux"))]
    complete_directory_sync(result, || {
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        let published = directory.open_with(published_name, &options)?;
        complete_filesystem_sync(rustix::fs::syncfs(&published), rustix::fs::sync)
    })?;
    #[cfg(not(any(target_os = "android", target_os = "linux")))]
    {
        let _ = published_name;
        result?;
    }
    Ok(())
}

#[cfg(any(target_os = "android", target_os = "linux"))]
pub(in crate::producer) fn complete_filesystem_sync<F>(
    result: rustix::io::Result<()>,
    global_sync: F,
) -> Result<(), Box<dyn Error>>
where
    F: FnOnce(),
{
    match result {
        Err(rustix::io::Errno::BADF) => {
            global_sync();
            Ok(())
        }
        result => {
            result?;
            Ok(())
        }
    }
}

#[cfg(any(target_os = "android", target_os = "linux"))]
pub(in crate::producer) fn complete_directory_sync<F>(
    result: rustix::io::Result<()>,
    filesystem_sync: F,
) -> Result<(), Box<dyn Error>>
where
    F: FnOnce() -> Result<(), Box<dyn Error>>,
{
    match result {
        Err(rustix::io::Errno::BADF) => filesystem_sync(),
        result => {
            result?;
            Ok(())
        }
    }
}

#[cfg(not(unix))]
pub(super) fn sync_directory(
    directory: &Dir,
    _published_name: &OsStr,
) -> Result<(), Box<dyn Error>> {
    directory.try_clone()?.into_std_file().sync_all()?;
    Ok(())
}
