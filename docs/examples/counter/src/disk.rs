//! Small, checksummed records, published atomically while the node owns its files.
use serde::{de::DeserializeOwned, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

pub fn save<T: Serialize>(path: &Path, value: &T) -> io::Result<()> {
    let payload = serde_json::to_vec(value)?;
    let temporary = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(b"RCT1")?;
    file.write_all(&rafter_crc32::crc32(&payload).to_be_bytes())?;
    file.write_all(&payload)?;
    file.sync_all()?;
    drop(file);
    fs::rename(temporary, path)?;
    File::open(path.parent().expect("record has a parent"))?.sync_all()
}

pub fn load<T: DeserializeOwned>(path: &Path) -> io::Result<T> {
    let mut bytes = Vec::new();
    File::open(path)?.take(65_537).read_to_end(&mut bytes)?;
    if bytes.len() < 8 || bytes.len() > 65_536 || &bytes[..4] != b"RCT1" {
        return Err(invalid("invalid counter record"));
    }
    let checksum = u32::from_be_bytes(bytes[4..8].try_into().unwrap());
    if rafter_crc32::crc32(&bytes[8..]) != checksum {
        return Err(invalid("counter record checksum mismatch"));
    }
    serde_json::from_slice(&bytes[8..]).map_err(io::Error::from)
}

pub fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
