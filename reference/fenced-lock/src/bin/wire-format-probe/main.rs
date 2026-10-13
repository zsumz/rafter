//! Exact-archive peer-wire compatibility fixture using public APIs only.

mod messages;
mod refusals;

use std::{collections::BTreeSet, error::Error, fs, path::Path};

use rafter_codec::{decode_message, encode_message};

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        return Err("expected: write|read|refuse DIRECTORY".into());
    }
    let path = Path::new(&args[2]);
    match args[1].as_str() {
        "write" => write(path)?,
        "read" => read(path)?,
        "refuse" => refusals::check(path)?,
        _ => return Err("unknown wire operation".into()),
    }
    println!("passed {}", args[1]);
    Ok(())
}

fn write(directory: &Path) -> Result<(), Box<dyn Error>> {
    fs::create_dir(directory)?;
    let mut tags = BTreeSet::new();
    for (name, message) in messages::cases() {
        let bytes = encode_message(&message)?;
        assert_eq!(&bytes[..4], b"RFPM");
        assert_eq!(bytes[4], 1);
        assert_eq!(bytes[5], messages::tag(&message));
        tags.insert(bytes[5]);
        fs::write(directory.join(format!("{name}.bin")), bytes)?;
    }
    assert_eq!(tags, BTreeSet::from([1, 2, 3, 4, 6, 7, 8, 9, 10]));
    fs::write(directory.join("cases.txt"), inventory())?;
    Ok(())
}

fn read(directory: &Path) -> Result<(), Box<dyn Error>> {
    assert_eq!(
        fs::read_to_string(directory.join("cases.txt"))?,
        inventory()
    );
    for (name, expected) in messages::cases() {
        let bytes = fs::read(directory.join(format!("{name}.bin")))?;
        let actual = decode_message(&bytes)?;
        assert_eq!(
            actual, expected,
            "decoded {name} preserves every public field"
        );
        assert_eq!(
            encode_message(&actual)?,
            bytes,
            "{name} retains canonical v1 bytes"
        );
    }
    Ok(())
}

fn inventory() -> String {
    let mut inventory = String::new();
    for (name, _) in messages::cases() {
        inventory.push_str(&name);
        inventory.push('\n');
    }
    inventory
}
