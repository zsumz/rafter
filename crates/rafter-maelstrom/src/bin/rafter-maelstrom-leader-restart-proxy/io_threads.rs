use std::io::{BufRead, BufReader};
use std::sync::mpsc;
use std::thread;

pub(super) enum Event {
    Stdin(String),
    ChildStdout(String),
    ChildStderr(String),
}

pub(super) fn spawn_stdin_reader(tx: mpsc::Sender<Event>) {
    thread::spawn(move || {
        for line in std::io::stdin().lock().lines() {
            let Ok(line) = line else {
                break;
            };
            if tx.send(Event::Stdin(line)).is_err() {
                break;
            }
        }
    });
}

pub(super) fn spawn_line_reader<R, F>(reader: R, tx: mpsc::Sender<Event>, wrap: F)
where
    R: std::io::Read + Send + 'static,
    F: Fn(String) -> Event + Send + Copy + 'static,
{
    thread::spawn(move || {
        for line in BufReader::new(reader).lines() {
            let Ok(line) = line else {
                break;
            };
            if tx.send(wrap(line)).is_err() {
                break;
            }
        }
    });
}
