use std::{
    io::{BufRead, BufReader, Read},
    sync::mpsc::{self, Receiver, TryRecvError},
    thread,
};

/// Output lines retained after they have been observed.
#[derive(Debug)]
pub(super) struct RetainedLines {
    receiver: Receiver<String>,
    reader: Option<thread::JoinHandle<()>>,
    lines: Vec<String>,
}

impl RetainedLines {
    pub(super) fn capture(reader: impl Read + Send + 'static) -> Self {
        let (sender, receiver) = mpsc::channel();
        let reader = thread::spawn(move || {
            for line in BufReader::new(reader).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    return;
                }
            }
        });
        Self {
            receiver,
            reader: Some(reader),
            lines: Vec::new(),
        }
    }

    pub(super) fn drain(&mut self) {
        loop {
            match self.receiver.try_recv() {
                Ok(line) => self.lines.push(line),
                Err(TryRecvError::Empty | TryRecvError::Disconnected) => return,
            }
        }
    }

    pub(super) fn lines(&mut self) -> &[String] {
        self.drain();
        &self.lines
    }

    pub(super) fn finish(&mut self) {
        if let Some(reader) = self.reader.take() {
            drop(reader.join());
        }
        self.drain();
    }
}
