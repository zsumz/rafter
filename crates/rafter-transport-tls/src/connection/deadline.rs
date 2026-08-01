//! End-to-end deadline enforcement for unauthenticated handshakes.

use std::{
    io::{self, Read, Write},
    net::TcpStream,
    time::{Duration, Instant},
};

use rustls::StreamOwned;

#[derive(Clone, Copy, Debug)]
pub(crate) struct HandshakeDeadline {
    expires_at: Instant,
}

impl HandshakeDeadline {
    pub(crate) fn new(timeout: Duration) -> io::Result<Self> {
        let expires_at = Instant::now().checked_add(timeout).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "TLS handshake timeout exceeds the platform clock range",
            )
        })?;
        Ok(Self { expires_at })
    }

    pub(crate) fn configure(self, socket: &TcpStream) -> io::Result<()> {
        let remaining = self.expires_at.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "TLS and Rafter handshake deadline expired",
            ));
        }
        socket.set_read_timeout(Some(remaining))?;
        socket.set_write_timeout(Some(remaining))
    }

    pub(crate) fn stream<C>(self, stream: &mut StreamOwned<C, TcpStream>) -> DeadlineStream<'_, C> {
        DeadlineStream {
            stream,
            deadline: self,
        }
    }

    pub(crate) fn socket(self, socket: &mut TcpStream) -> DeadlineSocket<'_> {
        DeadlineSocket {
            socket,
            deadline: self,
        }
    }
}

pub(crate) struct DeadlineSocket<'a> {
    socket: &'a mut TcpStream,
    deadline: HandshakeDeadline,
}

impl Read for DeadlineSocket<'_> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.deadline.configure(self.socket)?;
        self.socket.read(output)
    }
}

impl Write for DeadlineSocket<'_> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.deadline.configure(self.socket)?;
        self.socket.write(input)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.deadline.configure(self.socket)?;
        self.socket.flush()
    }
}

pub(crate) struct DeadlineStream<'a, C> {
    stream: &'a mut StreamOwned<C, TcpStream>,
    deadline: HandshakeDeadline,
}

impl<C> Read for DeadlineStream<'_, C>
where
    StreamOwned<C, TcpStream>: Read,
{
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.deadline.configure(&self.stream.sock)?;
        self.stream.read(output)
    }
}

impl<C> Write for DeadlineStream<'_, C>
where
    StreamOwned<C, TcpStream>: Write,
{
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.deadline.configure(&self.stream.sock)?;
        self.stream.write(input)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.deadline.configure(&self.stream.sock)?;
        self.stream.flush()
    }
}
