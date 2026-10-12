//! End-to-end deadline enforcement for TLS and Rafter handshakes.

use std::{
    io::{self, Read, Write},
    net::TcpStream,
    ops::DerefMut,
    time::{Duration, Instant},
};

use rustls::{ConnectionCommon, SideData, Stream, StreamOwned};

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

// A TLS operation may perform multiple ciphertext reads or writes. The
// underlying socket must enforce the same absolute deadline for each one.
pub(crate) struct DeadlineStream<'a, C> {
    stream: &'a mut StreamOwned<C, TcpStream>,
    deadline: HandshakeDeadline,
}

impl<C, S> Read for DeadlineStream<'_, C>
where
    C: DerefMut<Target = ConnectionCommon<S>>,
    S: SideData,
{
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        self.deadline.configure(&self.stream.sock)?;
        let mut socket = self.deadline.socket(&mut self.stream.sock);
        Stream::new(&mut self.stream.conn, &mut socket).read(output)
    }
}

impl<C, S> Write for DeadlineStream<'_, C>
where
    C: DerefMut<Target = ConnectionCommon<S>>,
    S: SideData,
{
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.deadline.configure(&self.stream.sock)?;
        let mut socket = self.deadline.socket(&mut self.stream.sock);
        Stream::new(&mut self.stream.conn, &mut socket).write(input)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.deadline.configure(&self.stream.sock)?;
        let mut socket = self.deadline.socket(&mut self.stream.sock);
        Stream::new(&mut self.stream.conn, &mut socket).flush()
    }
}
