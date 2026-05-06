// In-process pipe pair backed by crossbeam channels — test infrastructure only.
use super::Pipe;
use crossbeam_channel::{unbounded, Receiver, Sender};
use std::io::{self, Read, Write};

struct ChanWriter(Sender<Vec<u8>>);
struct ChanReader {
    rx: Receiver<Vec<u8>>,
    buf: Vec<u8>,
}

impl Write for ChanWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .send(buf.to_vec())
            .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "channel closed"))?;
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Read for ChanReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        loop {
            if !self.buf.is_empty() {
                let n = buf.len().min(self.buf.len());
                buf[..n].copy_from_slice(&self.buf[..n]);
                self.buf.drain(..n);
                return Ok(n);
            }
            match self.rx.recv() {
                Ok(data) => self.buf.extend_from_slice(&data),
                Err(_) => return Ok(0), // EOF
            }
        }
    }
}

#[allow(dead_code)]
pub fn pipe_pair() -> (Pipe, Pipe) {
    let (s1, r1) = unbounded::<Vec<u8>>();
    let (s2, r2) = unbounded::<Vec<u8>>();
    let a = Pipe::new(
        ChanReader {
            rx: r1,
            buf: Vec::new(),
        },
        ChanWriter(s2),
    );
    let b = Pipe::new(
        ChanReader {
            rx: r2,
            buf: Vec::new(),
        },
        ChanWriter(s1),
    );
    (a, b)
}
