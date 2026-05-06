pub mod daemon;
pub mod ssh;

/// Bidirectional byte channel used by sender/receiver
pub struct Pipe {
    pub rx: Box<dyn std::io::Read + Send>,
    pub tx: Box<dyn std::io::Write + Send>,
}

impl Pipe {
    pub fn new(
        rx: impl std::io::Read + Send + 'static,
        tx: impl std::io::Write + Send + 'static,
    ) -> Self {
        Self {
            rx: Box::new(rx),
            tx: Box::new(tx),
        }
    }

    /// Block until the remote end writes its first byte (i.e. SSH auth is done
    /// and the remote process has started). Puts the byte back so callers see
    /// a normal stream.
    pub fn wait_for_remote(&mut self) -> anyhow::Result<()> {
        use std::io::Read;
        let mut buf = [0u8; 1];
        self.rx.read_exact(&mut buf)?;
        let old = std::mem::replace(&mut self.rx, Box::new(std::io::empty()));
        self.rx = Box::new(std::io::Cursor::new(buf.to_vec()).chain(old));
        Ok(())
    }
}
