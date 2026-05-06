use super::Pipe;
use anyhow::{Context, Result};
use std::net::TcpStream;

pub const DEFAULT_PORT: u16 = 873;

pub fn connect(host: &str, port: u16) -> Result<Pipe> {
    let stream =
        TcpStream::connect((host, port)).with_context(|| format!("tcp connect {host}:{port}"))?;
    stream.set_nodelay(true)?;
    let reader = stream.try_clone()?;
    Ok(Pipe::new(reader, stream))
}
