use crate::delta::Token;
use crate::flist::{EntryKind, FileEntry};
use byteorder::{LE, ReadBytesExt, WriteBytesExt};
/// Wire protocol for network (SSH / daemon) mode.
///
/// Forward pipe (sender → receiver): flist, then per-file tokens + hash
/// Backward pipe (receiver → sender): per-file sums, then TAG_DONE
use std::io::{self, Read, Write};
use std::path::PathBuf;

pub const TAG_FLIST_START: u8 = 1;
pub const TAG_FLIST_ENTRY: u8 = 2;
pub const TAG_FLIST_END: u8 = 3;
pub const TAG_SUM_HEAD: u8 = 4;
pub const TAG_SUM_BLOCK: u8 = 5;
pub const TAG_SUM_END: u8 = 6;
pub const TAG_TOKEN_COPY: u8 = 7;
pub const TAG_TOKEN_DATA: u8 = 8;
pub const TAG_TOKEN_END: u8 = 9;
pub const TAG_FILE_HASH: u8 = 10;
pub const TAG_DONE: u8 = 11;

// ── helpers ──────────────────────────────────────────────────────────────────

fn write_buf<W: Write>(w: &mut W, b: &[u8]) -> io::Result<()> {
    let len = u16::try_from(b.len()).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "path too long for protocol (>65535 bytes)",
        )
    })?;
    w.write_u16::<LE>(len)?;
    w.write_all(b)
}

fn read_buf<R: Read + ?Sized>(r: &mut R) -> io::Result<Vec<u8>> {
    let len = r.read_u16::<LE>()? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn path_bytes(p: &std::path::Path) -> Vec<u8> {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        p.as_os_str().as_bytes().to_vec()
    }
    #[cfg(not(unix))]
    {
        p.to_string_lossy().into_owned().into_bytes()
    }
}

fn bytes_path(b: Vec<u8>) -> PathBuf {
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStringExt;
        PathBuf::from(std::ffi::OsString::from_vec(b))
    }
    #[cfg(not(unix))]
    {
        PathBuf::from(String::from_utf8_lossy(&b).into_owned())
    }
}

// ── file list (sender → receiver) ────────────────────────────────────────────

pub fn write_flist_start<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_u8(TAG_FLIST_START)
}

pub fn write_flist_end<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_u8(TAG_FLIST_END)
}

pub fn write_entry<W: Write>(w: &mut W, e: &FileEntry) -> io::Result<()> {
    w.write_u8(TAG_FLIST_ENTRY)?;
    write_buf(w, &path_bytes(&e.path))?;
    w.write_u64::<LE>(e.size)?;
    w.write_i64::<LE>(e.mtime)?;
    w.write_u32::<LE>(e.mode)?;
    w.write_u32::<LE>(e.uid)?;
    w.write_u32::<LE>(e.gid)?;
    match &e.kind {
        EntryKind::Regular => w.write_u8(0)?,
        EntryKind::Dir => w.write_u8(1)?,
        EntryKind::Symlink { target } => {
            w.write_u8(2)?;
            write_buf(w, &path_bytes(target))?;
        }
        EntryKind::Other => w.write_u8(3)?,
    }
    Ok(())
}

pub fn read_entry<R: Read + ?Sized>(r: &mut R) -> io::Result<FileEntry> {
    let path = bytes_path(read_buf(r)?);
    let size = r.read_u64::<LE>()?;
    let mtime = r.read_i64::<LE>()?;
    let mode = r.read_u32::<LE>()?;
    let uid = r.read_u32::<LE>()?;
    let gid = r.read_u32::<LE>()?;
    let kind = match r.read_u8()? {
        0 => EntryKind::Regular,
        1 => EntryKind::Dir,
        2 => EntryKind::Symlink {
            target: bytes_path(read_buf(r)?),
        },
        _ => EntryKind::Other,
    };
    Ok(FileEntry {
        path,
        size,
        mtime,
        mode,
        uid,
        gid,
        kind,
    })
}

// ── checksum requests (receiver → sender) ────────────────────────────────────

pub fn write_sum_head<W: Write>(
    w: &mut W,
    file_idx: u32,
    block_len: u32,
    count: u32,
) -> io::Result<()> {
    w.write_u8(TAG_SUM_HEAD)?;
    w.write_u32::<LE>(file_idx)?;
    w.write_u32::<LE>(block_len)?;
    w.write_u32::<LE>(count)
}

pub fn write_sum_block<W: Write>(w: &mut W, rolling: u32, strong: &[u8; 16]) -> io::Result<()> {
    w.write_u8(TAG_SUM_BLOCK)?;
    w.write_u32::<LE>(rolling)?;
    w.write_all(strong)
}

pub fn write_sum_end<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_u8(TAG_SUM_END)
}

pub fn write_done<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_u8(TAG_DONE)
}

pub struct SumHead {
    pub file_idx: u32,
    pub block_len: u32,
    pub count: u32,
}

/// Read the next message from the receiver (sums or done)
pub enum ReceiverReq {
    Sums(SumHead),
    Done,
}

pub fn read_receiver_req<R: Read + ?Sized>(r: &mut R) -> io::Result<ReceiverReq> {
    loop {
        let tag = r.read_u8()?;
        match tag {
            TAG_SUM_HEAD => {
                let file_idx = r.read_u32::<LE>()?;
                let block_len = r.read_u32::<LE>()?;
                let count = r.read_u32::<LE>()?;
                return Ok(ReceiverReq::Sums(SumHead {
                    file_idx,
                    block_len,
                    count,
                }));
            }
            TAG_DONE => return Ok(ReceiverReq::Done),
            _ => {} // skip unknown tags
        }
    }
}

pub const MAX_SUM_BLOCKS: u32 = 1 << 24; // 16M blocks max (~2TB at min block size)
const MAX_TOKEN_DATA: usize = 256 * 1024 * 1024; // 256 MB per token

pub fn read_sum_blocks<R: Read + ?Sized>(
    r: &mut R,
    count: u32,
) -> io::Result<Vec<(u32, [u8; 16])>> {
    if count > MAX_SUM_BLOCKS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("block count {count} exceeds maximum {MAX_SUM_BLOCKS}"),
        ));
    }
    let mut out = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let tag = r.read_u8()?;
        if tag != TAG_SUM_BLOCK {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected TAG_SUM_BLOCK",
            ));
        }
        let rolling = r.read_u32::<LE>()?;
        let mut strong = [0u8; 16];
        r.read_exact(&mut strong)?;
        out.push((rolling, strong));
    }
    // consume trailing SUM_END
    let end = r.read_u8()?;
    if end != TAG_SUM_END {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected TAG_SUM_END",
        ));
    }
    Ok(out)
}

// ── token stream (sender → receiver) ─────────────────────────────────────────

pub fn write_token<W: Write>(w: &mut W, t: &Token) -> io::Result<()> {
    match t {
        Token::Copy { offset, len } => {
            w.write_u8(TAG_TOKEN_COPY)?;
            w.write_u64::<LE>(*offset)?;
            w.write_u32::<LE>(*len)
        }
        Token::Data(data) => write_data_tokens(w, data, MAX_TOKEN_DATA),
    }
}

pub fn write_token_end<W: Write>(w: &mut W) -> io::Result<()> {
    w.write_u8(TAG_TOKEN_END)
}

pub fn write_file_hash<W: Write>(w: &mut W, hash: &[u8; 32]) -> io::Result<()> {
    w.write_u8(TAG_FILE_HASH)?;
    w.write_all(hash)
}

fn write_data_tokens<W: Write>(w: &mut W, data: &[u8], chunk_size: usize) -> io::Result<()> {
    if data.is_empty() {
        w.write_u8(TAG_TOKEN_DATA)?;
        w.write_u32::<LE>(0)?;
        return Ok(());
    }
    for chunk in data.chunks(chunk_size) {
        w.write_u8(TAG_TOKEN_DATA)?;
        w.write_u32::<LE>(chunk.len() as u32)?;
        w.write_all(chunk)?;
    }
    Ok(())
}

/// Read next token; `None` = TAG_TOKEN_END
pub fn read_token<R: Read + ?Sized>(r: &mut R) -> io::Result<Option<Token>> {
    let tag = r.read_u8()?;
    match tag {
        TAG_TOKEN_COPY => {
            let offset = r.read_u64::<LE>()?;
            let len = r.read_u32::<LE>()?;
            Ok(Some(Token::Copy { offset, len }))
        }
        TAG_TOKEN_DATA => {
            let n = r.read_u32::<LE>()? as usize;
            if n > MAX_TOKEN_DATA {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("token data size {n} exceeds maximum {MAX_TOKEN_DATA}"),
                ));
            }
            let mut data = vec![0u8; n];
            r.read_exact(&mut data)?;
            Ok(Some(Token::Data(data)))
        }
        TAG_TOKEN_END => Ok(None),
        other => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unexpected token tag: {other}"),
        )),
    }
}

pub fn read_file_hash<R: Read + ?Sized>(r: &mut R) -> io::Result<[u8; 32]> {
    let tag = r.read_u8()?;
    if tag != TAG_FILE_HASH {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "expected TAG_FILE_HASH",
        ));
    }
    let mut h = [0u8; 32];
    r.read_exact(&mut h)?;
    Ok(h)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn write_data_tokens_chunked_round_trip() {
        let data: Vec<u8> = (0..53).map(|i| i as u8).collect();
        let mut buf = Vec::new();
        write_data_tokens(&mut buf, &data, 8).unwrap();
        write_token_end(&mut buf).unwrap();

        let mut r = Cursor::new(buf);
        let mut out = Vec::new();
        let mut segments = 0usize;
        loop {
            match read_token(&mut r).unwrap() {
                Some(Token::Data(d)) => {
                    assert!(d.len() <= 8);
                    segments += 1;
                    out.extend_from_slice(&d);
                }
                Some(Token::Copy { .. }) => unreachable!("only data tokens expected"),
                None => break,
            }
        }
        assert!(segments > 1);
        assert_eq!(out, data);
    }
}
