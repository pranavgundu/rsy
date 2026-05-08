use crate::checksum::{RollingSum, rolling_sum, strong_sum};
use rayon::prelude::*;
use rustc_hash::FxHashMap;
use std::io::{self, Write};

#[derive(Debug, Clone)]
pub struct BlockSum {
    pub rolling: u32,
    pub strong: [u8; 16],
    pub offset: u64,
}

/// Threshold above which we parallelize block-checksum computation within a
/// single file. Below this, file-level parallelism from the caller is enough.
const PAR_SUMS_THRESHOLD: usize = 4 * 1024 * 1024;

/// Compute block checksums for the basis (destination) file.
pub fn basis_sums(data: &[u8], blen: usize) -> Vec<BlockSum> {
    if data.is_empty() || blen == 0 {
        return Vec::new();
    }
    let make = |(i, chunk): (usize, &[u8])| BlockSum {
        rolling: rolling_sum(chunk),
        strong: strong_sum(chunk),
        offset: (i * blen) as u64,
    };
    if data.len() >= PAR_SUMS_THRESHOLD {
        data.par_chunks(blen).enumerate().map(make).collect()
    } else {
        data.chunks(blen).enumerate().map(make).collect()
    }
}

/// Streaming sink for tokens. Sender side writes wire format; local patch path
/// writes reconstructed bytes to disk.
pub trait TokenSink {
    fn on_copy(&mut self, basis_offset: u64, len: u32) -> io::Result<()>;
    fn on_data(&mut self, bytes: &[u8]) -> io::Result<()>;
}

/// Walk `src` against block sums, emitting Copy/Data tokens into `sink`.
/// Returns (literal_bytes, matched_bytes).
pub fn diff_stream<S: TokenSink>(
    src: &[u8],
    sums: &[BlockSum],
    blen: usize,
    sink: &mut S,
) -> io::Result<(usize, usize)> {
    let mut lit = 0usize;
    let mut mat = 0usize;

    if sums.is_empty() {
        if !src.is_empty() {
            sink.on_data(src)?;
            lit = src.len();
        }
        return Ok((lit, mat));
    }
    if src.is_empty() {
        return Ok((0, 0));
    }
    if src.len() < blen {
        sink.on_data(src)?;
        return Ok((src.len(), 0));
    }

    // rolling digest → indices into `sums`
    let mut table: FxHashMap<u32, Vec<u32>> = FxHashMap::default();
    table.reserve(sums.len());
    for (i, bs) in sums.iter().enumerate() {
        table.entry(bs.rolling).or_default().push(i as u32);
    }

    let mut pos: usize = 0;
    let mut lit_start: usize = 0;
    let mut rs = RollingSum::init(&src[0..blen]);

    loop {
        let d = rs.digest();
        if let Some(cands) = table.get(&d) {
            let strong = strong_sum(&src[pos..pos + blen]);
            if let Some(&idx) = cands.iter().find(|&&i| sums[i as usize].strong == strong) {
                if lit_start < pos {
                    let chunk = &src[lit_start..pos];
                    sink.on_data(chunk)?;
                    lit += chunk.len();
                }
                sink.on_copy(sums[idx as usize].offset, blen as u32)?;
                mat += blen;
                pos += blen;
                lit_start = pos;
                if pos + blen > src.len() {
                    break;
                }
                rs = RollingSum::init(&src[pos..pos + blen]);
                continue;
            }
        }

        let out = src[pos];
        pos += 1;
        if pos + blen > src.len() {
            break;
        }
        rs.roll(out, src[pos + blen - 1]);
    }

    if lit_start < src.len() {
        let chunk = &src[lit_start..];
        sink.on_data(chunk)?;
        lit += chunk.len();
    }
    Ok((lit, mat))
}

/// Apply a stream of tokens against `basis`, writing the reconstructed bytes
/// into `out`. Used by the local delta path to avoid materialising the full
/// new file in memory.
pub struct PatchWriter<'b, W: Write> {
    basis: &'b [u8],
    out: W,
    hasher: blake3::Hasher,
}

impl<'b, W: Write> PatchWriter<'b, W> {
    pub fn new(basis: &'b [u8], out: W) -> Self {
        Self {
            basis,
            out,
            hasher: blake3::Hasher::new(),
        }
    }
    pub fn finalize(self) -> (W, [u8; 32]) {
        (self.out, *self.hasher.finalize().as_bytes())
    }
}

impl<'b, W: Write> TokenSink for PatchWriter<'b, W> {
    fn on_copy(&mut self, offset: u64, len: u32) -> io::Result<()> {
        let s = offset as usize;
        let e = s.saturating_add(len as usize).min(self.basis.len());
        if s < self.basis.len() {
            let slice = &self.basis[s..e];
            self.hasher.update(slice);
            self.out.write_all(slice)?;
        }
        Ok(())
    }
    fn on_data(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.hasher.update(bytes);
        self.out.write_all(bytes)
    }
}

// ─── test helpers (also used by network code paths via shared sink trait) ────

/// Materialised token (used by tests and the receiver path which needs to own
/// data read from the wire).
#[derive(Debug, PartialEq, Eq)]
#[cfg(test)]
pub enum Token {
    Copy { offset: u64, len: u32 },
    Data(Vec<u8>),
}

/// Collect tokens into a Vec — convenience for tests and small in-memory paths.
#[cfg(test)]
pub fn diff(src: &[u8], sums: &[BlockSum], blen: usize) -> Vec<Token> {
    struct Vec_(Vec<Token>);
    impl TokenSink for Vec_ {
        fn on_copy(&mut self, offset: u64, len: u32) -> io::Result<()> {
            self.0.push(Token::Copy { offset, len });
            Ok(())
        }
        fn on_data(&mut self, bytes: &[u8]) -> io::Result<()> {
            self.0.push(Token::Data(bytes.to_vec()));
            Ok(())
        }
    }
    let mut v = Vec_(Vec::new());
    diff_stream(src, sums, blen, &mut v).expect("Vec sink never fails");
    v.0
}

#[cfg(test)]
pub fn patch(basis: &[u8], tokens: &[Token]) -> Vec<u8> {
    let cap: usize = tokens
        .iter()
        .map(|t| match t {
            Token::Copy { len, .. } => *len as usize,
            Token::Data(d) => d.len(),
        })
        .sum();
    let mut out = Vec::with_capacity(cap);
    for t in tokens {
        match t {
            Token::Copy { offset, len } => {
                let s = *offset as usize;
                let e = (s + *len as usize).min(basis.len());
                if s < basis.len() {
                    out.extend_from_slice(&basis[s..e]);
                }
            }
            Token::Data(d) => out.extend_from_slice(d),
        }
    }
    out
}

#[cfg(test)]
pub fn token_stats(tokens: &[Token]) -> (usize, usize) {
    let (mut lit, mut mat) = (0usize, 0usize);
    for t in tokens {
        match t {
            Token::Copy { len, .. } => mat += *len as usize,
            Token::Data(d) => lit += d.len(),
        }
    }
    (lit, mat)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(src: &[u8], basis: &[u8], blen: usize) {
        let sums = basis_sums(basis, blen);
        let tokens = diff(src, &sums, blen);
        let got = patch(basis, &tokens);
        assert_eq!(got, src, "round-trip mismatch");
    }

    #[test]
    fn identical_files_are_all_copy() {
        let blen = 64usize;
        let data = vec![0xABu8; blen * 20];
        let sums = basis_sums(&data, blen);
        let tokens = diff(&data, &sums, blen);
        let (lit, matched) = token_stats(&tokens);
        assert_eq!(lit, 0);
        assert!(matched > 0);
        round_trip(&data, &data, blen);
    }

    #[test]
    fn new_file_is_all_data() {
        let src = b"brand new content";
        let sums = basis_sums(&[], 700);
        let tokens = diff(src, &sums, 700);
        assert!(matches!(tokens.as_slice(), [Token::Data(_)]));
        round_trip(src, &[], 700);
    }

    #[test]
    fn append_to_file() {
        let basis = b"original content here".repeat(50);
        let mut src = basis.clone();
        src.extend_from_slice(b"appended stuff");
        round_trip(&src, &basis, 700);
    }

    #[test]
    fn small_edit_in_middle() {
        let mut basis = vec![0u8; 8000];
        basis[4000] = 1;
        let mut src = basis.clone();
        src[4000] = 2;
        let sums = basis_sums(&basis, 700);
        let tokens = diff(&src, &sums, 700);
        let (_, matched) = token_stats(&tokens);
        let got = patch(&basis, &tokens);
        assert_eq!(got, src);
        assert!(matched > 0);
    }

    #[test]
    fn empty_src_empty_basis() {
        round_trip(&[], &[], 700);
    }

    #[test]
    fn empty_src_nonempty_basis() {
        round_trip(&[], b"some existing data", 700);
    }

    #[test]
    fn prepend_to_file() {
        let basis = b"original content here".repeat(50);
        let mut src = b"PREPENDED HEADER\n".to_vec();
        src.extend_from_slice(&basis);
        round_trip(&src, &basis, 700);
    }

    #[test]
    fn delete_bytes_from_middle() {
        let basis: Vec<u8> = (0u8..=255).cycle().take(8000).collect();
        let src: Vec<u8> = basis[..3000]
            .iter()
            .chain(basis[3500..].iter())
            .copied()
            .collect();
        round_trip(&src, &basis, 700);
    }

    #[test]
    fn all_changed_is_all_literal() {
        let blen = 64usize;
        let basis = vec![0u8; blen * 10];
        let src = vec![0xFFu8; blen * 10];
        let sums = basis_sums(&basis, blen);
        let tokens = diff(&src, &sums, blen);
        let (lit, matched) = token_stats(&tokens);
        assert_eq!(matched, 0);
        assert_eq!(lit, src.len());
        round_trip(&src, &basis, blen);
    }

    #[test]
    fn single_byte_file() {
        round_trip(b"A", b"B", 700);
        round_trip(b"A", b"A", 700);
        round_trip(b"A", &[], 700);
    }

    #[test]
    fn repeated_pattern_reuses_blocks() {
        let blen = 64usize;
        let block = vec![0x42u8; blen];
        let basis: Vec<u8> = block.repeat(4);
        let src: Vec<u8> = block.repeat(8);
        let sums = basis_sums(&basis, blen);
        let tokens = diff(&src, &sums, blen);
        let (lit, _) = token_stats(&tokens);
        assert_eq!(lit, 0);
        round_trip(&src, &basis, blen);
    }

    #[test]
    fn basis_sums_rejects_zero_block_size() {
        assert!(basis_sums(b"data", 0).is_empty());
    }

    #[test]
    fn basis_sums_keeps_final_partial_block_offset() {
        let sums = basis_sums(b"abcdefghi", 4);

        assert_eq!(sums.len(), 3);
        assert_eq!(sums[0].offset, 0);
        assert_eq!(sums[1].offset, 4);
        assert_eq!(sums[2].offset, 8);
    }

    #[test]
    fn exact_block_sized_file_round_trips() {
        let basis = b"abcdefgh".repeat(8);
        let mut src = basis.clone();
        src.extend_from_slice(&basis);

        round_trip(&src, &basis, 8);
    }

    #[test]
    fn large_file_parallel_sums_match_serial() {
        let data: Vec<u8> = (0u8..=255).cycle().take(4 * 1024 * 1024 + 1).collect();
        let blen = 700;
        let par = basis_sums(&data, blen);
        let serial: Vec<_> = data
            .chunks(blen)
            .enumerate()
            .map(|(i, chunk)| BlockSum {
                rolling: crate::checksum::rolling_sum(chunk),
                strong: crate::checksum::strong_sum(chunk),
                offset: (i * blen) as u64,
            })
            .collect();
        assert_eq!(par.len(), serial.len());
        for (p, s) in par.iter().zip(serial.iter()) {
            assert_eq!(p.rolling, s.rolling);
            assert_eq!(p.strong, s.strong);
            assert_eq!(p.offset, s.offset);
        }
    }

    #[test]
    fn patch_writer_streams() {
        let basis = b"hello world repeated content".repeat(50);
        let mut src = basis.clone();
        src[10] = b'X';
        let sums = basis_sums(&basis, 64);

        let mut buf: Vec<u8> = Vec::new();
        let mut pw = PatchWriter::new(&basis, &mut buf);
        diff_stream(&src, &sums, 64, &mut pw).unwrap();
        let (_, hash) = pw.finalize();

        assert_eq!(buf, src);
        assert_eq!(hash, *blake3::hash(&src).as_bytes());
    }
}
