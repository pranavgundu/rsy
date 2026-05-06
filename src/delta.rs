use crate::checksum::{RollingSum, rolling_sum, strong_sum};
use rayon::prelude::*;
use rustc_hash::FxHashMap;

#[derive(Debug, Clone)]
pub struct BlockSum {
    pub rolling: u32,
    pub strong: [u8; 16],
    pub offset: u64,
}

/// Threshold above which we parallelize block-checksum computation within a
/// single file.  Below this, file-level parallelism from the caller is enough.
const PAR_SUMS_THRESHOLD: usize = 4 * 1024 * 1024;

/// Compute block checksums for the basis (destination) file.
/// Uses rayon only for large files to avoid par overhead on small ones.
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

#[derive(Debug)]
pub enum Token {
    Copy { offset: u64, len: u32 },
    Data(Vec<u8>),
}

/// Produce minimal token stream to turn `basis` into `src`.
///
/// Uses a rolling (weak) checksum + blake3 (strong) checksum with a
/// FxHashMap for O(1) block lookup.  Window slides one byte at a time,
/// so worst-case is O(src_len) weak checks + a handful of strong checks.
pub fn diff(src: &[u8], sums: &[BlockSum], blen: usize) -> Vec<Token> {
    if sums.is_empty() {
        return if src.is_empty() {
            Vec::new()
        } else {
            vec![Token::Data(src.to_vec())]
        };
    }
    if src.is_empty() {
        return Vec::new();
    }
    if src.len() < blen {
        return vec![Token::Data(src.to_vec())];
    }

    // rolling digest → indices into `sums`
    let mut table: FxHashMap<u32, Vec<u32>> = FxHashMap::default();
    table.reserve(sums.len());
    for (i, bs) in sums.iter().enumerate() {
        table.entry(bs.rolling).or_default().push(i as u32);
    }

    let mut tokens: Vec<Token> = Vec::new();
    let mut pos: usize = 0;
    let mut lit_start: usize = 0;
    let mut rs = RollingSum::init(&src[0..blen]);

    loop {
        let d = rs.digest();

        if let Some(cands) = table.get(&d) {
            let strong = strong_sum(&src[pos..pos + blen]);
            if let Some(&idx) = cands.iter().find(|&&i| sums[i as usize].strong == strong) {
                // Flush pending literals
                if lit_start < pos {
                    tokens.push(Token::Data(src[lit_start..pos].to_vec()));
                }
                tokens.push(Token::Copy {
                    offset: sums[idx as usize].offset,
                    len: blen as u32,
                });
                pos += blen;
                lit_start = pos;
                if pos + blen > src.len() {
                    break;
                }
                rs = RollingSum::init(&src[pos..pos + blen]);
                continue;
            }
        }

        // Slide window one byte
        let out = src[pos];
        pos += 1;
        if pos + blen > src.len() {
            break;
        }
        rs.roll(out, src[pos + blen - 1]);
    }

    if lit_start < src.len() {
        tokens.push(Token::Data(src[lit_start..].to_vec()));
    }
    tokens
}

/// Reconstruct file from basis + token stream
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

pub fn token_stats(tokens: &[Token]) -> (usize, usize) {
    let (mut lit, mut matched) = (0usize, 0usize);
    for t in tokens {
        match t {
            Token::Copy { len, .. } => matched += *len as usize,
            Token::Data(d) => lit += d.len(),
        }
    }
    (lit, matched)
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
        // make length an exact multiple of blen so there's no tail literal
        let data = vec![0xABu8; blen * 20];
        let sums = basis_sums(&data, blen);
        let tokens = diff(&data, &sums, blen);
        let (lit, matched) = token_stats(&tokens);
        assert_eq!(lit, 0, "identical files: zero literal bytes expected");
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
        let (_, matched) = {
            let sums = basis_sums(&basis, 700);
            let tokens = diff(&src, &sums, 700);
            let stats = token_stats(&tokens);
            let got = patch(&basis, &tokens);
            assert_eq!(got, src);
            stats
        };
        assert!(matched > 0, "unchanged blocks should be copied");
    }

    #[test]
    fn empty_src_empty_basis() {
        round_trip(&[], &[], 700);
    }

    #[test]
    fn empty_src_nonempty_basis() {
        let basis = b"some existing data";
        round_trip(&[], basis, 700);
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
        // remove 500 bytes from middle — surrounding blocks should match
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
        assert_eq!(matched, 0, "no blocks should match");
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
        // src has many copies of same block — delta should reuse from basis
        let block = vec![0x42u8; blen];
        let basis: Vec<u8> = block.repeat(4); // 4 blocks
        let src: Vec<u8> = block.repeat(8); // 8 blocks — all matchable from basis
        let sums = basis_sums(&basis, blen);
        let tokens = diff(&src, &sums, blen);
        let (lit, _matched) = token_stats(&tokens);
        assert_eq!(lit, 0, "all blocks present in basis, zero literal bytes");
        round_trip(&src, &basis, blen);
    }

    #[test]
    fn large_file_parallel_sums_match_serial() {
        // PAR_SUMS_THRESHOLD is 4MB — create a file just above it
        let data: Vec<u8> = (0u8..=255).cycle().take(4 * 1024 * 1024 + 1).collect();
        let blen = 700;
        let par = basis_sums(&data, blen);
        // force serial by chunking manually
        let serial: Vec<_> = data
            .chunks(blen)
            .enumerate()
            .map(|(i, chunk)| crate::delta::BlockSum {
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
}
