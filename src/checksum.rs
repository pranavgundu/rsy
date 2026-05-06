pub fn block_size(file_len: u64) -> usize {
    if file_len < 1024 {
        return 700;
    }
    let s = (file_len as f64).sqrt() as usize;
    s.clamp(700, 131_072)
}

/// Rolling (weak) checksum — Adler-32 variant with O(1) slide
#[derive(Clone, Default)]
pub struct RollingSum {
    s1: u32,
    s2: u32,
    pub window: u32,
}

impl RollingSum {
    pub fn init(data: &[u8]) -> Self {
        let mut rs = Self::default();
        for &b in data {
            rs.s1 = rs.s1.wrapping_add(b as u32);
            rs.s2 = rs.s2.wrapping_add(rs.s1);
            rs.window += 1;
        }
        rs
    }

    /// Slide window one byte: drop `out`, add `in_`
    #[inline(always)]
    pub fn roll(&mut self, out: u8, in_: u8) {
        let out = out as u32;
        let in_ = in_ as u32;
        self.s1 = self.s1.wrapping_sub(out).wrapping_add(in_);
        self.s2 = self
            .s2
            .wrapping_sub(self.window.wrapping_mul(out))
            .wrapping_add(self.s1);
    }

    #[inline(always)]
    pub fn digest(&self) -> u32 {
        (self.s1 & 0xffff) | (self.s2 << 16)
    }
}

#[inline]
pub fn rolling_sum(data: &[u8]) -> u32 {
    RollingSum::init(data).digest()
}

/// First 16 bytes of blake3 — SIMD-accelerated
#[inline]
pub fn strong_sum(data: &[u8]) -> [u8; 16] {
    let h = blake3::hash(data);
    h.as_bytes()[..16].try_into().unwrap()
}

/// Full 32-byte blake3 for end-of-file verification
#[inline]
pub fn file_hash(data: &[u8]) -> [u8; 32] {
    *blake3::hash(data).as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rolling_sum_matches_init_on_same_data() {
        let data = b"hello world test data here";
        assert_eq!(rolling_sum(data), RollingSum::init(data).digest());
    }

    #[test]
    fn rolling_slide_matches_fresh_init() {
        let data = b"abcdefghijklmnop";
        let blen = 8;
        // init on first window
        let mut rs = RollingSum::init(&data[..blen]);
        let d0 = rs.digest();
        assert_eq!(d0, rolling_sum(&data[..blen]));
        // slide one byte
        rs.roll(data[0], data[blen]);
        let d1 = rs.digest();
        assert_eq!(d1, rolling_sum(&data[1..blen + 1]));
        // slide again
        rs.roll(data[1], data[blen + 1]);
        assert_eq!(rs.digest(), rolling_sum(&data[2..blen + 2]));
    }

    #[test]
    fn rolling_different_data_different_digest() {
        assert_ne!(rolling_sum(b"aaaa"), rolling_sum(b"bbbb"));
    }

    #[test]
    fn strong_sum_deterministic() {
        let a = strong_sum(b"foo");
        let b = strong_sum(b"foo");
        assert_eq!(a, b);
        assert_ne!(strong_sum(b"foo"), strong_sum(b"bar"));
    }

    #[test]
    fn file_hash_deterministic() {
        assert_eq!(file_hash(b"x"), file_hash(b"x"));
        assert_ne!(file_hash(b"x"), file_hash(b"y"));
    }

    #[test]
    fn block_size_clamps() {
        assert_eq!(block_size(0), 700);
        assert_eq!(block_size(1023), 700);
        // sqrt(1MB) ~ 1024, clamped to 1024
        assert_eq!(block_size(1024 * 1024), 1024);
        // very large file — clamps to max
        assert_eq!(block_size(u64::MAX), 131_072);
    }
}
