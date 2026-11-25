//! The tokeniser: ASCII A-Z folded to a-z; a token is a maximal run of a-z, 0-9 and bytes >= 0x80;
//! every other byte separates tokens; tokens longer than 64 bytes are reported as "long".

pub const MAX_TOKEN: usize = 64;

const LUT: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 256 {
        let b = i as u8;
        t[i] = match b {
            b'a'..=b'z' | b'0'..=b'9' | 0x80..=0xff => b,
            b'A'..=b'Z' => b + 32,
            _ => 0,
        };
        i += 1;
    }
    t
};

/// Calls `f(token, false)` for every token of up to 64 bytes (lowercased, in a reused stack buffer)
/// and `f(&[], true)` for every longer token, in order. Long tokens are not indexed but still
/// count as tokens for adjacency. No allocation.
#[inline]
pub fn tokenize(text: &[u8], mut f: impl FnMut(&[u8], bool)) {
    let mut buf = [0u8; MAX_TOKEN];
    let mut n = 0usize;
    for &b in text {
        let c = LUT[b as usize];
        if c != 0 {
            if n < MAX_TOKEN {
                buf[n] = c;
            }
            n += 1;
        } else if n > 0 {
            if n <= MAX_TOKEN {
                f(&buf[..n], false);
            } else {
                f(&[], true);
            }
            n = 0;
        }
    }
    if n > 0 {
        if n <= MAX_TOKEN {
            f(&buf[..n], false);
        } else {
            f(&[], true);
        }
    }
}

/// True if the tokens `a` then `b` are consecutive in the token stream of `text`.
pub fn has_phrase(text: &[u8], a: &[u8], b: &[u8]) -> bool {
    let (mut prev_a, mut found) = (false, false);
    tokenize(text, |t, long| {
        if prev_a && !long && t == b {
            found = true;
        }
        prev_a = !long && t == a;
    });
    found
}
