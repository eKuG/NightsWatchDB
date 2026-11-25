//! The tokeniser.
//!
//! ASCII `A-Z` folds to `a-z`. A token is a maximal run of bytes that are
//! `a-z`, `A-Z`, `0-9` or at least `0x80`; every other byte separates tokens.
//! Bytes at or above `0x80` are token bytes so that UTF-8 text in any script
//! stays together without the index needing to know about Unicode.
//!
//! A token longer than [`MAX_TOKEN_LEN`] bytes is not indexed, but it still
//! counts as a token, so that it breaks or extends adjacency in a phrase.

/// The longest token that is indexed, in bytes.
pub const MAX_TOKEN_LEN: usize = 64;

const IS_TOKEN_BYTE: [bool; 256] = {
    let mut t = [false; 256];
    let mut i = 0;
    while i < 256 {
        let b = i as u8;
        t[i] = matches!(b, b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | 0x80..=0xff);
        i += 1;
    }
    t
};

/// One token of a text: a slice of the original bytes, not yet lowercased.
///
/// Keeping the slice (rather than a lowercased copy) is what lets the
/// tokeniser run without allocating, and lets a caller compare a token of any
/// length, including one too long to index.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Token<'a> {
    raw: &'a [u8],
}

impl<'a> Token<'a> {
    /// The token exactly as it appears in the text (original case).
    pub fn raw(&self) -> &'a [u8] {
        self.raw
    }

    /// Length in bytes. A token is never empty.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.raw.len()
    }

    /// True if the token is short enough to be indexed.
    pub fn is_indexable(&self) -> bool {
        self.raw.len() <= MAX_TOKEN_LEN
    }

    /// The lowercased token, written into `buf`, or `None` if the token is
    /// too long to index.
    pub fn lowercase<'b>(&self, buf: &'b mut [u8; MAX_TOKEN_LEN]) -> Option<&'b [u8]> {
        if self.raw.len() > MAX_TOKEN_LEN {
            return None;
        }
        let out = &mut buf[..self.raw.len()];
        out.copy_from_slice(self.raw);
        out.make_ascii_lowercase();
        Some(&*out)
    }

    /// True if this token equals `other` once ASCII case is folded.
    pub fn eq_folded(&self, other: &[u8]) -> bool {
        self.raw.eq_ignore_ascii_case(other)
    }
}

/// Iterator over the tokens of a byte slice. It holds only a slice, so it
/// allocates nothing, per token or at all.
#[derive(Clone, Debug)]
pub struct Tokens<'a> {
    rest: &'a [u8],
}

/// Tokenises `text`.
pub fn tokens(text: &[u8]) -> Tokens<'_> {
    Tokens { rest: text }
}

impl<'a> Iterator for Tokens<'a> {
    type Item = Token<'a>;

    fn next(&mut self) -> Option<Token<'a>> {
        let start = self.rest.iter().position(|&b| IS_TOKEN_BYTE[b as usize])?;
        let rest = &self.rest[start..];
        let end = rest
            .iter()
            .position(|&b| !IS_TOKEN_BYTE[b as usize])
            .unwrap_or(rest.len());
        self.rest = &rest[end..];
        Some(Token { raw: &rest[..end] })
    }
}
