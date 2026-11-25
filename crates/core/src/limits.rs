/// Bounds for a buffer of rows: it is full when either bound is reached.
///
/// Rows in this database range from about 1 KB to several MB, so the same
/// number of rows can be a few megabytes or many gigabytes. Sizing buffers by
/// rows alone has caused out-of-memory kills, which is why a byte bound is
/// always part of the limits and why the default byte bound is what usually
/// triggers a flush on real agent traces.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchLimits {
    /// Maximum number of rows.
    pub max_rows: usize,
    /// Maximum number of value bytes (the variable-length payload of the
    /// rows, not counting fixed-width columns).
    pub max_bytes: usize,
}

impl BatchLimits {
    /// 8,192 rows or 32 MiB of value bytes, whichever comes first.
    pub const DEFAULT: BatchLimits = BatchLimits {
        max_rows: 8_192,
        max_bytes: 32 * 1024 * 1024,
    };
}

impl Default for BatchLimits {
    fn default() -> Self {
        Self::DEFAULT
    }
}
