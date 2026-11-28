//! Resident memory of this process, read from `/proc/self/status` (Linux).

/// The value of a `/proc/self/status` line such as `VmHWM:`, in kilobytes;
/// `None` when the file or the line cannot be read.
pub fn status_kb(key: &str) -> Option<usize> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with(key))?;
    line.split_whitespace().nth(1)?.parse().ok()
}

/// Like [`status_kb`] in bytes; panics when the line cannot be read.
pub fn status_bytes(key: &str) -> usize {
    status_kb(key).unwrap() * 1024
}

/// Peak resident memory (`VmHWM`) in bytes.
pub fn peak() -> usize {
    status_bytes("VmHWM:")
}

/// Peak and current (`VmRSS`) resident memory in bytes.
pub fn memory() -> (usize, usize) {
    (status_bytes("VmHWM:"), status_bytes("VmRSS:"))
}
