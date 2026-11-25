use std::fmt::Display;

/// The error type returned by every NightsWatchDB crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// An operating-system I/O failure.
    #[error("i/o error: {0}")]
    Io(#[from] std::io::Error),
    /// Data on disk is wrong: bad checksum, truncated file, unknown format.
    #[error("corrupt data: {0}")]
    Corrupt(String),
    /// The caller passed something that can never be valid.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// A requested object does not exist.
    #[error("not found: {0}")]
    NotFound(String),
    /// A compare-and-swap was lost to a concurrent writer; the caller may
    /// re-read and retry.
    #[error("conflict: {0}")]
    Conflict(String),
    /// The component can no longer be used and the outcome of the operation
    /// is unknown, for example after a failed sync. The process must stop,
    /// restart and recover; the operation must not be retried on the same
    /// handle.
    #[error("fatal: {0}")]
    Fatal(String),
    /// Any other failure, such as an error from a library.
    #[error("{0}")]
    Other(Box<dyn std::error::Error + Send + Sync>),
}

/// `Result` with [`Error`] as the error type.
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    /// Builds [`Error::Corrupt`].
    pub fn corrupt(msg: impl Display) -> Self {
        Self::Corrupt(msg.to_string())
    }

    /// Builds [`Error::InvalidArgument`].
    pub fn invalid_argument(msg: impl Display) -> Self {
        Self::InvalidArgument(msg.to_string())
    }

    /// Builds [`Error::NotFound`].
    pub fn not_found(msg: impl Display) -> Self {
        Self::NotFound(msg.to_string())
    }

    /// Builds [`Error::Conflict`].
    pub fn conflict(msg: impl Display) -> Self {
        Self::Conflict(msg.to_string())
    }

    /// Builds [`Error::Fatal`].
    pub fn fatal(msg: impl Display) -> Self {
        Self::Fatal(msg.to_string())
    }

    /// Wraps any error as [`Error::Other`].
    pub fn other(err: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Self {
        Self::Other(err.into())
    }
}

impl From<arrow_schema::ArrowError> for Error {
    fn from(err: arrow_schema::ArrowError) -> Self {
        Self::other(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constructors_and_display() {
        assert_eq!(Error::corrupt("x").to_string(), "corrupt data: x");
        assert!(matches!(
            Error::invalid_argument("x"),
            Error::InvalidArgument(_)
        ));
        assert!(matches!(Error::not_found("x"), Error::NotFound(_)));
        assert!(matches!(Error::conflict("x"), Error::Conflict(_)));
        assert_eq!(Error::fatal("x").to_string(), "fatal: x");
        assert_eq!(Error::other("boom").to_string(), "boom");
        let io = std::io::Error::other("disk");
        assert!(matches!(Error::from(io), Error::Io(_)));
    }

    #[test]
    fn is_send_and_sync() {
        fn check<T: Send + Sync + 'static>() {}
        check::<Error>();
    }
}
