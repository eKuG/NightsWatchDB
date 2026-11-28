use std::time::{Duration, Instant};

/// Polls `cond` every 5 ms until it holds; panics naming `what` after `timeout`.
pub fn wait_for(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let end = Instant::now() + timeout;
    while !cond() {
        assert!(Instant::now() < end, "timed out waiting for {what}");
        std::thread::sleep(Duration::from_millis(5));
    }
}
