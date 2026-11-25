/// Default width of a part's time window: 15 minutes, in nanoseconds.
pub const DEFAULT_WINDOW_NS: i64 = 15 * 60 * 1_000_000_000;

/// Returns the start of the window of width `window_ns` that contains
/// `ts_ns`. Windows are aligned to the Unix epoch and the division rounds
/// toward negative infinity, so timestamps before 1970 land in the window
/// that contains them rather than the one after.
///
/// A trace belongs to the window of its earliest span start, so every span of
/// a trace is placed by that one value no matter when it arrives.
///
/// The result saturates at `i64::MIN` for timestamps within one window of it.
///
/// # Panics
///
/// Panics if `window_ns` is not positive.
pub fn window_start(ts_ns: i64, window_ns: i64) -> i64 {
    assert!(window_ns > 0, "window_ns must be positive, got {window_ns}");
    ts_ns.saturating_sub(ts_ns.rem_euclid(window_ns))
}

#[cfg(test)]
mod tests {
    use super::*;

    const W: i64 = DEFAULT_WINDOW_NS;

    #[test]
    fn default_is_fifteen_minutes() {
        assert_eq!(W, 900_000_000_000);
    }

    #[test]
    fn positive_and_boundaries() {
        assert_eq!(window_start(0, W), 0);
        assert_eq!(window_start(1, W), 0);
        assert_eq!(window_start(W - 1, W), 0);
        assert_eq!(window_start(W, W), W);
        assert_eq!(window_start(W + 1, W), W);
        assert_eq!(window_start(5 * W + 7, W), 5 * W);
    }

    #[test]
    fn negative_uses_floor() {
        assert_eq!(window_start(-1, W), -W);
        assert_eq!(window_start(-W, W), -W);
        assert_eq!(window_start(-W - 1, W), -2 * W);
        assert_eq!(window_start(-W + 1, W), -W);
    }

    #[test]
    fn extremes_do_not_overflow() {
        assert_eq!(window_start(i64::MAX, 1), i64::MAX);
        assert_eq!(window_start(i64::MAX, W), i64::MAX - i64::MAX % W);
        assert_eq!(window_start(i64::MIN, W), i64::MIN);
        assert_eq!(window_start(i64::MIN + 1, 3), i64::MIN);
    }

    #[test]
    #[should_panic]
    fn zero_window_panics() {
        window_start(1, 0);
    }
}
