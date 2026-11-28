//! When to merge parts: the options and the policy, a pure function with no
//! access to the engine, so that it is tested on its own.
//!
//! Every flush writes one small part per window, so a window that keeps
//! receiving data ends up as many small parts, and a window that has stopped
//! receiving it should end up as one. Two rules do that.
//!
//! - **Tiered.** Parts of similar size are merged together once there are
//!   enough of them. A byte is then rewritten once per tier it climbs, about
//!   `log(window size / flush size)` times, instead of once per later flush.
//! - **Final.** A window with several parts of which none is new is merged
//!   into one part. "New" is about when the part became live in this process,
//!   never about the event time of the data, which may be loaded long after it
//!   happened.
//!
//! A merge never crosses a window, never takes more than `max_parts` inputs
//! or a fixed number of input bytes, and never takes inputs whose pieces of
//! text add up to more than a part can hold. At most one merge is chosen at a
//! time.

use std::collections::BTreeMap;
use std::time::Duration;

use nwdb_core::{Error, Result};

/// Settings of compaction, [`EngineOptions::compaction`](crate::EngineOptions).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompactionOptions {
    /// Merge parts at all. Default true.
    pub enabled: bool,
    /// The fewest parts of similar size that are merged by the tiered rule;
    /// at least 2. Default 4.
    pub min_parts: usize,
    /// "Similar" for the tiered rule: the largest part of a run is at most
    /// this many times the smallest; at least 1. Default 4.
    pub size_ratio: u64,
    /// The most parts one merge takes; at least `min_parts`. Default 32.
    pub max_parts: usize,
    /// A window with at least two parts, none of which became live less than
    /// this long ago, is merged into one part. Default 60 seconds.
    pub quiet: Duration,
    /// The most bytes of inputs one merge takes; positive. Default 4 GiB.
    pub max_input_bytes: u64,
    /// After a merge of a window fails, that window is left alone for this
    /// long, and for twice as long after the next failure, and so on up to 64
    /// times this. Other windows are not held back. Default 30
    /// seconds; zero tries again at every pass.
    pub retry_backoff: Duration,
    /// Let the maintenance thread merge. When false the thread only does
    /// retention and merges happen only in [`Engine::run_maintenance`], so a
    /// test decides when. Default true; leave it alone outside tests.
    ///
    /// [`Engine::run_maintenance`]: crate::Engine::run_maintenance
    pub background: bool,
}

impl Default for CompactionOptions {
    fn default() -> Self {
        Self {
            enabled: true,
            min_parts: 4,
            size_ratio: 4,
            max_parts: 32,
            quiet: Duration::from_secs(60),
            max_input_bytes: 4 << 30,
            retry_backoff: Duration::from_secs(30),
            background: true,
        }
    }
}

impl CompactionOptions {
    /// Refuses settings that make no sense. Called by `Engine::open`.
    pub fn validate(&self) -> Result<()> {
        if self.min_parts < 2 {
            return Err(Error::invalid_argument(
                "compaction.min_parts must be at least 2",
            ));
        }
        if self.size_ratio < 1 {
            return Err(Error::invalid_argument(
                "compaction.size_ratio must be at least 1",
            ));
        }
        if self.max_parts < self.min_parts {
            return Err(Error::invalid_argument(
                "compaction.max_parts must be at least min_parts",
            ));
        }
        if self.max_parts > usize::from(u16::MAX) {
            return Err(Error::invalid_argument(
                "compaction.max_parts must be at most 65535",
            ));
        }
        if self.max_input_bytes == 0 {
            return Err(Error::invalid_argument(
                "compaction.max_input_bytes must be positive",
            ));
        }
        Ok(())
    }
}

/// What the policy knows about one live part.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PartInfo {
    /// The part's id.
    pub id: u64,
    /// Start of the part's window, Unix nanoseconds.
    pub window_start_ns: i64,
    /// Width of the part's window, nanoseconds.
    pub window_ns: i64,
    /// Bytes on disk.
    pub size_bytes: u64,
    /// Pieces of text the part holds (`meta.payload.pieces`).
    pub pieces: u64,
    /// Compaction level: 0 for a flush, one more than the highest input for
    /// a merge.
    pub level: u8,
    /// How long ago the part became live in this process.
    pub live_for: Duration,
}

/// Which rule chose a merge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Rule {
    /// Several parts of similar size.
    Tiered,
    /// A window that has stopped receiving parts.
    Final,
}

/// One merge to do: parts of one window, smallest first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MergePlan {
    /// Start of the window, Unix nanoseconds.
    pub window_start_ns: i64,
    /// Width of the window, nanoseconds.
    pub window_ns: i64,
    /// The rule that chose it.
    pub rule: Rule,
    /// Ids of the parts to merge, smallest first.
    pub parts: Vec<u64>,
}

/// The identity of a window.
pub(crate) type WindowKey = (i64, i64);

/// Chooses the merge to do now, or none. `skip` names windows that must not
/// be chosen (backing off or damaged). Among the windows that qualify the one
/// with the most parts goes first, the oldest on a tie.
///
/// `max_pieces` is the most pieces of text one part is written with. A part
/// holds no more; text past it is stored inline and cannot be searched, so a
/// merge whose inputs have more pieces than that between them would change
/// what a search finds. Such inputs are not chosen: the largest are left out
/// until the rest fit, and a window where no two parts fit stays as it is.
///
/// What it does with one huge part and many tiny ones: the tiny ones are
/// merged among themselves (the tiered rule only joins parts within
/// `size_ratio` of each other, so the huge one is not in a run with them) and
/// climb tiers until `min_parts - 1` of them are within `size_ratio` of the
/// huge one, so the huge part is rewritten once for at least about
/// `(min_parts - 1) / size_ratio` of its own size in new data. Once the window
/// has been quiet, the final rule rewrites everything once, huge part
/// included; a window that is a single part is never chosen, so repeated
/// passes cost nothing.
pub(crate) fn choose_merge(
    opts: &CompactionOptions,
    parts: &[PartInfo],
    max_pieces: u64,
    skip: &dyn Fn(i64, i64) -> bool,
) -> Option<MergePlan> {
    if !opts.enabled {
        return None;
    }
    let mut windows: BTreeMap<WindowKey, Vec<&PartInfo>> = BTreeMap::new();
    for p in parts {
        windows
            .entry((p.window_start_ns, p.window_ns))
            .or_default()
            .push(p);
    }
    let mut best: Option<MergePlan> = None;
    let mut best_count = 0;
    for ((start, width), mut ps) in windows {
        if ps.len() < 2 || skip(start, width) {
            continue;
        }
        // Smallest first; the id makes the order total.
        ps.sort_by_key(|p| (p.size_bytes, p.id));
        let quiet = ps.iter().all(|p| p.live_for >= opts.quiet);
        let (rule, mut chosen) = if quiet {
            // The smallest, as many as one merge takes; the window becomes
            // one part over as many merges as that needs.
            let mut all = ps.clone();
            all.truncate(opts.max_parts);
            (Rule::Final, all)
        } else if let Some(run) = tiered_run(opts, &ps) {
            (Rule::Tiered, run)
        } else {
            continue;
        };
        // The byte cap and the piece ceiling drop the largest first.
        let mut bytes: u64 = chosen.iter().map(|p| p.size_bytes).sum();
        let mut pieces: u64 = chosen.iter().map(|p| p.pieces).sum();
        while (bytes > opts.max_input_bytes || pieces > max_pieces) && chosen.len() >= 2 {
            let last = chosen.pop().expect("two or more parts");
            bytes -= last.size_bytes;
            pieces -= last.pieces;
        }
        if chosen.len() < 2 || bytes > opts.max_input_bytes || pieces > max_pieces {
            continue;
        }
        if best.is_none() || ps.len() > best_count {
            best_count = ps.len();
            best = Some(MergePlan {
                window_start_ns: start,
                window_ns: width,
                rule,
                parts: chosen.iter().map(|p| p.id).collect(),
            });
        }
    }
    best
}

/// The first (smallest) run of at least `min_parts` parts, in size order, whose
/// largest is at most `size_ratio` times its smallest, cut to `max_parts`.
fn tiered_run<'a>(opts: &CompactionOptions, sorted: &[&'a PartInfo]) -> Option<Vec<&'a PartInfo>> {
    for (i, first) in sorted.iter().enumerate() {
        // A size of zero must not make every other size "too big".
        let limit = first.size_bytes.max(1).saturating_mul(opts.size_ratio);
        let end = i + sorted[i..]
            .iter()
            .take_while(|p| p.size_bytes <= limit)
            .count();
        if end - i >= opts.min_parts {
            let end = end.min(i + opts.max_parts);
            return Some(sorted[i..end].to_vec());
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    const W: i64 = 1_000;

    fn p(id: u64, window: i64, size: u64, live_secs: u64) -> PartInfo {
        PartInfo {
            id,
            window_start_ns: window * W,
            window_ns: W,
            size_bytes: size,
            pieces: 0,
            level: 0,
            live_for: Duration::from_secs(live_secs),
        }
    }

    type Picked = Option<(Rule, i64, Vec<u64>)>;

    fn pick(opts: &CompactionOptions, parts: &[PartInfo]) -> Picked {
        choose_merge(opts, parts, u64::MAX, &|_, _| false)
            .map(|m| (m.rule, m.window_start_ns / W, m.parts))
    }

    #[test]
    fn tables_of_cases() {
        let o = CompactionOptions::default();
        // (name, parts, expected)
        let cases: Vec<(&str, Vec<PartInfo>, Picked)> = vec![
            ("no parts", vec![], None),
            ("one part, quiet", vec![p(1, 0, 10, 999)], None),
            (
                "three equal parts, not quiet: below the threshold",
                vec![p(1, 0, 10, 1), p(2, 0, 10, 1), p(3, 0, 10, 1)],
                None,
            ),
            (
                "exactly min_parts equal parts",
                (1..=4).map(|i| p(i, 0, 10, 1)).collect(),
                Some((Rule::Tiered, 0, vec![1, 2, 3, 4])),
            ),
            (
                "largest exactly ratio times the smallest is inside",
                vec![
                    p(1, 0, 10, 1),
                    p(2, 0, 20, 1),
                    p(3, 0, 30, 1),
                    p(4, 0, 40, 1),
                ],
                Some((Rule::Tiered, 0, vec![1, 2, 3, 4])),
            ),
            (
                "largest one byte over the ratio is outside",
                vec![
                    p(1, 0, 10, 1),
                    p(2, 0, 20, 1),
                    p(3, 0, 30, 1),
                    p(4, 0, 41, 1),
                ],
                None,
            ),
            (
                "a run that starts above the smallest part",
                vec![
                    p(1, 0, 1, 1),
                    p(2, 0, 100, 1),
                    p(3, 0, 110, 1),
                    p(4, 0, 120, 1),
                    p(5, 0, 130, 1),
                ],
                Some((Rule::Tiered, 0, vec![2, 3, 4, 5])),
            ),
            (
                "the smallest run wins when two exist",
                vec![
                    p(1, 0, 10, 1),
                    p(2, 0, 11, 1),
                    p(3, 0, 12, 1),
                    p(4, 0, 13, 1),
                    p(5, 0, 1000, 1),
                    p(6, 0, 1001, 1),
                    p(7, 0, 1002, 1),
                    p(8, 0, 1003, 1),
                ],
                Some((Rule::Tiered, 0, vec![1, 2, 3, 4])),
            ),
            (
                "quiet window with two parts: final, however different the sizes",
                vec![p(1, 0, 1, 60), p(2, 0, 1_000_000, 61)],
                Some((Rule::Final, 0, vec![1, 2])),
            ),
            (
                "one part live for 59 seconds keeps the window from the final rule",
                vec![p(1, 0, 1, 60), p(2, 0, 1_000_000, 59)],
                None,
            ),
            (
                "final wins over tiered when the window is quiet",
                (1..=6).map(|i| p(i, 0, 10, 60)).collect(),
                Some((Rule::Final, 0, vec![1, 2, 3, 4, 5, 6])),
            ),
            (
                "a part in another window does not count towards the threshold",
                vec![
                    p(1, 0, 10, 1),
                    p(2, 0, 10, 1),
                    p(3, 0, 10, 1),
                    p(4, 1, 10, 1),
                ],
                None,
            ),
        ];
        for (name, parts, want) in cases {
            assert_eq!(pick(&o, &parts), want, "{name}");
        }
    }

    #[test]
    fn at_most_max_parts_the_smallest_first() {
        let o = CompactionOptions {
            max_parts: 5,
            ..CompactionOptions::default()
        };
        let parts: Vec<_> = (1..=9).map(|i| p(i, 0, 10 + i, 1)).collect();
        assert_eq!(
            pick(&o, &parts),
            Some((Rule::Tiered, 0, vec![1, 2, 3, 4, 5]))
        );
        // The final rule is limited by it too, and takes the smallest.
        let parts: Vec<_> = (1..=9).map(|i| p(i, 0, 10 + i, 99)).collect();
        let m = choose_merge(
            &CompactionOptions {
                max_parts: 5,
                ..CompactionOptions::default()
            },
            &parts,
            u64::MAX,
            &|_, _| false,
        )
        .unwrap();
        assert_eq!((m.rule, m.parts), (Rule::Final, vec![1, 2, 3, 4, 5]));
    }

    #[test]
    fn the_final_rule_takes_max_parts_at_a_time_until_one_part_is_left() {
        let o = CompactionOptions {
            max_parts: 4,
            ..CompactionOptions::default()
        };
        // 11 quiet parts; larger ids are larger parts. Every merge has at
        // most 4 inputs, the smallest first, and the window ends as one part.
        let mut parts: Vec<PartInfo> = (1..=11).map(|i| p(i, 0, 10 * i, 99)).collect();
        let first = choose_merge(&o, &parts, u64::MAX, &|_, _| false).unwrap();
        assert_eq!((first.rule, first.parts), (Rule::Final, vec![1, 2, 3, 4]));
        let mut merges = 0;
        while let Some(m) = choose_merge(&o, &parts, u64::MAX, &|_, _| false) {
            assert!(m.parts.len() <= 4 && m.parts.len() >= 2, "{m:?}");
            let size = parts
                .iter()
                .filter(|x| m.parts.contains(&x.id))
                .map(|x| x.size_bytes)
                .sum();
            let id = parts.iter().map(|x| x.id).max().unwrap() + 1;
            parts.retain(|x| !m.parts.contains(&x.id));
            parts.push(p(id, 0, size, 99));
            merges += 1;
            assert!(merges < 20, "does not end");
        }
        assert_eq!(parts.len(), 1);
        assert!(merges >= 3);
    }

    #[test]
    fn a_merge_never_takes_inputs_with_more_pieces_than_the_ceiling() {
        let o = CompactionOptions::default();
        let with = |id, size, pieces, live| PartInfo {
            pieces,
            ..p(id, 0, size, live)
        };
        // Final rule: the largest part is left out until the rest fit.
        let parts = [
            with(1, 10, 40, 99),
            with(2, 20, 40, 99),
            with(3, 30, 40, 99),
        ];
        let m = choose_merge(&o, &parts, 100, &|_, _| false).unwrap();
        assert_eq!(m.parts, vec![1, 2]);
        // Exactly at the ceiling fits.
        let m = choose_merge(&o, &parts, 80, &|_, _| false).unwrap();
        assert_eq!(m.parts, vec![1, 2]);
        // One piece less and no two parts fit: the window is left alone.
        assert_eq!(choose_merge(&o, &parts, 79, &|_, _| false), None);
        // Tiered rule: the same.
        let parts: Vec<_> = (1..=4).map(|i| with(i, 10, 30, 1)).collect();
        let m = choose_merge(&o, &parts, 100, &|_, _| false).unwrap();
        assert_eq!((m.rule, m.parts), (Rule::Tiered, vec![1, 2, 3]));
        assert_eq!(choose_merge(&o, &parts, 59, &|_, _| false), None);
        // A window that cannot merge does not hold back another one.
        let mut parts = vec![with(1, 10, 60, 99), with(2, 20, 60, 99)];
        parts.extend([p(3, 1, 5, 99), p(4, 1, 5, 99)]);
        let m = choose_merge(&o, &parts, 100, &|_, _| false).unwrap();
        assert_eq!((m.window_start_ns, m.parts), (W, vec![3, 4]));
    }

    #[test]
    fn the_byte_cap_drops_the_largest() {
        let o = CompactionOptions {
            max_input_bytes: 100,
            ..CompactionOptions::default()
        };
        // Exactly at the cap fits.
        let parts = vec![p(1, 0, 40, 99), p(2, 0, 60, 99)];
        assert_eq!(pick(&o, &parts), Some((Rule::Final, 0, vec![1, 2])));
        // One byte over: the largest goes, one is left, so the window is skipped.
        let parts = vec![p(1, 0, 40, 99), p(2, 0, 61, 99)];
        assert_eq!(pick(&o, &parts), None);
        // Three parts: the largest is dropped and two remain.
        let parts = vec![p(1, 0, 30, 99), p(2, 0, 40, 99), p(3, 0, 70, 99)];
        let m = choose_merge(&o, &parts, u64::MAX, &|_, _| false).unwrap();
        let bytes: u64 = parts
            .iter()
            .filter(|p| m.parts.contains(&p.id))
            .map(|p| p.size_bytes)
            .sum();
        assert_eq!((m.parts, bytes), (vec![1, 2], 70));
        // A tiered run over the cap is cut the same way.
        let parts: Vec<_> = (1..=4).map(|i| p(i, 0, 30, 1)).collect();
        let m = choose_merge(&o, &parts, u64::MAX, &|_, _| false).unwrap();
        assert_eq!((m.rule, m.parts), (Rule::Tiered, vec![1, 2, 3]));
        // A window skipped for its cap does not hold back another window.
        let mut parts = vec![p(1, 0, 40, 99), p(2, 0, 61, 99)];
        parts.extend([p(3, 1, 5, 99), p(4, 1, 5, 99)]);
        assert_eq!(pick(&o, &parts), Some((Rule::Final, 1, vec![3, 4])));
    }

    #[test]
    fn the_window_with_the_most_parts_goes_first_and_skipped_ones_are_left() {
        let o = CompactionOptions::default();
        let mut parts = Vec::new();
        parts.extend((1..=2).map(|i| p(i, 0, 10, 99)));
        parts.extend((3..=7).map(|i| p(i, 1, 10, 99)));
        parts.extend((8..=10).map(|i| p(i, 2, 10, 99)));
        assert_eq!(pick(&o, &parts).unwrap().1, 1);
        let m = choose_merge(&o, &parts, u64::MAX, &|s, _| s == W).unwrap();
        assert_eq!(m.window_start_ns, 2 * W);
        // A tie goes to the older window.
        let parts: Vec<_> = [(1, 5), (2, 5), (3, 2), (4, 2)]
            .into_iter()
            .map(|(i, w)| p(i, w, 10, 99))
            .collect();
        assert_eq!(pick(&o, &parts).unwrap().1, 2);
        // A window that does not qualify does not take the turn.
        let mut parts: Vec<_> = (1..=6).map(|i| p(i, 0, 10u64.pow(i as u32), 1)).collect();
        parts.extend([p(7, 1, 10, 99), p(8, 1, 10, 99)]);
        assert_eq!(pick(&o, &parts).unwrap().1, 1);
        assert!(
            choose_merge(
                &CompactionOptions {
                    enabled: false,
                    ..o
                },
                &parts,
                u64::MAX,
                &|_, _| false
            )
            .is_none()
        );
    }

    #[test]
    fn a_size_of_zero_is_not_a_trap() {
        let o = CompactionOptions::default();
        let parts: Vec<_> = (1..=4).map(|i| p(i, 0, 0, 1)).collect();
        assert!(pick(&o, &parts).is_some());
    }

    #[test]
    fn options_are_validated() {
        assert!(CompactionOptions::default().validate().is_ok());
        for bad in [
            CompactionOptions {
                min_parts: 1,
                ..CompactionOptions::default()
            },
            CompactionOptions {
                size_ratio: 0,
                ..CompactionOptions::default()
            },
            CompactionOptions {
                max_parts: 3,
                ..CompactionOptions::default()
            },
            CompactionOptions {
                max_parts: 70_000,
                ..CompactionOptions::default()
            },
            CompactionOptions {
                max_input_bytes: 0,
                ..CompactionOptions::default()
            },
        ] {
            assert!(bad.validate().is_err(), "{bad:?}");
        }
    }

    /// Plays the policy alone against a long ingest into one window: one
    /// part arrives per tick, merges are applied at once (the output is as
    /// big as its inputs, the worst case: nothing is saved by merging), and
    /// the bytes of every merge are counted.
    struct Sim {
        parts: Vec<PartInfo>,
        next_id: u64,
        ingested: u64,
        rewritten: u64,
        merges: u64,
    }

    impl Sim {
        fn new(parts: Vec<PartInfo>) -> Self {
            let next_id = parts.iter().map(|p| p.id).max().unwrap_or(0) + 1;
            Sim {
                ingested: parts.iter().map(|p| p.size_bytes).sum(),
                parts,
                next_id,
                rewritten: 0,
                merges: 0,
            }
        }

        fn add(&mut self, size: u64) {
            self.ingested += size;
            self.parts.push(p(self.next_id, 0, size, 0));
            self.next_id += 1;
        }

        /// Merges until the policy has nothing left; returns the inputs and
        /// the output id of each merge.
        fn settle(&mut self, o: &CompactionOptions) -> Vec<(Vec<u64>, u64)> {
            let mut done = Vec::new();
            while let Some(m) = choose_merge(o, &self.parts, u64::MAX, &|_, _| false) {
                let level = self
                    .parts
                    .iter()
                    .filter(|p| m.parts.contains(&p.id))
                    .map(|p| p.level)
                    .max()
                    .unwrap();
                let input_bytes: u64 = self
                    .parts
                    .iter()
                    .filter(|p| m.parts.contains(&p.id))
                    .map(|p| p.size_bytes)
                    .sum();
                self.parts.retain(|p| !m.parts.contains(&p.id));
                self.parts.push(PartInfo {
                    level: level + 1,
                    ..p(self.next_id, 0, input_bytes, 0)
                });
                self.next_id += 1;
                self.rewritten += input_bytes;
                self.merges += 1;
                done.push((m.parts, self.next_id - 1));
            }
            done
        }
    }

    #[test]
    fn a_long_ingest_rewrites_each_byte_about_log_times() {
        let o = CompactionOptions::default();
        let n: u64 = 4096;
        let mut sim = Sim::new(Vec::new());
        let mut most_parts = 0;
        for _ in 0..n {
            sim.add(1_000);
            sim.settle(&o);
            most_parts = most_parts.max(sim.parts.len());
        }
        // Every merge joins at least 4 parts, so a byte climbs at most
        // log4(4096) = 6 tiers (7 with the rounding of runs of unequal parts).
        let ratio = sim.rewritten as f64 / sim.ingested as f64;
        eprintln!(
            "{n} flushes of one window: {} merges, {:.2} bytes rewritten per byte ingested, \
             at most {most_parts} parts, {} at the end",
            sim.merges,
            ratio,
            sim.parts.len()
        );
        assert!(ratio <= 7.0, "{ratio}");
        assert!(most_parts <= 3 * 7 + 4, "{most_parts}");
        // Not quadratic: merging all parts at every flush would rewrite
        // n * n / 2 flushes' worth.
        assert!(sim.rewritten < sim.ingested * 8);
    }

    #[test]
    fn one_huge_part_is_not_rewritten_by_a_trickle_of_small_ones() {
        let o = CompactionOptions::default();
        let huge = 1_000_000u64;
        let mut sim = Sim::new(vec![p(1, 0, huge, 0)]);
        let mut huge_rewrites = 0u64;
        let mut huge_ids = vec![1u64];
        let mut small_ingested = 0;
        // Late parts of 1,000 bytes arrive, the window never being quiet, until
        // as much as ten times the huge part has come in.
        while small_ingested < 10 * huge {
            sim.add(1_000);
            small_ingested += 1_000;
            for (inputs, output) in sim.settle(&o) {
                if inputs.iter().any(|id| huge_ids.contains(id)) {
                    huge_rewrites += 1;
                    // The output is now "the huge part".
                    huge_ids.push(output);
                }
            }
        }
        eprintln!(
            "one part of {huge} bytes, {small_ingested} bytes of late data in parts of 1000: \
             the big part was in {huge_rewrites} merges, {} bytes rewritten, {} ingested",
            sim.rewritten, sim.ingested
        );
        // Each rewrite of the big part needs at least min_parts - 1 = 3 parts
        // within the ratio of it, so about 0.75 of its size in new data: the
        // total rewritten stays proportional to the data ingested times the
        // number of tiers, and the big part is not touched on every flush.
        assert!(huge_rewrites <= 20, "{huge_rewrites}");
        assert!(
            huge_rewrites * 100 < small_ingested / 1_000,
            "{huge_rewrites}"
        );
        assert!(sim.rewritten < sim.ingested * 10, "{}", sim.rewritten);
    }

    #[test]
    fn the_final_rule_rewrites_everything_once_when_the_window_goes_quiet() {
        let o = CompactionOptions::default();
        let mut parts = vec![p(1, 0, 1_000_000, 1)];
        parts.extend((2..=4).map(|i| p(i, 0, 1_000, 1)));
        // Not quiet: three small parts are below the tiered threshold and the
        // huge one is out of their ratio.
        assert_eq!(pick(&o, &parts), None);
        for x in &mut parts {
            x.live_for = Duration::from_secs(61);
        }
        let m = choose_merge(&o, &parts, u64::MAX, &|_, _| false).unwrap();
        assert_eq!((m.rule, m.parts.len()), (Rule::Final, 4));
        // After the merge the window is one part and nothing is chosen again.
        let one = [p(5, 0, 1_003_000, 0)];
        assert_eq!(pick(&o, &one), None);
    }
}
