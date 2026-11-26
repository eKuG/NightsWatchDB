//! Which copy of a span stored more than once the queries return. The rule is
//! that the earliest arrival wins, wherever the copies are stored: in a flush,
//! in `get_trace`, in a merge and in search, before and after any merge, and
//! after a restart that replays the log.

mod common;

use std::time::Duration;

use common::*;
use nwdb_core::TraceId;
use nwdb_engine::textindex::parse;
use nwdb_engine::{AggQuery, Engine, SearchQuery};
use rand::rngs::StdRng;
use rand::{RngCore, SeedableRng};

/// The span sent twice: same trace and span id, different content.
fn copy(name: &str, model: &str, tokens: i64, start: i64) -> TSpan {
    let mut s = TSpan::new(1, 1, start).root();
    s.name = name.into();
    s.model = Some(model.into());
    s.input = Some(tokens);
    s.attrs = format!("needle in the {name} copy").into_bytes();
    s
}

fn name_of(e: &Engine, trace: u128) -> String {
    let got = e.get_trace(TraceId(trace.to_be_bytes())).unwrap().unwrap();
    let spans = from_batches(&got);
    spans
        .iter()
        .find(|s| s.span == 1)
        .map(|s| s.name.clone())
        .unwrap()
}

fn search_names(e: &Engine, limit: usize) -> Vec<String> {
    e.search(&SearchQuery {
        query: parse("needle").unwrap(),
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        limit,
    })
    .unwrap()
    .hits
    .into_iter()
    .map(|h| h.name)
    .collect()
}

fn models(e: &Engine) -> Vec<(Option<String>, u64)> {
    e.aggregate_tokens(&AggQuery {
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        operation: None,
    })
    .unwrap()
    .into_iter()
    .filter(|a| a.model.as_deref().is_some_and(|m| m.starts_with("m-")))
    .map(|a| (a.model, a.count))
    .collect()
}

/// The tiered rule picks parts by size, so it can merge the part holding the
/// first copy with later parts and leave out the part holding the second
/// copy between them. The merged part gets a new, higher id than that part,
/// and from then on both `get_trace` (parts in id order) and every later merge
/// (inputs in id order) take the second copy.
#[test]
fn the_earliest_copy_of_a_span_wins_whatever_parts_the_policy_merges() {
    let dir = tempfile::tempdir().unwrap();
    let mut o = compact_opts(dir.path());
    // Only the tiered rule, until the test reopens the database.
    o.compaction.quiet = Duration::from_secs(3600);
    let e = open(o);
    let mut rng = StdRng::seed_from_u64(7);

    // Flush 1: the first copy, a small part.
    ingest(&e, &[copy("first", "m-first", 10, 100)]).unwrap();
    e.flush().unwrap();
    // Flush 2: the second copy with much other data, a large part.
    let mut big = vec![copy("second", "m-second", 20, 100)];
    for i in 0..300u128 {
        let mut s = TSpan::new(1_000 + i, 1, 200 + (i as i64 % 700)).root();
        s.attrs = vec![0u8; 1024];
        rng.fill_bytes(&mut s.attrs);
        big.push(s);
    }
    ingest(&e, &big).unwrap();
    e.flush().unwrap();
    // Flushes 3 to 5: small parts like the first.
    for k in 0..3u128 {
        let mut s = TSpan::new(2_000 + k, 1, 300).root();
        s.attrs = b"a small late span".to_vec();
        ingest(&e, &[s]).unwrap();
        e.flush().unwrap();
    }
    assert_eq!(e.stats().parts, 5);

    let mut seen = vec![
        ("before any merge, get_trace", name_of(&e, 1)),
        ("before any merge, search", search_names(&e, 10).join(",")),
    ];

    e.run_maintenance().unwrap();
    // The tiered merge took the four small parts (1, 3, 4, 5) and left the
    // large one (2) alone.
    let ids: Vec<u64> = e.snapshot().part_traces().iter().map(|p| p.0).collect();
    assert_eq!(ids, vec![2, 6], "setup: the tiered merge left part 2 alone");
    seen.push(("after the tiered merge, get_trace", name_of(&e, 1)));
    seen.push((
        "after the tiered merge, search",
        search_names(&e, 10).join(","),
    ));
    e.shutdown().unwrap();
    drop(e);

    // The window goes quiet: one part.
    let e = open(compact_opts(dir.path()));
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 1, "setup: the final merge ran");
    seen.push(("after the final merge, get_trace", name_of(&e, 1)));
    seen.push((
        "after the final merge, search",
        search_names(&e, 10).join(","),
    ));
    let agg = models(&e);
    e.shutdown().unwrap();

    let wrong: Vec<_> = seen.iter().filter(|(_, got)| got != "first").collect();
    assert!(
        wrong.is_empty() && agg == vec![(Some("m-first".to_string()), 1)],
        "the span's first copy must win at every stage; observed {seen:?}, aggregate after \
         the final merge {agg:?}"
    );
}

/// Copies in two windows are never merged together, and `get_trace` returns
/// the one in the part with the lower id. Merging the other window gives the
/// first copy's data a higher id than the second copy's part, so the answer
/// changes.
#[test]
fn merging_another_window_does_not_change_which_copy_get_trace_returns() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    // Flush 1: trace 5 starts in window 5 in this flush; the first copy of
    // span 1 is there.
    let mut first = TSpan::new(5, 1, 5_100);
    first.name = "first".into();
    ingest(&e, &[first]).unwrap();
    e.flush().unwrap();
    // Flush 2: an earlier span of trace 5 puts this flush's rows of the trace
    // in window 0, with the second copy of span 1.
    let mut second = TSpan::new(5, 1, 5_100);
    second.name = "second".into();
    ingest(&e, &[TSpan::new(5, 2, 100).root(), second]).unwrap();
    e.flush().unwrap();
    // Flush 3: another trace in window 5.
    ingest(&e, &[TSpan::new(6, 1, 5_200).root()]).unwrap();
    e.flush().unwrap();
    assert_eq!(e.stats().parts, 3);
    let before = name_of(&e, 5);

    // Window 5 has two parts and goes quiet: they are merged; window 0 is one
    // part and is left alone.
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().merges_done, 1, "setup: window 5 was merged");
    let after = name_of(&e, 5);
    e.shutdown().unwrap();
    assert_eq!(
        (before.as_str(), after.as_str()),
        ("first", "first"),
        "get_trace must return the first copy before and after the merge"
    );
}

/// Search keeps the copy of the older part, but stops early on the bound of
/// the next part: when the newer part's copy starts later, it is held first,
/// fills the limit, and the older part is never searched. The answer depends
/// on the limit, disagrees with `get_trace`, and changes when the two parts
/// are merged (the merge keeps the older part's copy).
#[test]
fn search_returns_the_older_parts_copy_whatever_the_limit_and_before_and_after_a_merge() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    ingest(&e, &[copy("first", "m-first", 10, 100)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[copy("second", "m-second", 20, 900)]).unwrap();
    e.flush().unwrap();

    let trace = name_of(&e, 1);
    let one = search_names(&e, 1);
    let ten = search_names(&e, 10);
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 1, "setup: the two parts were merged");
    let merged = search_names(&e, 1);
    e.shutdown().unwrap();

    assert_eq!(
        (trace.as_str(), one, ten, merged),
        (
            "first",
            vec!["first".to_string()],
            vec!["first".to_string()],
            vec!["first".to_string()]
        ),
        "(get_trace, search limit 1, search limit 10, search limit 1 after the merge)"
    );
}

// ---------------------------------------------------------------- more ---

fn search_in(e: &Engine, from_ns: i64, to_ns: i64, limit: usize) -> Vec<String> {
    e.search(&SearchQuery {
        query: parse("needle").unwrap(),
        from_ns,
        to_ns,
        limit,
    })
    .unwrap()
    .hits
    .into_iter()
    .map(|h| h.name)
    .collect()
}

/// `(span id, arrival)` of every row of every part on disk, part by part.
fn arrivals(dir: &std::path::Path) -> Vec<Vec<(u64, u64)>> {
    use arrow_array::cast::AsArray;
    use arrow_array::types::UInt64Type;
    part_dir_names(dir)
        .iter()
        .map(|name| {
            let r = nwdb_segment::PartReader::open(dir.join("parts").join(name)).unwrap();
            let batches = r
                .read_rows(0, r.row_count(), Some(&["span_id", "arrival"]))
                .unwrap();
            let mut out = Vec::new();
            for b in &batches {
                let spans = b.column(0).as_fixed_size_binary();
                let arrival = b.column(1).as_primitive::<UInt64Type>();
                for i in 0..b.num_rows() {
                    let id = u64::from_be_bytes(spans.value(i).try_into().unwrap());
                    out.push((id, arrival.value(i)));
                }
            }
            out
        })
        .collect()
}

/// Two parts of one window hold two copies of span 1. A search whose time
/// range excludes the earlier copy must not return the later one, whatever
/// the limit and before and after the merge.
#[test]
fn a_copy_that_is_not_the_earliest_is_never_a_hit_when_the_earliest_is_out_of_range() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    ingest(&e, &[copy("first", "m-first", 10, 100)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[copy("second", "m-second", 20, 900)]).unwrap();
    e.flush().unwrap();
    assert_eq!(e.stats().parts, 2);

    let mut seen = Vec::new();
    for limit in [1, 10] {
        seen.push(search_in(&e, 500, 2_000, limit));
        seen.push(search_in(&e, 0, 500, limit));
    }
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 1, "setup: the two parts were merged");
    for limit in [1, 10] {
        seen.push(search_in(&e, 500, 2_000, limit));
        seen.push(search_in(&e, 0, 500, limit));
    }
    e.shutdown().unwrap();
    let first = vec!["first".to_string()];
    let none: Vec<String> = Vec::new();
    // Before and after the merge: the range that holds the first copy sees it,
    // the other sees nothing.
    let want = vec![
        none.clone(),
        first.clone(),
        none.clone(),
        first.clone(),
        none,
        first.clone(),
        vec![],
        first,
    ];
    assert_eq!(seen, want);
}

/// The earliest copy does not contain the word; the later one does. No copy
/// is a hit, before and after the merge.
#[test]
fn a_copy_that_is_not_the_earliest_is_never_a_hit_when_the_earliest_does_not_match() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    let mut first = copy("first", "m-first", 10, 100);
    first.attrs = b"nothing to see in the first copy".to_vec();
    ingest(&e, &[first]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[copy("second", "m-second", 20, 900)]).unwrap();
    e.flush().unwrap();

    let before: Vec<_> = [1, 10].map(|l| search_names(&e, l)).into();
    e.run_maintenance().unwrap();
    assert_eq!(e.stats().parts, 1, "setup: the two parts were merged");
    let after: Vec<_> = [1, 10].map(|l| search_names(&e, l)).into();
    let name = name_of(&e, 1);
    e.shutdown().unwrap();
    let none: Vec<String> = Vec::new();
    assert_eq!(
        (before, after, name.as_str()),
        (
            vec![none.clone(), none.clone()],
            vec![none.clone(), none],
            "first"
        )
    );
}

/// The check costs a lookup only for a span of a trace that is in two parts.
#[test]
fn only_hits_of_a_split_trace_cost_a_lookup() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    // Trace 1 in two parts, trace 2 and 3 in one each.
    ingest(&e, &[copy("first", "m-first", 10, 100)]).unwrap();
    e.flush().unwrap();
    let mut other = TSpan::new(1, 2, 150);
    other.attrs = b"needle in another span".to_vec();
    let mut lone = TSpan::new(2, 1, 200).root();
    lone.attrs = b"needle in a trace of one part".to_vec();
    ingest(&e, &[other, lone]).unwrap();
    e.flush().unwrap();
    let q = SearchQuery {
        query: parse("needle").unwrap(),
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        limit: 10,
    };
    let (hits, stats) = e.snapshot().search_with_stats(&q).unwrap();
    assert_eq!(hits.len(), 3);
    // Trace 1 is in two parts and has a hit in each: each asks the other
    // part once. Trace 2 asks nothing.
    assert_eq!(stats.winner_lookups, 2);

    // Without the first span of trace 1 in the range, the trace is still read
    // from each of its two holders.
    let later = SearchQuery {
        from_ns: 150,
        to_ns: 300,
        ..q
    };
    let (hits, stats) = e.snapshot().search_with_stats(&later).unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(
        stats.winner_lookups, 2,
        "one read per holder of the split trace"
    );
    e.shutdown().unwrap();
}

/// A trace held by six parts with a matching row in each is read once from
/// each holder in a search, and each span shows its earliest copy.
#[test]
fn a_trace_split_over_six_parts_is_read_once_per_holder() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    for k in 0..6 {
        let mut spans = vec![copy(&format!("one-{k}"), "m", 1, 100)];
        if k == 3 || k == 5 {
            let mut two = TSpan::new(1, 2, 200).root();
            two.name = format!("two-{k}");
            two.attrs = format!("needle in the two-{k} copy").into_bytes();
            spans.push(two);
        }
        ingest(&e, &spans).unwrap();
        e.flush().unwrap();
    }
    assert_eq!(e.stats().parts, 6);
    let q = SearchQuery {
        query: parse("needle").unwrap(),
        from_ns: i64::MIN,
        to_ns: i64::MAX,
        limit: 10,
    };
    let (hits, stats) = e.snapshot().search_with_stats(&q).unwrap();
    let names: Vec<_> = hits.iter().map(|h| h.name.as_str()).collect();
    assert_eq!(names, vec!["two-3", "one-0"]);
    assert_eq!(stats.winner_lookups, 6);
    e.shutdown().unwrap();
}

/// A later copy in the buffer never shows while the flushed one exists, and
/// the flush that writes it does not change that.
#[test]
fn a_later_copy_in_the_buffer_does_not_hide_the_flushed_one() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    ingest(&e, &[copy("first", "m-first", 10, 100)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[copy("second", "m-second", 20, 100)]).unwrap();
    let mut seen = vec![
        ("buffered, get_trace", name_of(&e, 1)),
        ("buffered, search", search_names(&e, 10).join(",")),
    ];
    e.flush().unwrap();
    seen.push(("flushed, get_trace", name_of(&e, 1)));
    seen.push(("flushed, search", search_names(&e, 10).join(",")));
    e.run_maintenance().unwrap();
    seen.push(("merged, get_trace", name_of(&e, 1)));
    seen.push(("merged, search", search_names(&e, 10).join(",")));
    e.shutdown().unwrap();
    let wrong: Vec<_> = seen.iter().filter(|(_, got)| got != "first").collect();
    assert!(wrong.is_empty(), "{seen:?}");
}

/// Two copies in the buffer, in different records, with the summaries that
/// read the buffer: the first to arrive stays, before and after the flush.
#[test]
fn two_copies_in_the_buffer_keep_the_one_that_arrived_first() {
    let dir = tempfile::tempdir().unwrap();
    let e = open(compact_opts(dir.path()));
    ingest(&e, &[copy("first", "m-first", 10, 100)]).unwrap();
    ingest(&e, &[copy("second", "m-second", 20, 100)]).unwrap();
    let before_flush = name_of(&e, 1);
    let models_before = models(&e);
    e.flush().unwrap();
    let after_flush = name_of(&e, 1);
    let models_after = models(&e);
    e.shutdown().unwrap();
    assert_eq!(
        (before_flush.as_str(), after_flush.as_str()),
        ("first", "first")
    );
    assert_eq!(models_before, vec![(Some("m-first".to_string()), 1)]);
    assert_eq!(models_after, models_before);
}

/// A restart that replays the log between the two arrivals: the values the
/// rows get are the ones they got in the first run, and so are the answers.
#[test]
fn replaying_the_log_between_two_arrivals_gives_the_same_values_and_answers() {
    // Run 1: no restart. Records 1 and 2 are two copies of span 1; record 3
    // is another span.
    let live = tempfile::tempdir().unwrap();
    let e = open(compact_opts(live.path()));
    ingest(&e, &[copy("first", "m-first", 10, 100)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[copy("second", "m-second", 20, 100)]).unwrap();
    let mut other = TSpan::new(2, 7, 300).root();
    other.attrs = b"needle elsewhere".to_vec();
    ingest(&e, &[other.clone()]).unwrap();
    e.flush().unwrap();
    let live_answers = (name_of(&e, 1), search_names(&e, 10));
    e.shutdown().unwrap();

    // Run 2: the engine stops without flushing after the second record (its
    // log is replayed at the next start), then the third arrives.
    let replayed = tempfile::tempdir().unwrap();
    let e = open(compact_opts(replayed.path()));
    ingest(&e, &[copy("first", "m-first", 10, 100)]).unwrap();
    e.flush().unwrap();
    ingest(&e, &[copy("second", "m-second", 20, 100)]).unwrap();
    drop(e);
    let e = open(compact_opts(replayed.path()));
    let during = name_of(&e, 1);
    ingest(&e, &[other]).unwrap();
    e.flush().unwrap();
    let replayed_answers = (name_of(&e, 1), search_names(&e, 10));
    e.shutdown().unwrap();

    assert_eq!(during, "first");
    assert_eq!(live_answers, replayed_answers);
    assert_eq!(live_answers.0, "first");
    // The flushed copy of the second record is in a part of its own, with the
    // arrival it had when it was first applied.
    assert_eq!(arrivals(live.path()), arrivals(replayed.path()));
    assert_eq!(
        arrivals(live.path()),
        vec![vec![(1, 1)], vec![(1, 2), (7, 3)]]
    );
}
