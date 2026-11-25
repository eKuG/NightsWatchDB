//! One summary row per trace, used to list and filter traces without opening
//! their spans.

use crate::{Error, Result, SpanRecord, TraceId};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

/// What is known about a whole trace, derived from all of its spans.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceSummary {
    /// The trace.
    pub trace_id: TraceId,
    /// Earliest span start, Unix nanoseconds.
    pub start_ns: i64,
    /// Latest span end, Unix nanoseconds.
    pub end_ns: i64,
    /// `service_name` of the root span, if there is a root and it has one.
    /// With several roots, the one of the root with the smallest
    /// `(name, service)` pair.
    pub root_service: Option<String>,
    /// `name` of the root span, if there is a root. With several roots, the
    /// smallest name.
    pub root_name: Option<String>,
    /// Number of spans.
    pub span_count: u32,
    /// True if any span failed.
    pub error: bool,
    /// Sum of `input_tokens` over all spans.
    pub input_tokens: i64,
    /// Sum of `output_tokens` over all spans.
    pub output_tokens: i64,
    /// Sorted distinct `request_model` values.
    pub models: Vec<String>,
    /// Smallest non-null `session_id` among the spans, byte-wise.
    pub session_id: Option<String>,
    /// Smallest non-null `user_id` among the spans, byte-wise.
    pub user_id: Option<String>,
}

impl TraceSummary {
    /// Merges the summary of another part of the same trace into this one.
    ///
    /// A trace's summary is a function of its set of spans only, so merging
    /// partial summaries in any order and grouping gives the summary of all
    /// the spans together (the operation is commutative and associative).
    /// Rules:
    ///
    /// - The trace ids must be equal, else [`Error::InvalidArgument`] and
    ///   `self` is unchanged.
    /// - `start_ns` is the minimum and `end_ns` the maximum.
    /// - `span_count` and the token sums add, saturating; a negative token sum
    ///   counts as 0.
    /// - `error` is the logical or.
    /// - `models` is the sorted distinct union.
    /// - `root_service` and `root_name` come from the summary that has a root
    ///   (a `root_name`). If both do, from the one with the smaller
    ///   `(root_name, root_service)` pair, so they always come from one span.
    /// - `session_id` and `user_id` are the smaller non-null value, byte-wise.
    ///
    /// The root rule for a malformed trace with several roots picks the
    /// smallest name, not the earliest root. Showing the earliest root would
    /// need the root's start time and span id stored in the summary, which
    /// would be a new column.
    pub fn merge(&mut self, other: &TraceSummary) -> Result<()> {
        if self.trace_id != other.trace_id {
            return Err(Error::invalid_argument(format!(
                "cannot merge the summary of trace {} into trace {}",
                other.trace_id, self.trace_id
            )));
        }
        if other.root_name.is_some()
            && (self.root_name.is_none()
                || (&other.root_name, &other.root_service) < (&self.root_name, &self.root_service))
        {
            self.root_name = other.root_name.clone();
            self.root_service = other.root_service.clone();
        }
        let smaller = |mine: &mut Option<String>, theirs: &Option<String>| {
            if let Some(t) = theirs
                && mine.as_ref().is_none_or(|m| t < m)
            {
                *mine = Some(t.clone());
            }
        };
        smaller(&mut self.session_id, &other.session_id);
        smaller(&mut self.user_id, &other.user_id);

        self.start_ns = self.start_ns.min(other.start_ns);
        self.end_ns = self.end_ns.max(other.end_ns);
        self.span_count = self.span_count.saturating_add(other.span_count);
        self.error |= other.error;
        // A negative count in a column counts as 0 (see `SummaryBuilder`), so
        // both sides are non-negative here and saturating addition is
        // associative: the total does not depend on how spans were grouped.
        self.input_tokens = self
            .input_tokens
            .max(0)
            .saturating_add(other.input_tokens.max(0));
        self.output_tokens = self
            .output_tokens
            .max(0)
            .saturating_add(other.output_tokens.max(0));
        let models: BTreeSet<&String> = self.models.iter().chain(&other.models).collect();
        self.models = models.into_iter().cloned().collect();
        Ok(())
    }
}

/// The Arrow schema of a summary file: one row per [`TraceSummary`], with the
/// same field names. `models` is a list of strings.
pub fn summary_schema() -> SchemaRef {
    static SCHEMA: OnceLock<SchemaRef> = OnceLock::new();
    SCHEMA
        .get_or_init(|| {
            use DataType::*;
            Arc::new(Schema::new(vec![
                Field::new("trace_id", FixedSizeBinary(16), false),
                Field::new("start_ns", Int64, false),
                Field::new("end_ns", Int64, false),
                Field::new("root_service", Utf8, true),
                Field::new("root_name", Utf8, true),
                Field::new("span_count", UInt32, false),
                Field::new("error", Boolean, false),
                Field::new("input_tokens", Int64, false),
                Field::new("output_tokens", Int64, false),
                Field::new(
                    "models",
                    List(Arc::new(Field::new_list_field(Utf8, true))),
                    false,
                ),
                Field::new("session_id", Utf8, true),
                Field::new("user_id", Utf8, true),
            ]))
        })
        .clone()
}

/// OTLP `Status.StatusCode` value for an error.
const STATUS_CODE_ERROR: i32 = 2;

struct Acc {
    trace_id: TraceId,
    start_ns: i64,
    end_ns: i64,
    /// `(name, service)` of the smallest root so far.
    root: Option<(String, Option<String>)>,
    span_count: u32,
    error: bool,
    input_tokens: i64,
    output_tokens: i64,
    models: BTreeSet<String>,
    session_id: Option<String>,
    user_id: Option<String>,
}

/// Accumulates the spans of one trace at a time into a [`TraceSummary`].
///
/// Rules:
///
/// - `start_ns` is the earliest span start and `end_ns` the latest span end.
/// - The root is the span with no parent. With several, the one with the
///   smallest `(name, service)` pair. With none, `root_service` and
///   `root_name` are `None`. See [`TraceSummary::merge`] for the limit of
///   this choice.
/// - `error` is true if any span has a non-null `error_type` or a
///   `status_code` of 2.
/// - Token sums treat null as 0 and saturate instead of overflowing.
/// - `models` is the sorted distinct non-null `request_model` values.
/// - `session_id` and `user_id` are the smallest non-null values, so the result
///   does not depend on push order.
#[derive(Default)]
pub struct SummaryBuilder {
    acc: Option<Acc>,
}

impl SummaryBuilder {
    /// Creates a builder with no trace in progress.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a span to the trace in progress. The first span after
    /// [`finish_trace`](Self::finish_trace) starts a new trace.
    ///
    /// Returns [`Error::InvalidArgument`] if the span belongs to a different
    /// trace than the spans already pushed; the span is not added.
    pub fn push(&mut self, s: &SpanRecord<'_>) -> Result<()> {
        let acc = match &mut self.acc {
            Some(acc) => {
                if acc.trace_id != s.trace_id {
                    return Err(Error::invalid_argument(format!(
                        "span of trace {} pushed while summarising trace {}",
                        s.trace_id, acc.trace_id
                    )));
                }
                acc
            }
            None => self.acc.insert(Acc {
                trace_id: s.trace_id,
                start_ns: s.start_ns,
                end_ns: s.end_ns,
                root: None,
                span_count: 0,
                error: false,
                input_tokens: 0,
                output_tokens: 0,
                models: BTreeSet::new(),
                session_id: None,
                user_id: None,
            }),
        };

        acc.start_ns = acc.start_ns.min(s.start_ns);
        acc.end_ns = acc.end_ns.max(s.end_ns);
        acc.span_count = acc.span_count.saturating_add(1);
        acc.error |= s.error_type.is_some() || s.status_code == STATUS_CODE_ERROR;
        // A token count cannot be negative; one that is (written by another
        // ingest path) counts as 0. With only non-negative terms saturating
        // addition is associative, so the sums do not depend on which spans
        // were summarised together.
        acc.input_tokens = acc
            .input_tokens
            .saturating_add(s.input_tokens.unwrap_or(0).max(0));
        acc.output_tokens = acc
            .output_tokens
            .saturating_add(s.output_tokens.unwrap_or(0).max(0));

        if s.parent_span_id.is_none() {
            let cand = (s.name, s.service_name);
            if acc
                .root
                .as_ref()
                .is_none_or(|(n, v)| cand < (n.as_str(), v.as_deref()))
            {
                acc.root = Some((s.name.to_owned(), s.service_name.map(str::to_owned)));
            }
        }
        if let Some(m) = s.request_model
            && !acc.models.contains(m)
        {
            acc.models.insert(m.to_owned());
        }
        for (mine, theirs) in [
            (&mut acc.session_id, s.session_id),
            (&mut acc.user_id, s.user_id),
        ] {
            if let Some(t) = theirs
                && mine.as_deref().is_none_or(|m| t < m)
            {
                *mine = Some(t.to_owned());
            }
        }
        Ok(())
    }

    /// Returns the summary of the trace in progress and clears the builder,
    /// or `None` if no span has been pushed.
    pub fn finish_trace(&mut self) -> Option<TraceSummary> {
        let a = self.acc.take()?;
        let (root_service, root_name) = match a.root {
            Some((name, service)) => (service, Some(name)),
            None => (None, None),
        };
        Some(TraceSummary {
            trace_id: a.trace_id,
            start_ns: a.start_ns,
            end_ns: a.end_ns,
            root_service,
            root_name,
            span_count: a.span_count,
            error: a.error,
            input_tokens: a.input_tokens,
            output_tokens: a.output_tokens,
            models: a.models.into_iter().collect(),
            session_id: a.session_id,
            user_id: a.user_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SpanId;

    const T: TraceId = TraceId([7; 16]);

    fn span(n: u8, parent: Option<u8>, start: i64, end: i64) -> SpanRecord<'static> {
        SpanRecord {
            trace_id: T,
            span_id: SpanId([n; 8]),
            parent_span_id: parent.map(|p| SpanId([p; 8])),
            name: "span",
            start_ns: start,
            end_ns: end,
            ..SpanRecord::default()
        }
    }

    fn summarise(spans: &[SpanRecord<'_>]) -> TraceSummary {
        let mut b = SummaryBuilder::new();
        for s in spans {
            b.push(s).unwrap();
        }
        b.finish_trace().unwrap()
    }

    #[test]
    fn schema_shape() {
        let s = summary_schema();
        let names: Vec<_> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            [
                "trace_id",
                "start_ns",
                "end_ns",
                "root_service",
                "root_name",
                "span_count",
                "error",
                "input_tokens",
                "output_tokens",
                "models",
                "session_id",
                "user_id"
            ]
        );
        assert!(matches!(s.field(9).data_type(), DataType::List(_)));
        assert!(s.field(3).is_nullable() && !s.field(5).is_nullable());
    }

    #[test]
    fn time_span_is_earliest_start_latest_end() {
        let s = summarise(&[
            span(1, None, 50, 60),
            span(2, Some(1), 10, 20),
            span(3, Some(1), 30, 90),
        ]);
        assert_eq!((s.start_ns, s.end_ns, s.span_count), (10, 90, 3));
        assert_eq!(s.trace_id, T);
    }

    #[test]
    fn root_is_the_parentless_span() {
        let mut root = span(1, None, 5, 9);
        root.name = "invoke_agent";
        root.service_name = Some("agent");
        let mut child = span(2, Some(1), 1, 2);
        child.service_name = Some("other");
        let s = summarise(&[child, root]);
        assert_eq!(s.root_name.as_deref(), Some("invoke_agent"));
        assert_eq!(s.root_service.as_deref(), Some("agent"));
    }

    #[test]
    fn root_without_service_has_null_service() {
        let s = summarise(&[span(1, None, 0, 1)]);
        assert_eq!(s.root_name.as_deref(), Some("span"));
        assert_eq!(s.root_service, None);
    }

    #[test]
    fn two_roots_pick_the_smallest_pair() {
        let mut late = span(1, None, 10, 20);
        late.name = "b";
        let mut early = span(2, None, 100, 200);
        early.name = "a";
        assert_eq!(summarise(&[late, early]).root_name.as_deref(), Some("a"));

        // Same name: the smaller service, with none before any.
        let mut a = span(1, None, 10, 20);
        a.service_name = Some("z");
        let mut b = span(2, None, 10, 20);
        b.service_name = Some("y");
        let c = span(3, None, 10, 20);
        assert_eq!(summarise(&[a, b, c]).root_service, None);
    }

    #[test]
    fn no_root_gives_null_root_fields() {
        let mut orphan = span(2, Some(1), 0, 1);
        orphan.service_name = Some("svc");
        let s = summarise(&[orphan]);
        assert_eq!((s.root_name, s.root_service), (None, None));
    }

    #[test]
    fn error_rule() {
        assert!(!summarise(&[span(1, None, 0, 1)]).error);
        let mut coded = span(2, Some(1), 0, 1);
        coded.status_code = 2;
        assert!(summarise(&[span(1, None, 0, 1), coded]).error);
        let mut typed = span(3, Some(1), 0, 1);
        typed.error_type = Some("timeout");
        assert!(summarise(&[typed]).error);
        let mut ok = span(4, Some(1), 0, 1);
        ok.status_code = 1;
        assert!(!summarise(&[ok]).error);
    }

    #[test]
    fn token_sums_treat_null_as_zero() {
        let mut a = span(1, None, 0, 1);
        a.input_tokens = Some(10);
        let mut b = span(2, Some(1), 0, 1);
        b.output_tokens = Some(5);
        let mut c = span(3, Some(1), 0, 1);
        c.input_tokens = Some(1);
        c.output_tokens = Some(2);
        let s = summarise(&[a, b, c, span(4, Some(1), 0, 1)]);
        assert_eq!((s.input_tokens, s.output_tokens), (11, 7));
        assert_eq!(summarise(&[span(1, None, 0, 1)]).input_tokens, 0);
    }

    #[test]
    fn models_are_sorted_and_distinct() {
        let mut spans = Vec::new();
        for (i, m) in [Some("zeta"), None, Some("alpha"), Some("zeta")]
            .into_iter()
            .enumerate()
        {
            let mut s = span(i as u8 + 1, Some(9), 0, 1);
            s.request_model = m;
            spans.push(s);
        }
        assert_eq!(summarise(&spans).models, ["alpha", "zeta"]);
        assert!(summarise(&[span(1, None, 0, 1)]).models.is_empty());
    }

    #[test]
    fn session_and_user_are_smallest_non_null() {
        let mut a = span(1, None, 0, 1);
        a.user_id = Some("u1");
        let mut b = span(2, Some(1), 0, 1);
        b.session_id = Some("s1");
        b.user_id = Some("u2");
        let mut c = span(3, Some(1), 0, 1);
        c.session_id = Some("s2");
        let s = summarise(&[a, b, c]);
        assert_eq!(s.session_id.as_deref(), Some("s1"));
        assert_eq!(s.user_id.as_deref(), Some("u1"));
        let mut c2 = span(3, Some(1), 0, 1);
        c2.session_id = Some("s0");
        assert_eq!(summarise(&[c2]).session_id.as_deref(), Some("s0"));
    }

    #[test]
    fn finish_resets_and_empty_is_none() {
        let mut b = SummaryBuilder::new();
        assert!(b.finish_trace().is_none());
        b.push(&span(1, None, 0, 1)).unwrap();
        assert!(b.finish_trace().is_some());
        assert!(b.finish_trace().is_none());

        // A new trace starts clean.
        let mut other = span(2, None, 100, 200);
        other.trace_id = TraceId([8; 16]);
        b.push(&other).unwrap();
        let s = b.finish_trace().unwrap();
        assert_eq!(
            (s.trace_id, s.start_ns, s.span_count),
            (TraceId([8; 16]), 100, 1)
        );
    }

    fn full(id: u8, start: i64, root: Option<(&str, Option<&str>)>) -> TraceSummary {
        TraceSummary {
            trace_id: TraceId([id; 16]),
            start_ns: start,
            end_ns: start + 10,
            root_service: root.and_then(|r| r.1).map(str::to_owned),
            root_name: root.map(|r| r.0.to_owned()),
            span_count: 2,
            error: false,
            input_tokens: 5,
            output_tokens: 7,
            models: vec!["m1".into()],
            session_id: None,
            user_id: None,
        }
    }

    /// Merges in both orders, checks they agree and returns the result.
    fn merged(a: &TraceSummary, b: &TraceSummary) -> TraceSummary {
        let mut ab = a.clone();
        ab.merge(b).unwrap();
        let mut ba = b.clone();
        ba.merge(a).unwrap();
        assert_eq!(ab, ba, "merge must not depend on the order");
        ab
    }

    #[test]
    fn merge_requires_the_same_trace() {
        let mut a = full(1, 0, None);
        let before = a.clone();
        assert!(matches!(
            a.merge(&full(2, 0, None)),
            Err(Error::InvalidArgument(_))
        ));
        assert_eq!(a, before);
    }

    #[test]
    fn merge_combines_times_counts_tokens_error_and_models() {
        let mut a = full(1, 100, None);
        a.end_ns = 500;
        a.models = vec!["b".into(), "d".into()];
        let mut b = full(1, 50, None);
        b.end_ns = 200;
        b.error = true;
        b.span_count = 3;
        b.input_tokens = 1;
        b.output_tokens = 2;
        b.models = vec!["a".into(), "b".into()];
        let m = merged(&a, &b);
        assert_eq!((m.start_ns, m.end_ns), (50, 500));
        assert_eq!(m.span_count, 5);
        assert_eq!((m.input_tokens, m.output_tokens), (6, 9));
        assert!(m.error);
        assert_eq!(m.models, ["a", "b", "d"]);

        let mut big = full(1, 0, None);
        big.span_count = u32::MAX;
        big.input_tokens = i64::MAX;
        let m = merged(&big, &full(1, 0, None));
        assert_eq!((m.span_count, m.input_tokens), (u32::MAX, i64::MAX));
    }

    #[test]
    fn merge_takes_the_root_from_the_summary_that_has_one() {
        let rooted = full(1, 500, Some(("root", Some("svc"))));
        let rootless = full(1, 10, None);
        let m = merged(&rooted, &rootless);
        assert_eq!(m.root_name.as_deref(), Some("root"));
        assert_eq!(m.root_service.as_deref(), Some("svc"));
        let m = merged(&rootless, &rootless.clone());
        assert_eq!((m.root_name, m.root_service), (None, None));
    }

    #[test]
    fn merge_with_two_roots_takes_the_smallest_pair() {
        let early = full(1, 10, Some(("b", None)));
        let late = full(1, 20, Some(("a", Some("svc"))));
        let m = merged(&early, &late);
        // The service is that of the root that won the name.
        assert_eq!(m.root_name.as_deref(), Some("a"));
        assert_eq!(m.root_service.as_deref(), Some("svc"));

        // The smaller (name, service) wins in either order.
        let a = full(1, 10, Some(("a", Some("z"))));
        let b = full(1, 10, Some(("b", Some("a"))));
        assert_eq!(merged(&a, &b).root_name.as_deref(), Some("a"));
        let a2 = full(1, 10, Some(("a", None)));
        assert_eq!(merged(&a, &a2).root_service, None);
    }

    #[test]
    fn merge_session_and_user_take_the_smaller() {
        let mut early = full(1, 10, None);
        early.session_id = Some("s-b".into());
        let mut late = full(1, 20, None);
        late.session_id = Some("s-a".into());
        late.user_id = Some("u-late".into());
        let m = merged(&early, &late);
        // Smaller session, and the user from the only one that has it.
        assert_eq!(m.session_id.as_deref(), Some("s-a"));
        assert_eq!(m.user_id.as_deref(), Some("u-late"));

        // The smaller string, in either order.
        let mut a = full(1, 10, None);
        a.user_id = Some("b".into());
        let mut b = full(1, 10, None);
        b.user_id = Some("a".into());
        assert_eq!(merged(&a, &b).user_id.as_deref(), Some("a"));

        let m = merged(&full(1, 0, None), &full(1, 1, None));
        assert_eq!((m.session_id, m.user_id), (None, None));
    }

    #[test]
    fn rejects_span_of_another_trace() {
        let mut b = SummaryBuilder::new();
        b.push(&span(1, None, 0, 1)).unwrap();
        let mut other = span(2, None, 0, 1);
        other.trace_id = TraceId([8; 16]);
        assert!(matches!(b.push(&other), Err(Error::InvalidArgument(_))));
        assert_eq!(b.finish_trace().unwrap().span_count, 1);
    }

    /// Seeded generator: traces with several roots, missing and conflicting
    /// session and user ids, and equal start times.
    fn random_spans(rng: &mut impl rand::Rng) -> Vec<SpanRecord<'static>> {
        const NAMES: [&str; 3] = ["a", "b", "c"];
        const SERVICES: [Option<&str>; 3] = [None, Some("x"), Some("y")];
        const IDS: [Option<&str>; 4] = [None, Some("p"), Some("q"), Some("")];
        const MODELS: [Option<&str>; 3] = [None, Some("m1"), Some("m2")];
        let n = rng.random_range(1..=7u8);
        (0..n)
            .map(|i| {
                let mut s = span(
                    i + 1,
                    if rng.random_bool(0.4) { None } else { Some(99) },
                    rng.random_range(0..3),
                    rng.random_range(3..6),
                );
                s.name = NAMES[rng.random_range(0..3)];
                s.service_name = SERVICES[rng.random_range(0..3)];
                s.session_id = IDS[rng.random_range(0..4)];
                s.user_id = IDS[rng.random_range(0..4)];
                s.request_model = MODELS[rng.random_range(0..3)];
                s.input_tokens = rng.random_bool(0.5).then(|| match rng.random_range(0..6) {
                    0 => i64::MAX,
                    1 => i64::MAX - rng.random_range(0..3),
                    2 => -rng.random_range(1..9),
                    3 => i64::MIN,
                    _ => rng.random_range(0..9),
                });
                s.output_tokens = rng.random_bool(0.5).then(|| match rng.random_range(0..6) {
                    0 => i64::MAX,
                    1 => -1,
                    _ => rng.random_range(0..9),
                });
                s.status_code = rng.random_range(0..3);
                s
            })
            .collect()
    }

    #[test]
    fn summary_is_a_function_of_the_span_set() {
        use rand::seq::SliceRandom;
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(0x5eed);
        for _ in 0..2000 {
            let mut spans = random_spans(&mut rng);
            let want = summarise(&spans);

            // Any order of the spans.
            spans.shuffle(&mut rng);
            assert_eq!(summarise(&spans), want, "order changed the summary");

            // Any partition into parts, merged in any order and grouping.
            let parts = rng.random_range(1..=spans.len());
            let mut groups: Vec<Vec<SpanRecord<'_>>> = (0..parts).map(|_| Vec::new()).collect();
            for (i, s) in spans.iter().enumerate() {
                // The first `parts` spans make sure no group is empty.
                let g = if i < parts {
                    i
                } else {
                    rng.random_range(0..parts)
                };
                groups[g].push(*s);
            }
            let mut partials: Vec<TraceSummary> = groups.iter().map(|g| summarise(g)).collect();
            partials.shuffle(&mut rng);
            // Left fold.
            let mut left = partials[0].clone();
            for p in &partials[1..] {
                left.merge(p).unwrap();
            }
            assert_eq!(left, want, "partition changed the summary");
            // Random pairwise grouping.
            while partials.len() > 1 {
                let b = partials.swap_remove(rng.random_range(0..partials.len()));
                let i = rng.random_range(0..partials.len());
                partials[i].merge(&b).unwrap();
            }
            assert_eq!(partials[0], want, "grouping changed the summary");
        }
    }

    #[test]
    fn merge_is_associative_and_commutative() {
        use rand::SeedableRng;
        let mut rng = rand::rngs::StdRng::seed_from_u64(7);
        for _ in 0..2000 {
            let a = summarise(&random_spans(&mut rng));
            let b = summarise(&random_spans(&mut rng));
            let c = summarise(&random_spans(&mut rng));
            let m = |x: &TraceSummary, y: &TraceSummary| {
                let mut x = x.clone();
                x.merge(y).unwrap();
                x
            };
            assert_eq!(m(&a, &b), m(&b, &a));
            assert_eq!(m(&m(&a, &b), &c), m(&a, &m(&b, &c)));
        }
    }
}
