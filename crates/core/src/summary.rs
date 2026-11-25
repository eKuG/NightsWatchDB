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
    pub root_service: Option<String>,
    /// `name` of the root span, if there is a root.
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
    /// First non-null `session_id` in span order.
    pub session_id: Option<String>,
    /// First non-null `user_id` in span order.
    pub user_id: Option<String>,
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

struct Root {
    start_ns: i64,
    service: Option<String>,
    name: String,
}

struct Acc {
    trace_id: TraceId,
    start_ns: i64,
    end_ns: i64,
    root: Option<Root>,
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
///   earliest start (the first pushed on a tie). With none, `root_service`
///   and `root_name` are `None`.
/// - `error` is true if any span has a non-null `error_type` or a
///   `status_code` of 2.
/// - Token sums treat null as 0 and saturate instead of overflowing.
/// - `models` is the sorted distinct non-null `request_model` values.
/// - `session_id` and `user_id` are the first non-null values in push order.
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
        acc.input_tokens = acc.input_tokens.saturating_add(s.input_tokens.unwrap_or(0));
        acc.output_tokens = acc
            .output_tokens
            .saturating_add(s.output_tokens.unwrap_or(0));

        if s.parent_span_id.is_none() && acc.root.as_ref().is_none_or(|r| s.start_ns < r.start_ns) {
            acc.root = Some(Root {
                start_ns: s.start_ns,
                service: s.service_name.map(str::to_owned),
                name: s.name.to_owned(),
            });
        }
        if let Some(m) = s.request_model
            && !acc.models.contains(m)
        {
            acc.models.insert(m.to_owned());
        }
        if acc.session_id.is_none() {
            acc.session_id = s.session_id.map(str::to_owned);
        }
        if acc.user_id.is_none() {
            acc.user_id = s.user_id.map(str::to_owned);
        }
        Ok(())
    }

    /// Returns the summary of the trace in progress and clears the builder,
    /// or `None` if no span has been pushed.
    pub fn finish_trace(&mut self) -> Option<TraceSummary> {
        let a = self.acc.take()?;
        let (root_service, root_name) = match a.root {
            Some(r) => (r.service, Some(r.name)),
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
    fn two_roots_pick_the_earliest() {
        let mut late = span(1, None, 100, 200);
        late.name = "late";
        let mut early = span(2, None, 10, 20);
        early.name = "early";
        assert_eq!(
            summarise(&[late, early]).root_name.as_deref(),
            Some("early")
        );

        // A tie goes to the first pushed.
        let mut a = span(1, None, 10, 20);
        a.name = "first";
        let mut b = span(2, None, 10, 20);
        b.name = "second";
        assert_eq!(summarise(&[a, b]).root_name.as_deref(), Some("first"));
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
    fn session_and_user_are_first_non_null() {
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

    #[test]
    fn rejects_span_of_another_trace() {
        let mut b = SummaryBuilder::new();
        b.push(&span(1, None, 0, 1)).unwrap();
        let mut other = span(2, None, 0, 1);
        other.trace_id = TraceId([8; 16]);
        assert!(matches!(b.push(&other), Err(Error::InvalidArgument(_))));
        assert_eq!(b.finish_trace().unwrap().span_count, 1);
    }
}
