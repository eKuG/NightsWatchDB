//! `POST /api/v1/annotations`, and how an annotation is shown in the other
//! answers.

use super::body::JsonBody;
use super::{Api, ApiError, Slot, json_response, json_str, now_ns, short};
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use nwdb_core::{SpanId, TraceId};
use nwdb_engine::Annotation;
use serde::de::value::{
    BoolDeserializer, F64Deserializer, I64Deserializer, MapAccessDeserializer, U64Deserializer,
};
use serde::de::{Error as _, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Value, json};
use std::fmt;
use std::io::{self, Write};
use std::marker::PhantomData;
use tokio::sync::oneshot;

/// Items in one request; the engine's limit.
const MAX_ITEMS: usize = 1000;
/// Bytes of a parse error that a 400 repeats; a parse error can quote the
/// offending text.
const ERROR_DETAIL: usize = 160;

/// A `T` that is read from a JSON object only. serde builds a derived struct
/// from a JSON array as well, field by field, and `deny_unknown_fields` does
/// not apply to that form; this wrapper asks for a map, so an array is an
/// error, and then lets `T` read the map as usual. A string gets a fixed
/// message: the one serde builds when a map is asked for quotes the string.
struct Object<T>(T);

impl<'de, T: Deserialize<'de>> Deserialize<'de> for Object<T> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V<T>(PhantomData<T>);
        impl<'de, T: Deserialize<'de>> Visitor<'de> for V<T> {
            type Value = T;

            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("a JSON object")
            }

            fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<T, A::Error> {
                T::deserialize(MapAccessDeserializer::new(map))
            }

            fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<T, E> {
                Err(E::custom("invalid type: string, expected a JSON object"))
            }
        }
        // `deserialize_map` would let serde_json build the error for a string
        // itself, quoting it; `deserialize_any` hands the string to the
        // visitor above.
        d.deserialize_any(V(PhantomData)).map(Object)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Req {
    #[serde(deserialize_with = "items")]
    annotations: Vec<Item>,
}

/// One item as sent. Ids stay strings here: a bad id rejects the item, not
/// the request.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Item {
    trace_id: String,
    span_id: Option<String>,
    name: String,
    source: Option<String>,
    #[serde(default, deserialize_with = "int")]
    time_ns: Option<i64>,
    #[serde(default, deserialize_with = "float")]
    value: Option<f64>,
    label: Option<String>,
    comment: Option<String>,
    #[serde(default, deserialize_with = "flag")]
    delete: Option<bool>,
}

/// An optional number or bool, read so that a string in its place is refused
/// with a fixed message: serde's own message quotes the string, and a string
/// can be a megabyte.
fn scalar<'de, D: Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
    what: &'static str,
) -> Result<Option<T>, D::Error> {
    struct V<T>(&'static str, PhantomData<T>);
    impl<'de, T: Deserialize<'de>> Visitor<'de> for V<T> {
        type Value = Option<T>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(self.0)
        }

        fn visit_unit<E: serde::de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }

        fn visit_i64<E: serde::de::Error>(self, v: i64) -> Result<Self::Value, E> {
            T::deserialize(I64Deserializer::<E>::new(v)).map(Some)
        }

        fn visit_u64<E: serde::de::Error>(self, v: u64) -> Result<Self::Value, E> {
            T::deserialize(U64Deserializer::<E>::new(v)).map(Some)
        }

        fn visit_f64<E: serde::de::Error>(self, v: f64) -> Result<Self::Value, E> {
            T::deserialize(F64Deserializer::<E>::new(v)).map(Some)
        }

        fn visit_bool<E: serde::de::Error>(self, v: bool) -> Result<Self::Value, E> {
            T::deserialize(BoolDeserializer::<E>::new(v)).map(Some)
        }

        fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self::Value, E> {
            Err(E::custom(format!(
                "invalid type: string, expected {}",
                self.0
            )))
        }
    }
    d.deserialize_any(V(what, PhantomData))
}

fn int<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
    scalar(d, "an integer")
}

fn float<'de, D: Deserializer<'de>>(d: D) -> Result<Option<f64>, D::Error> {
    scalar(d, "a number")
}

fn flag<'de, D: Deserializer<'de>>(d: D) -> Result<Option<bool>, D::Error> {
    scalar(d, "a boolean")
}

/// Reads the list one item at a time and stops at the first one past the
/// limit without building it, so a body of many tiny items never becomes a
/// big vector.
fn items<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Item>, D::Error> {
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Vec<Item>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("a list of annotations")
        }

        fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<Item>, A::Error> {
            let mut out = Vec::new();
            while out.len() < MAX_ITEMS {
                match seq.next_element::<Object<Item>>()? {
                    Some(item) => out.push(item.0),
                    None => return Ok(out),
                }
            }
            if seq.next_element::<IgnoredAny>()?.is_some() {
                return Err(A::Error::custom(format!(
                    "a request holds at most {MAX_ITEMS} annotations"
                )));
            }
            Ok(out)
        }
    }
    d.deserialize_seq(V)
}

/// Collects the start of what is written to it and refuses the rest, so that
/// formatting a long message stops as soon as there is enough of it.
struct Head {
    text: String,
    room: usize,
    cut: bool,
}

impl fmt::Write for Head {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        let mut end = s.len().min(self.room);
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        self.text.push_str(&s[..end]);
        self.room -= end;
        if end < s.len() {
            self.cut = true;
            return Err(fmt::Error);
        }
        Ok(())
    }
}

/// At most [`ERROR_DETAIL`] bytes of serde_json's message, without the
/// " at line L column C" it ends with, and `...` when the message is longer.
/// The message is not copied whole: serde has built it once, and the copy
/// stops at the cap.
fn detail(e: &serde_json::Error) -> String {
    use fmt::Write as _;
    let suffix = format!(" at line {} column {}", e.line(), e.column());
    let mut head = Head {
        text: String::new(),
        room: ERROR_DETAIL + suffix.len(),
        cut: false,
    };
    let _ = write!(head, "{e}");
    if head.cut {
        return short(&head.text, ERROR_DETAIL);
    }
    match head.text.strip_suffix(&suffix) {
        Some(m) => m.to_owned(),
        None => head.text,
    }
}

fn parse(body: &[u8]) -> Result<Vec<Item>, ApiError> {
    if body.is_empty() {
        return Err(ApiError::bad("the request body is empty"));
    }
    let Object(req): Object<Req> = serde_json::from_slice(body).map_err(|e| {
        // The message can quote the text that did not fit; the reply must not
        // grow with the request.
        ApiError::bad(format!(
            "not a valid annotation request: {} (line {}, column {})",
            detail(&e),
            e.line(),
            e.column()
        ))
    })?;
    if req.annotations.is_empty() {
        return Err(ApiError::bad("`annotations` must not be empty"));
    }
    Ok(req.annotations)
}

impl Item {
    /// The record to hand to the engine, or why this item is refused.
    fn into_annotation(self, now: i64) -> Result<Annotation, String> {
        let trace_id: TraceId = self
            .trace_id
            .parse()
            .map_err(|e: nwdb_core::Error| e.to_string())?;
        if trace_id.is_zero() {
            return Err("trace id must not be all zeros".into());
        }
        let span_id = match self.span_id {
            None => None,
            Some(s) => {
                let id: SpanId = s.parse().map_err(|e: nwdb_core::Error| e.to_string())?;
                if id.is_zero() {
                    return Err("span id must not be all zeros".into());
                }
                Some(id)
            }
        };
        Ok(Annotation {
            trace_id,
            span_id,
            name: self.name,
            source: self.source.unwrap_or_default(),
            time_ns: self.time_ns.unwrap_or(now),
            value: self.value,
            label: self.label,
            comment: self.comment,
            delete: self.delete.unwrap_or(false),
            arrival: 0,
        })
    }
}

/// The reply is sent after the log sync that covers the records.
pub(super) async fn annotate(
    State(api): State<Api>,
    Extension(slot): Extension<Slot>,
    JsonBody(body): JsonBody,
) -> Result<Response, ApiError> {
    let items = parse(&body)?;
    // The items own their text; the body is not needed while they are checked
    // and logged.
    drop(body);
    let now = now_ns();
    let mut rejected: Vec<(usize, String)> = Vec::new();
    let mut valid = Vec::with_capacity(items.len());
    // Where each item of `valid` was in the request.
    let mut at = Vec::with_capacity(items.len());
    for (i, item) in items.into_iter().enumerate() {
        match item.into_annotation(now) {
            Ok(a) => {
                valid.push(a);
                at.push(i);
            }
            Err(why) => rejected.push((i, why)),
        }
    }
    let mut accepted = 0;
    // The engine refuses an empty list; with nothing valid there is nothing
    // to log.
    if !valid.is_empty() {
        let (tx, rx) = oneshot::channel();
        // `slot` stays ours until the answer is sent or the client is gone.
        let done = api
            .run(slot.clone(), move |engine| {
                engine.annotate(valid, move |r| {
                    let _ = tx.send(r);
                })
            })
            .await?;
        accepted = done.accepted;
        rejected.extend(done.rejected.into_iter().map(|(j, why)| (at[j], why)));
        // Nothing is logged, and `on_durable` never runs, when none is accepted.
        if accepted > 0 {
            match rx.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => return Err(api.error(&e)),
                Err(_) => {
                    return Err(ApiError::new(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "the engine closed before the annotations were durable",
                    ));
                }
            }
        }
    }
    rejected.sort_by_key(|(i, _)| *i);
    let rejected: Vec<Value> = rejected
        .into_iter()
        .map(|(index, error)| json!({ "index": index, "error": error }))
        .collect();
    drop(slot);
    Ok(json_response(
        StatusCode::OK,
        &json!({ "accepted": accepted, "rejected": rejected }),
    ))
}

/// The annotations of one trace as a JSON array. The keys are in the order
/// of the keys of an object serde_json builds (alphabetical). A list shows no
/// comments.
pub(super) fn write_annotations(
    out: &mut dyn Write,
    list: &[Annotation],
    comments: bool,
) -> io::Result<()> {
    out.write_all(b"[")?;
    for (i, a) in list.iter().enumerate() {
        if i > 0 {
            out.write_all(b",")?;
        }
        write_annotation(out, a, comments)?;
    }
    out.write_all(b"]")
}

fn write_annotation(out: &mut dyn Write, a: &Annotation, comment: bool) -> io::Result<()> {
    out.write_all(b"{")?;
    let mut sep = "";
    if comment && let Some(c) = &a.comment {
        out.write_all(b"\"comment\":")?;
        json_str(out, c)?;
        sep = ",";
    }
    if let Some(l) = &a.label {
        write!(out, "{sep}\"label\":")?;
        json_str(out, l)?;
        sep = ",";
    }
    write!(out, "{sep}\"name\":")?;
    json_str(out, &a.name)?;
    if !a.source.is_empty() {
        out.write_all(b",\"source\":")?;
        json_str(out, &a.source)?;
    }
    if let Some(s) = a.span_id {
        write!(out, ",\"span_id\":\"{s}\"")?;
    }
    write!(out, ",\"time_ns\":{}", a.time_ns)?;
    if let Some(v) = a.value {
        out.write_all(b",\"value\":")?;
        serde_json::to_writer(&mut *out, &v).map_err(io::Error::from)?;
    }
    out.write_all(b"}")
}
