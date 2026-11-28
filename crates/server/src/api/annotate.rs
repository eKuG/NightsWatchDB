//! `POST /api/v1/annotations`, and how an annotation is shown in the other
//! answers.

use super::body::JsonBody;
use super::{Api, ApiError, Slot, json_response, now_ns};
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use nwdb_core::{SpanId, TraceId};
use nwdb_engine::Annotation;
use serde::de::{Error as _, IgnoredAny, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value, json};
use std::fmt;
use tokio::sync::oneshot;

/// Items in one request; the engine's limit.
const MAX_ITEMS: usize = 1000;

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
    time_ns: Option<i64>,
    value: Option<f64>,
    label: Option<String>,
    comment: Option<String>,
    delete: Option<bool>,
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
                match seq.next_element::<Item>()? {
                    Some(item) => out.push(item),
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

fn parse(body: &[u8]) -> Result<Vec<Item>, ApiError> {
    if body.is_empty() {
        return Err(ApiError::bad("the request body is empty"));
    }
    let req: Req = serde_json::from_slice(body)
        .map_err(|e| ApiError::bad(format!("not a valid annotation request: {e}")))?;
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

/// One annotation as the API shows it.
fn annotation_json(a: &Annotation) -> Value {
    let mut m = Map::new();
    if let Some(s) = a.span_id {
        m.insert("span_id".into(), s.to_string().into());
    }
    m.insert("name".into(), a.name.as_str().into());
    if !a.source.is_empty() {
        m.insert("source".into(), a.source.as_str().into());
    }
    m.insert("time_ns".into(), a.time_ns.into());
    if let Some(v) = a.value {
        m.insert("value".into(), v.into());
    }
    for (k, v) in [("label", &a.label), ("comment", &a.comment)] {
        if let Some(v) = v {
            m.insert(k.into(), v.as_str().into());
        }
    }
    Value::Object(m)
}

pub(super) fn annotations_json(list: &[Annotation]) -> Value {
    Value::Array(list.iter().map(annotation_json).collect())
}
