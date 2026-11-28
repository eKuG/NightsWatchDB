//! Receiving the body of a write request. A browser can send a POST to the
//! loopback address from any web page, and the `Host` check cannot tell that
//! from a request of the user. The defence is to refuse everything a page can
//! send across sites without a CORS preflight, and never to answer a
//! preflight with permission: no `Access-Control-*` header is ever sent.
//!
//! A write handler takes [`JsonBody`] as its last argument. Its checks run
//! after the `Host` check and after the request has its concurrency slot, so
//! the memory held for bodies is at most the limit times the slots.

use super::{Api, ApiError};
use axum::body::Body;
use axum::extract::{FromRequest, Request};
use axum::http::header::{CONTENT_ENCODING, CONTENT_LENGTH, CONTENT_TYPE, HOST, ORIGIN};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode};
use bytes::Bytes;
use http_body_util::BodyExt;
use std::time::Duration;

const SEC_FETCH_SITE: HeaderName = HeaderName::from_static("sec-fetch-site");

/// What a write request body may take.
#[derive(Clone, Copy)]
pub(crate) struct BodyLimits {
    /// Largest body in bytes.
    pub(crate) max: usize,
    /// Longest a sender may take to deliver the whole body.
    pub(crate) timeout: Duration,
}

/// The body of a request, exactly as it was sent. It is not parsed: that is
/// for the handler, which also decides what an empty body means.
pub(crate) struct JsonBody(pub(crate) Bytes);

impl FromRequest<Api> for JsonBody {
    type Rejection = ApiError;

    async fn from_request(req: Request, api: &Api) -> Result<Self, ApiError> {
        let (parts, body) = req.into_parts();
        check_headers(&parts.headers)?;
        let limits = api.body;
        let declared = match parts.headers.get(CONTENT_LENGTH) {
            None => None,
            Some(v) => Some(
                v.to_str()
                    .ok()
                    .and_then(|v| v.parse::<usize>().ok())
                    .ok_or_else(|| ApiError::bad("Content-Length is not a number"))?,
            ),
        };
        // Refused before a byte of the body is read.
        if declared.is_some_and(|n| n > limits.max) {
            return Err(too_large(limits.max));
        }
        // One deadline for the whole body, so that a sender that trickles
        // bytes cannot keep the slot either.
        let read = read(body, declared, limits.max);
        match tokio::time::timeout(limits.timeout, read).await {
            Ok(body) => body.map(JsonBody),
            Err(_) => Err(ApiError::new(
                StatusCode::REQUEST_TIMEOUT,
                "the request body was not received in time",
            )),
        }
    }
}

fn too_large(max: usize) -> ApiError {
    ApiError::new(
        StatusCode::PAYLOAD_TOO_LARGE,
        format!("the request body is over the limit of {max} bytes"),
    )
}

fn forbidden(msg: &str) -> ApiError {
    ApiError::new(StatusCode::FORBIDDEN, msg)
}

fn unsupported(msg: &str) -> ApiError {
    ApiError::new(StatusCode::UNSUPPORTED_MEDIA_TYPE, msg)
}

fn check_headers(h: &HeaderMap) -> Result<(), ApiError> {
    let mut origins = h.get_all(ORIGIN).iter();
    if let Some(origin) = origins.next()
        && (origins.next().is_some() || !same_origin(origin, h.get(HOST)))
    {
        return Err(forbidden(
            "the Origin header is not the origin of this server",
        ));
    }
    if h.get_all(SEC_FETCH_SITE)
        .iter()
        .any(|v| !matches!(v.as_bytes(), b"same-origin" | b"none"))
    {
        return Err(forbidden("cross-site requests are refused"));
    }
    if !is_json(h) {
        return Err(unsupported(
            "the Content-Type must be application/json (charset utf-8 if given)",
        ));
    }
    if h.get_all(CONTENT_ENCODING)
        .iter()
        .any(|v| !v.as_bytes().eq_ignore_ascii_case(b"identity"))
    {
        return Err(unsupported("a compressed request body is not supported"));
    }
    Ok(())
}

/// Whether `origin` is `http://` and the `Host` of the request itself. The
/// default port may be left out on either side.
fn same_origin(origin: &HeaderValue, host: Option<&HeaderValue>) -> bool {
    let (Ok(origin), Some(Ok(host))) = (origin.to_str(), host.map(HeaderValue::to_str)) else {
        return false;
    };
    let bare = |s: &str| s.strip_suffix(":80").unwrap_or(s).to_ascii_lowercase();
    let origin = origin.to_ascii_lowercase();
    let Some(origin) = origin.strip_prefix("http://") else {
        return false;
    };
    !origin.is_empty() && bare(origin) == bare(host)
}

/// `application/json` with no charset or `utf-8`, in exactly one header.
fn is_json(h: &HeaderMap) -> bool {
    let mut all = h.get_all(CONTENT_TYPE).iter();
    let (Some(v), None) = (all.next(), all.next()) else {
        return false;
    };
    let Ok(v) = v.to_str() else { return false };
    let mut parts = v.split(';');
    parts
        .next()
        .is_some_and(|t| t.trim().eq_ignore_ascii_case("application/json"))
        && parts.all(|p| match p.split_once('=') {
            Some((k, v)) if k.trim().eq_ignore_ascii_case("charset") => {
                v.trim().trim_matches('"').eq_ignore_ascii_case("utf-8")
            }
            _ => true,
        })
}

/// Reads the body. The buffer never grows past `max`: it is allocated at the
/// declared size, or doubled up to `max` when the size is not known, and the
/// bytes of a frame that would pass `max` are never copied.
async fn read(mut body: Body, declared: Option<usize>, max: usize) -> Result<Bytes, ApiError> {
    let mut buf = Vec::with_capacity(declared.unwrap_or(0));
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| ApiError::bad("the request body could not be read"))?;
        let Ok(data) = frame.into_data() else {
            continue;
        };
        if data.len() > max - buf.len() {
            return Err(too_large(max));
        }
        let want = buf.len() + data.len();
        if want > buf.capacity() {
            buf.reserve_exact((buf.capacity() * 2).clamp(want, max) - buf.len());
        }
        buf.extend_from_slice(&data);
    }
    Ok(Bytes::from(buf))
}

#[cfg(test)]
mod tests;
