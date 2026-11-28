//! The answer to a malformed request is small whatever was sent: a 400 does
//! not repeat the request's text, neither a body's nor a query string's.

mod common;

use nwdb_server::Server;

/// Largest 400 for any request here. A message is a fixed text, at most 160
/// bytes of serde's message and a position, or at most 64 bytes of a name.
const SMALL: usize = 1024;

async fn post(api: std::net::SocketAddr, body: Vec<u8>) -> common::Resp {
    common::http_body(
        api,
        "POST",
        "/api/v1/annotations",
        "Content-Type: application/json\r\n",
        Some(body),
        None,
    )
    .await
}

/// serde_json quotes the offending value (`invalid type: string "..."`, in
/// Rust's debug form, where U+0085 becomes `\u{85}`) and the reply used to
/// escape that again: a 1 MiB request got a reply of about 3.5 MiB. The detail
/// is now cut to a short prefix, with the position.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_400_does_not_repeat_the_body() {
    let dir = tempfile::tempdir().unwrap();
    let server = Server::start(common::opts(dir.path())).await.unwrap();
    let api = server.api_addr();

    // A string of U+0085, just under the 1 MiB limit.
    let mut text = String::from("\"");
    while text.len() + 3 <= 1024 * 1024 {
        text.push('\u{85}');
    }
    text.push('"');
    let item = |field: &str| format!(r#"{{"annotations":[{{"trace_id":"{field}","name":5}}]}}"#);
    let big = "\u{85}".repeat(300_000);
    let cases = [
        ("a string where an object is expected", text.clone()),
        ("an unknown top field with a long name", {
            format!(r#"{{"annotations":[],"{big}":1}}"#)
        }),
        ("an unknown item field with a long name", {
            format!(r#"{{"annotations":[{{"{big}":1}}]}}"#)
        }),
        ("a long string of the wrong type", item(&big)),
    ];
    for (what, body) in cases {
        let sent = body.len();
        let r = post(api, body.into_bytes()).await;
        assert_eq!(r.status, 400, "{what}");
        assert!(
            r.body.len() <= SMALL,
            "{what}: a request of {sent} bytes got a 400 of {} bytes",
            r.body.len()
        );
        // Where the error is, so that a client can find it.
        assert!(r.error().contains("line"), "{what}: {}", r.error());
    }

    // The same for the name of a query parameter, which is percent-decoded
    // and then escaped again (a control character is 3 bytes sent and 6 in
    // the reply).
    let name = "%01".repeat(20_000);
    let r = common::http(api, "GET", &format!("/api/v1/traces?{name}=1"), "").await;
    assert_eq!(r.status, 400);
    assert!(r.error().contains("unknown parameter"), "{}", r.error());
    assert!(r.body.len() <= SMALL, "a 400 of {} bytes", r.body.len());
    server.shutdown().await.unwrap();
}
