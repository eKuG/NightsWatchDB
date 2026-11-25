//! Lazy walker versus prost, and batch bounds. Requests are hand-encoded so they can carry
//! unknown fields at every level and fields out of order.
use crate::convert::*;

fn varint(mut v: u64) -> Vec<u8> {
    let mut o = vec![];
    while v >= 0x80 {
        o.push((v as u8) | 0x80);
        v >>= 7;
    }
    o.push(v as u8);
    o
}
fn ld(f: u32, b: &[u8]) -> Vec<u8> {
    [varint((f as u64) << 3 | 2), varint(b.len() as u64), b.to_vec()].concat()
}
fn vi(f: u32, v: u64) -> Vec<u8> {
    [varint((f as u64) << 3), varint(v)].concat()
}
fn fx(f: u32, v: u64) -> Vec<u8> {
    [varint((f as u64) << 3 | 1), v.to_le_bytes().to_vec()].concat()
}
fn cat(p: &[Vec<u8>]) -> Vec<u8> {
    p.concat()
}
fn any_s(s: &[u8]) -> Vec<u8> {
    ld(1, s)
}
fn kv(k: &str, any: Vec<u8>) -> Vec<u8> {
    cat(&[ld(1, k.as_bytes()), ld(2, &any)])
}
fn attr_s(k: &str, v: &[u8]) -> Vec<u8> {
    ld(9, &kv(k, any_s(v)))
}
fn attr_i(k: &str, v: i64) -> Vec<u8> {
    ld(9, &kv(k, vi(3, v as u64)))
}

fn text(n: usize) -> Vec<u8> {
    // multi-byte characters so the 1 MiB value is real UTF-8 with a boundary at the end
    "héllo wörld ✓ ".repeat(n / 18 + 1).bytes().take(n).collect()
}

fn good_text(n: usize) -> Vec<u8> {
    let mut t = text(n);
    while std::str::from_utf8(&t).is_err() {
        t.pop();
    }
    t
}

fn request() -> Vec<u8> {
    let big = good_text(1 << 20);
    let span_a = cat(&[
        ld(1, &[1; 16]),
        ld(2, &[2; 8]),
        ld(4, &[3; 8]),
        ld(5, b"chat gpt"),
        vi(6, 3),
        fx(7, 1_000),
        fx(8, 2_000),
        ld(15, &cat(&[ld(2, b"oops"), vi(3, 2)])),
        attr_s("gen_ai.operation.name", b"chat"),
        attr_s("gen_ai.provider.name", b"acme"),
        attr_s("gen_ai.request.model", b"m-1"),
        attr_s("gen_ai.conversation.id", b"c1"),
        attr_s("session.id", b"s1"),
        attr_s("user.id", b"u1"),
        attr_s("gen_ai.agent.name", b"agent"),
        attr_s("gen_ai.tool.name", b"tool"),
        attr_s("error.type", b"timeout"),
        attr_i("gen_ai.usage.input_tokens", 100),
        attr_i("gen_ai.usage.output_tokens", 7),
        attr_s("gen_ai.input.messages", &big),
        attr_s("gen_ai.output.messages", b"out"),
        attr_s("gen_ai.system_instructions", b"sys"),
        attr_s("gen_ai.tool.definitions", b"[]"),
        attr_s("gen_ai.tool.call.arguments", b"{}"),
        attr_s("gen_ai.tool.call.result", b"ok"),
        attr_s("other.one", b"x"),
        attr_i("other.two", 5),
        // key with a non-string value, then a repeated key: the last occurrence wins in both paths
        ld(9, &kv("session.id", vi(3, 9))),
        attr_s("user.id", b"u2"),
        // unknown fields inside an attribute and inside its value
        ld(9, &cat(&[ld(1, b"gen_ai.agent.name"), ld(2, &cat(&[any_s(b"agent2"), vi(50, 1)])), vi(51, 7)])),
        ld(11, &ld(2, b"event")),
        vi(99, 1),
    ]);
    let span_b = cat(&[ld(1, &[4; 16]), ld(2, &[5; 8]), ld(5, b"bare")]);
    let span_c = cat(&[
        ld(1, &[6; 16]),
        ld(2, &[7; 8]),
        ld(5, b"c"),
        attr_s("gen_ai.usage.input_tokens", b"12"),
        ld(9, &kv("gen_ai.usage.output_tokens", cat(&[vi(3, 4), fx(4, 0)]))),
        attr_s("gen_ai.tool.call.result", b""),
    ]);
    // resource follows the scope spans on the wire, with a leading attribute that is not service.name
    let rs1 = cat(&[ld(2, &cat(&[ld(1, &ld(1, b"scope")), ld(2, &span_a), ld(2, &span_b), vi(77, 1)])), ld(3, b"url"), ld(1, &cat(&[ld(1, &kv("host", any_s(b"h"))), ld(1, &kv("service.name", any_s(b"svc-a")))])), vi(88, 3)]);
    let rs2 = ld(2, &ld(2, &span_c));
    cat(&[ld(1, &rs1), vi(99, 5), ld(1, &rs2)])
}

fn run(f: fn(&[u8], &mut TraceAcc) -> Result<(), String>, req: &[u8]) -> TraceAcc {
    let mut a = TraceAcc::new(MAX_ROWS, MAX_BYTES);
    a.kept = Some(vec![]);
    f(req, &mut a).unwrap();
    a.close().unwrap();
    a
}

#[test]
fn lazy_equals_full() {
    let req = request();
    let (full, lazy) = (run(full_traces, &req), run(lazy_traces, &req));
    assert_eq!(full.kept, lazy.kept, "batches differ");
    assert_eq!(full.check, lazy.check);
    let c = &lazy.check;
    assert_eq!(c.rows, 3);
    assert_eq!((c.in_tok, c.out_tok), (100, 7));
    assert_eq!(c.nonnull, [1, 1, 1, 1, 1, 2]);
    assert_eq!(c.bytes[0], good_text(1 << 20).len() as u64);
    assert_eq!(c.bytes[5], 2);
    let (s, p) = &lazy.kept.as_ref().unwrap()[0];
    use arrow::array::*;
    let svc = s.column_by_name("service_name").unwrap().as_string::<i32>();
    assert_eq!((svc.value(0), svc.value(1), svc.is_null(2)), ("svc-a", "svc-a", true));
    let sess = s.column_by_name("session_id").unwrap().as_string::<i32>();
    assert!(sess.is_null(0), "last occurrence (an int) wins and is not a string");
    let user = s.column_by_name("user_id").unwrap().as_string::<i32>();
    assert_eq!(user.value(0), "u2");
    let agent = s.column_by_name("agent_name").unwrap().as_string::<i32>();
    assert_eq!(agent.value(0), "agent2");
    let other = s.column_by_name("other_attr_count").unwrap().as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(other.values().to_vec(), vec![2, 0, 0]);
    let par = s.column_by_name("parent_span_id").unwrap();
    assert_eq!(par.null_count(), 2);
    let status = s.column_by_name("status_code").unwrap().as_primitive::<arrow::datatypes::Int32Type>();
    assert_eq!(status.values().to_vec(), vec![2, 0, 0]);
    assert_eq!(p.num_rows(), 3);
}

#[test]
fn bad_input_rejected_by_both() {
    let req = request();
    for cut in [req.len() - 3, 5] {
        assert!(full_traces(&req[..cut], &mut TraceAcc::new(MAX_ROWS, MAX_BYTES)).is_err());
        assert!(lazy_traces(&req[..cut], &mut TraceAcc::new(MAX_ROWS, MAX_BYTES)).is_err());
    }
    // invalid UTF-8 in a payload value
    let span = cat(&[ld(1, &[1; 16]), ld(2, &[2; 8]), attr_s("gen_ai.input.messages", &[0xff, 0xfe])]);
    let bad = ld(1, &ld(2, &ld(2, &span)));
    assert!(full_traces(&bad, &mut TraceAcc::new(MAX_ROWS, MAX_BYTES)).is_err());
    assert!(lazy_traces(&bad, &mut TraceAcc::new(MAX_ROWS, MAX_BYTES)).is_err());
    // wrong id length
    let short = ld(1, &ld(2, &ld(2, &ld(1, &[1; 3]))));
    assert!(full_traces(&short, &mut TraceAcc::new(MAX_ROWS, MAX_BYTES)).is_err());
    assert!(lazy_traces(&short, &mut TraceAcc::new(MAX_ROWS, MAX_BYTES)).is_err());
}

fn row(pay: &[u8]) -> SpanRow<'_> {
    SpanRow { trace_id: &[1; 16], span_id: &[2; 8], name: "n", pay: [Some(pay), None, None, None, None, None], ..Default::default() }
}

#[test]
fn closes_on_row_bound() {
    let mut a = TraceAcc::new(MAX_ROWS, MAX_BYTES);
    a.kept = Some(vec![]);
    for _ in 0..MAX_ROWS + 1 {
        a.push(&row(b"x")).unwrap();
    }
    assert_eq!(a.kept.as_ref().unwrap().len(), 1);
    assert_eq!(a.kept.as_ref().unwrap()[0].0.num_rows(), MAX_ROWS);
    assert_eq!(a.open_rows(), 1);
}

#[test]
fn closes_on_byte_bound() {
    let mut a = TraceAcc::new(MAX_ROWS, MAX_BYTES);
    a.kept = Some(vec![]);
    let big = vec![b'a'; 10 << 20];
    for _ in 0..7 {
        a.push(&row(&big)).unwrap();
    }
    a.close().unwrap();
    let sizes: Vec<(usize, u64)> = a.kept.as_ref().unwrap().iter().map(|(s, p)| (s.num_rows(), p.column(2).as_ref().to_data().buffers()[1].len() as u64)).collect();
    // 3 x 10 MiB fit under 32 MiB, a fourth would not
    assert_eq!(sizes.iter().map(|s| s.0).collect::<Vec<_>>(), vec![3, 3, 1]);
    assert!(sizes.iter().all(|s| s.1 <= MAX_BYTES as u64));
    // a single value above the bound still gets a batch of its own
    let huge = vec![b'a'; MAX_BYTES + 1];
    let mut b = TraceAcc::new(MAX_ROWS, MAX_BYTES);
    b.push(&row(&huge)).unwrap();
    assert_eq!(b.batches, 1);
    assert_eq!(b.check.rows, 1);
}
