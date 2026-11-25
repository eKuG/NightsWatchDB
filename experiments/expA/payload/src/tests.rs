use crate::util::*;
use crate::*;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use prost::Message;

type Vals = [Option<Vec<u8>>; 6];
type Row = (u64, [u8; 16], [u8; 8], Vals);

fn text(rng: &mut Rng, n: usize) -> String {
    (0..n).map(|_| (b'a' + (rng.next() % 26) as u8) as char).collect()
}

/// Spans of a few traces in three 900 s windows, with repeated and tricky values.
fn synthetic(rng: &mut Rng) -> Vec<Row> {
    let sys = text(rng, 3000);
    let tools = format!("[{{\"name\":\"t1\",\"d\":\"{}\"}},{{\"name\":\"t2\"}}]", text(rng, 500));
    let big = text(rng, 1_500_000); // larger than a block
    let mid: Vec<String> = (0..3).map(|_| text(rng, 600_000)).collect(); // forces block flushes
    let tricky = ["{\"role\":\"user\",\"c\":\"a,b]c[d\\\"e\"}".to_string(), "\"plain, string\"".into(), " {\"nested\":[1,[2,{\"x\":\"}\"}]]} ".into(), "{\"q\":\"\\\\\"}".into()];
    let mut out = vec![];
    // (start seconds, span count) per trace
    for (t, (secs, n)) in [(0u64, 3usize), (100, 4), (1000, 2), (1100, 3), (2000, 2), (2050, 1)].into_iter().enumerate() {
        let tid = [t as u8 + 1; 16];
        for s in 0..n {
            let mut v: Vals = Default::default();
            let hist: Vec<&str> = tricky.iter().map(String::as_str).take(1 + (s + t) % 4).collect();
            v[0] = Some(format!("[{}]", hist.join(",")).into_bytes());
            v[1] = Some(match (t + s) % 6 {
                0 => b"[]".to_vec(),
                1 => b"not json at all".to_vec(),
                2 => b"".to_vec(),
                3 => b" [1,2]".to_vec(),
                4 => format!("[{}]", mid.join(",")).into_bytes(),
                _ => format!("[{{\"k\":\"{big}\"}}]").into_bytes(),
            });
            if s % 2 == 0 {
                v[2] = Some(sys.clone().into_bytes());
                v[3] = Some(tools.clone().into_bytes());
            }
            if s == 1 {
                v[4] = Some(b"{\"arg\":1}".to_vec());
                v[5] = Some(big.clone().into_bytes());
            }
            if t == 5 {
                v = Default::default(); // a trace with no payload values at all
            }
            out.push((secs * 1_000_000_000 + s as u64, tid, [s as u8; 8], v));
        }
    }
    out
}

fn write_input(path: &Path, spans: &[Row]) {
    let mut f = std::fs::File::create(path).unwrap();
    // two spans per request, so traces straddle request boundaries
    for chunk in spans.chunks(2) {
        let sp: Vec<Span> = chunk
            .iter()
            .map(|(start, tid, sid, v)| Span {
                trace_id: tid.to_vec(),
                span_id: sid.to_vec(),
                start_time_unix_nano: *start,
                attributes: v
                    .iter()
                    .enumerate()
                    .filter_map(|(i, x)| x.as_ref().map(|x| KeyValue { key: ATTR_KEYS[i].into(), key_strindex: 0, value: Some(AnyValue { value: Some(Value::StringValue(String::from_utf8(x.clone()).unwrap())) }) }))
                    .collect(),
                ..Default::default()
            })
            .collect();
        let req = ExportTraceServiceRequest { resource_spans: vec![ResourceSpans { scope_spans: vec![ScopeSpans { spans: sp, ..Default::default() }], ..Default::default() }] };
        let b = req.encode_to_vec();
        f.write_all(&(b.len() as u32).to_be_bytes()).unwrap();
        f.write_all(&b).unwrap();
    }
}

struct Collect(Vec<Vec<u8>>);
impl Visit for Collect {
    fn trace(&mut self, _: &[u8; 16]) {}
    fn value(&mut self, v: &[u8]) {
        self.0.push(v.to_vec());
    }
}

fn field<'a>(f: &'a Fields, k: &str) -> &'a str {
    &f.iter().find(|x| x.0 == k).unwrap().1
}

fn round_trip(variant: &str, tune: &str) -> Built {
    let mut rng = Rng(7);
    let spans = synthetic(&mut rng);
    let dir = std::env::temp_dir().join(format!("payload-test-{}-{variant}-{}", std::process::id(), tune.len()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    write_input(&dir.join("in.bin"), &spans);
    let mut b = make_builder(variant, &dir, 900, tune).unwrap();
    let built = build_into(&dir.join("in.bin"), &dir, b.as_mut()).unwrap();
    assert_eq!(built.rows, spans.len() as u64);
    assert_eq!(built.traces, 6);

    let check = load_check(&dir).unwrap();
    let mut store = open_store(variant, &dir).unwrap();
    assert_eq!(store.rows(), spans.len() as u64);
    // open: every trace verifies against check.bin
    let mut v = Verifier::new(&check.map);
    for id in &check.order {
        v.trace(id);
        assert!(store.read_trace(id, &mut v).unwrap());
        v.end();
    }
    assert_eq!((v.mismatches, v.traces), (0, 6));
    assert!(!store.read_trace(&[99; 16], &mut v).unwrap());
    // span: every row returns exactly its values
    for (row, s) in spans.iter().enumerate() {
        let mut c = Collect(vec![]);
        store.read_span(row as u64, &mut c).unwrap();
        let want: Vec<Vec<u8>> = s.3.iter().flatten().cloned().collect();
        assert!(c.0 == want, "row {row} differs");
    }
    // scan
    let mut v = Verifier::new(&check.map);
    store.scan(&mut v).unwrap();
    v.end();
    assert_eq!((v.mismatches, v.traces), (0, 6));
    assert_eq!(v.bytes, built.raw.iter().sum::<u64>());
    drop(store);
    std::fs::remove_dir_all(&dir).unwrap();
    built
}

#[test]
fn parquet_round_trip() {
    round_trip("parquet", "");
}

#[test]
fn parquet_tuned_round_trip() {
    round_trip("parquet-tuned", "");
    // small row groups and pages so selections cross row group and page boundaries
    round_trip("parquet-tuned", "rg_rows=4,rg_mb=1,page_kb=64,write_batch=1");
}

#[test]
fn zstdlong_round_trip() {
    round_trip("zstdlong", "");
}

#[test]
fn dedup_round_trip_and_stats() {
    let b = round_trip("dedup", "");
    assert_eq!(field(&b.extra, "hash_collisions"), "0");
    assert_eq!(field(&b.extra, "scopes"), "3");
    // output_messages that are not arrays ("not json at all", "", " [1,2]") appear in several rows
    let fb: u64 = field(&b.extra, "whole_fallbacks").parse().unwrap();
    assert!(fb >= 3, "fallbacks {fb}");
    let ratio = field(&b.extra, "scope_ratio").to_string();
    let get = |k: &str| -> f64 {
        let i = ratio.find(k).unwrap() + k.len() + 1;
        ratio[i..].split([',', '}']).next().unwrap().parse().unwrap()
    };
    // a wider scope can only see more repetition
    assert!(get("\"trace\"") >= get("\"segment\"") && get("\"segment\"") >= get("\"dataset\""), "{ratio}");
    assert!(get("\"dataset\"") < get("\"trace\""), "{ratio}");
}

#[test]
fn sampling_rule() {
    let ids: Vec<u32> = (0..10).collect();
    assert_eq!(sample(&ids, 3).copied().collect::<Vec<_>>(), [0, 3, 6]);
    assert_eq!(sample(&ids, 20).copied().collect::<Vec<_>>(), ids);
    assert_eq!(sample(&ids, 1).copied().collect::<Vec<_>>(), [0]);
}

#[test]
fn hex_round_trip() {
    let id = [0xab, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 255];
    assert_eq!(unhex(&hex(&id)).unwrap(), id);
    assert!(unhex("zz").is_err());
}
