//! Synthetic end-to-end tests: build all layouts from one small input and compare them.
use crate::query::{self, Layout};
use crate::{build, Variant};
use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use prost::Message;
use std::collections::HashMap;
use std::io::Write;
use std::path::PathBuf;

const BASE: i64 = 1_700_000_000 * 1_000_000_000;
const WINDOW_S: u64 = 10;

fn kv(k: &str, v: Value) -> KeyValue {
    KeyValue { key: k.into(), value: Some(AnyValue { value: Some(v) }), ..Default::default() }
}

struct Info {
    id: [u8; 16],
    start: i64,
    svc: &'static str,
    spans: usize,
    err: bool,
    models: Vec<&'static str>,
}

/// 600 traces, one second apart (pairs share a start time), several spans each, spread over many
/// 10 s windows; ids are unrelated to time order; requests of 13 spans cut traces in the middle.
fn synth(path: &std::path::Path) -> Vec<Info> {
    let svcs = ["alpha", "beta", "gamma"];
    let models = ["m-small", "m-large", "m-mid"];
    let mut infos = vec![];
    let mut spans: Vec<(Span, &str)> = vec![];
    for i in 0..600usize {
        let id = ((i as u128 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15_F39C_C060_5CED_C835)).to_be_bytes();
        let start = BASE + (i as i64 / 2) * 1_000_000_000;
        let svc = svcs[if i % 5 == 0 { 0 } else { i % 3 }];
        let mk = |n: u8, parent: bool, off: i64, attrs: Vec<KeyValue>| Span {
            trace_id: id.to_vec(),
            span_id: vec![n; 8],
            parent_span_id: if parent { vec![0; 8] .iter().map(|_| 1u8).collect() } else { vec![] },
            name: format!("span-{n}"),
            start_time_unix_nano: (start + off) as u64,
            end_time_unix_nano: (start + off + 5_000_000) as u64,
            attributes: attrs,
            ..Default::default()
        };
        spans.push((mk(1, false, 0, vec![kv("gen_ai.operation.name", Value::StringValue("invoke_agent".into())), kv("session.id", Value::StringValue(format!("s{}", i / 4)))]), svc));
        let mut tr = Info { id, start, svc, spans: 1, err: false, models: vec![] };
        for j in 0..(2 + i % 4) {
            let m = models[(i + j) % 3];
            let mut a = vec![
                kv("gen_ai.operation.name", Value::StringValue("chat".into())),
                kv("gen_ai.request.model", Value::StringValue(m.into())),
                kv("gen_ai.usage.input_tokens", Value::IntValue((10 * j + i) as i64)),
                kv("gen_ai.usage.output_tokens", Value::IntValue(j as i64 + 1)),
                kv("gen_ai.input.messages", Value::StringValue("payload".into())),
                kv("other.thing", Value::BoolValue(true)),
            ];
            if i % 7 == 0 && j == 0 {
                a.push(kv("error.type", Value::StringValue("timeout".into())));
                tr.err = true;
            }
            if !tr.models.contains(&m) {
                tr.models.push(m);
            }
            spans.push((mk(2 + j as u8, true, 1_000_000 * (j as i64 + 1), a), svc));
            tr.spans += 1;
        }
        spans.push((mk(20, true, 9_000_000, vec![kv("gen_ai.tool.name", Value::StringValue("search".into()))]), "tools"));
        tr.spans += 1;
        infos.push(tr);
    }
    let mut f = std::fs::File::create(path).unwrap();
    for chunk in spans.chunks(13) {
        let mut rs: Vec<ResourceSpans> = vec![];
        for (s, svc) in chunk {
            if rs.last().is_none_or(|r| r.resource.as_ref().unwrap().attributes[0].value.as_ref().unwrap().value != Some(Value::StringValue(svc.to_string()))) {
                rs.push(ResourceSpans { resource: Some(Resource { attributes: vec![kv("service.name", Value::StringValue(svc.to_string()))], ..Default::default() }), scope_spans: vec![ScopeSpans::default()], ..Default::default() });
            }
            rs.last_mut().unwrap().scope_spans[0].spans.push(s.clone());
        }
        let buf = ExportTraceServiceRequest { resource_spans: rs }.encode_to_vec();
        f.write_all(&(buf.len() as u32).to_be_bytes()).unwrap();
        f.write_all(&buf).unwrap();
    }
    infos
}

fn tmp(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("layout-test-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
fn segments_never_split_a_trace() {
    let dir = tmp("seg");
    let input = dir.join("in.bin");
    let infos = synth(&input);
    let out = dir.join("cotrace");
    let r = build::run(&input, &out, Variant::Cotrace, WINDOW_S, 2, 256).unwrap();
    assert_eq!(r.traces, 600);
    assert!(r.segments > 20, "segments: {}", r.segments);
    assert_eq!(r.rows as usize, infos.iter().map(|i| i.spans).sum::<usize>());
    assert_eq!(r.notes[0].1, "0"); // no multi-root traces
    let want: HashMap<[u8; 16], usize> = infos.iter().map(|i| (i.id, i.spans)).collect();
    let mut seen: HashMap<[u8; 16], (usize, usize)> = HashMap::new(); // trace -> (segment, rows)
    let mut seg_window = vec![];
    for s in 0..r.segments as usize {
        let rdr = ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(out.join(format!("seg-{s:05}.parquet"))).unwrap()).unwrap().build().unwrap();
        let (mut last, mut windows) = (None, std::collections::BTreeSet::new());
        let mut closed = std::collections::HashSet::new();
        for b in rdr {
            let b = b.unwrap();
            let (t, st) = (b.column_by_name("trace_id").unwrap().as_fixed_size_binary(), b.column_by_name("start_ns").unwrap().as_primitive::<Int64Type>());
            for i in 0..b.num_rows() {
                let id: [u8; 16] = t.value(i).try_into().unwrap();
                if last != Some(id) {
                    assert!(closed.insert(id), "trace re-appears inside segment {s}");
                    last = Some(id);
                }
                let e = seen.entry(id).or_insert((s, 0));
                assert_eq!(e.0, s, "trace split across segments");
                e.1 += 1;
                if st.value(i) >= 0 && infos.iter().any(|x| x.id == id && x.start == st.value(i)) {
                    windows.insert(st.value(i).div_euclid(WINDOW_S as i64 * 1_000_000_000));
                }
            }
        }
        assert_eq!(windows.len(), 1, "one window per segment");
        seg_window.push(*windows.first().unwrap());
    }
    assert!(seg_window.windows(2).all(|w| w[0] < w[1]));
    assert_eq!(seen.len(), 600);
    for (id, (_, n)) in seen {
        assert_eq!(want[&id], n);
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn layouts_agree_and_match_brute_force() {
    let dir = tmp("agree");
    let input = dir.join("in.bin");
    let infos = synth(&input);
    let cases = [("cotrace", Variant::Cotrace, 256), ("svc", Variant::Svc, 0), ("traceid", Variant::Traceid, 0), ("traceid-1run", Variant::Traceid, 256), ("svc-1run", Variant::Svc, 256)];
    let layouts: Vec<Layout> = cases
        .iter()
        .map(|(n, v, mb)| {
            let out = dir.join(n);
            build::run(&input, &out, *v, WINDOW_S, 2, *mb).unwrap();
            assert!(!out.join(".runs").exists());
            Layout::open(&out, *v).unwrap()
        })
        .collect();
    let p0 = query::params(&layouts[0], 2).unwrap();
    for l in &layouts {
        let p = query::params(l, 2).unwrap();
        assert_eq!((&p.service, &p.model, p.from, p.to), (&p0.service, &p0.model, p0.from, p0.to));
    }
    // brute force from the generator's own description
    let mut w1: Vec<_> = infos.iter().filter(|i| i.svc == p0.service && i.start >= p0.from && i.start < p0.to).collect();
    let mut w2: Vec<_> = infos.iter().filter(|i| i.err && i.models.contains(&p0.model.as_str()) && i.start >= p0.from && i.start < p0.to).collect();
    for w in [&mut w1, &mut w2] {
        w.sort_by(|a, b| b.start.cmp(&a.start).then(a.id.cmp(&b.id)));
        w.truncate(50);
    }
    assert_eq!(w1.len(), 50);
    assert!(!w2.is_empty());
    let sha = |w: &Vec<&Info>| query::result_sha256(&w.iter().map(|i| query::Item { id: i.id, start: 0, dur: 0, name: String::new() }).collect::<Vec<_>>());
    let agg0 = format!("{:?}", query::agg(&layouts[0], &p0, 2).unwrap());
    assert!(agg0.contains("m-small"));
    let (_, ids0) = query::sample_ids(&layouts[0], 50, 2).unwrap();
    assert_eq!(ids0.len(), 50);
    for (l, (name, _, _)) in layouts.iter().zip(cases) {
        assert_eq!(query::result_sha256(&query::list1(l, &p0, 2).unwrap()), sha(&w1), "list1 {name}");
        assert_eq!(query::result_sha256(&query::list2(l, &p0, 2).unwrap()), sha(&w2), "list2 {name}");
        assert_eq!(format!("{:?}", query::agg(l, &p0, 2).unwrap()), agg0, "agg {name}");
        assert_eq!(query::sample_ids(l, 50, 2).unwrap().1, ids0, "sample-ids {name}");
    }
    // open: every trace, every layout, same rows
    for info in &infos {
        let base = query::digest(&layouts[0].open_trace(&info.id).unwrap().unwrap());
        assert_eq!(base.0, info.spans);
        for (l, (name, _, _)) in layouts.iter().zip(cases) {
            let b = l.open_trace(&info.id).unwrap().unwrap();
            assert_eq!(query::digest(&b), base, "open {name}");
            let st = b.column_by_name("start_ns").unwrap().as_primitive::<Int64Type>();
            assert!((1..st.len()).all(|i| st.value(i - 1) <= st.value(i)), "ordered by start_ns");
        }
    }
    assert!(layouts[0].open_trace(&[7; 16]).unwrap().is_none());
    assert!(layouts[2].open_trace(&[7; 16]).unwrap().is_none());
    assert!(layouts[1].open_trace(&[7; 16]).unwrap().is_none());
    let _ = std::fs::remove_dir_all(dir);
}
