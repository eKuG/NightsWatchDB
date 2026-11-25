//! Process-wide counters and the final JSON report.
use crate::ipc::Ctr;
use crate::trace::{Check, PAY_COLS};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Mutex;
use std::time::{Duration, Instant};

#[derive(Default)]
struct Inner {
    first: Option<Instant>,
    last: Option<Instant>,
    check: Check,
    ctr: [u64; 6],
    max_dict_bytes: u64,
    by_type: BTreeMap<i32, (u64, u64)>,
    schemas: BTreeMap<String, u64>,
    encodings: BTreeMap<String, u64>,
}

#[derive(Default)]
pub struct Stats {
    pub requests: AtomicU64,
    pub wire_bytes: AtomicU64,
    pub ipc_bytes: AtomicU64,
    pub spans: AtomicU64,
    pub log_records: AtomicU64,
    pub metric_points: AtomicU64,
    pub batches: AtomicU64,
    pub streams_opened: AtomicU64,
    pub errors: AtomicU64,
    /// streams opened per service: traces, logs, metrics
    pub streams_by_signal: [AtomicU64; 3],
    m: Mutex<Inner>,
}

impl Stats {
    pub fn on_request(&self, wire: u64, ipc: u64) {
        self.requests.fetch_add(1, Relaxed);
        self.wire_bytes.fetch_add(wire, Relaxed);
        self.ipc_bytes.fetch_add(ipc, Relaxed);
        let mut m = self.m.lock().unwrap();
        let now = Instant::now();
        m.first.get_or_insert(now);
        m.last = Some(now);
    }
    pub fn add_encoding(&self, n: &str) {
        *self.m.lock().unwrap().encodings.entry(n.to_string()).or_default() += 1;
    }
    pub fn touch(&self) {
        self.m.lock().unwrap().last = Some(Instant::now());
    }
    pub fn idle_for(&self) -> Option<Duration> {
        let m = self.m.lock().unwrap();
        m.first.and(m.last).map(|l| l.elapsed())
    }
    pub fn add_ipc(&self, c: &Ctr) {
        let mut m = self.m.lock().unwrap();
        for (i, v) in [c.ipc_streams, c.schema_resets, c.dict_batches, c.dict_deltas, c.dict_replacements].iter().enumerate() {
            m.ctr[i] += v;
        }
        m.max_dict_bytes = m.max_dict_bytes.max(c.dict_bytes);
        for (t, d) in &c.schemas {
            *m.schemas.entry(format!("{t}: {d}")).or_default() += 1;
        }
    }
    pub fn add_payload(&self, t: i32, rows: u64) {
        let mut m = self.m.lock().unwrap();
        let e = m.by_type.entry(t).or_default();
        e.0 += 1;
        e.1 += rows;
    }
    pub fn add_check(&self, c: &Check, batches: u64) {
        self.spans.fetch_add(c.rows, Relaxed);
        self.batches.fetch_add(batches, Relaxed);
        self.m.lock().unwrap().check.add(c);
    }
}

fn cpu_times() -> (f64, f64) {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let f: Vec<&str> = s.rsplit_once(')').map_or("", |x| x.1).split_whitespace().collect();
    let t = |i: usize| f.get(i).and_then(|x| x.parse::<f64>().ok()).unwrap_or(0.0) / 100.0;
    (t(11), t(12))
}

fn peak_rss() -> u64 {
    std::fs::read_to_string("/proc/self/status").ok().and_then(|s| s.lines().find_map(|l| l.strip_prefix("VmHWM:")?.trim().strip_suffix("kB")?.trim().parse::<u64>().ok())).map_or(0, |k| k * 1024)
}

fn q(s: &str) -> String {
    let mut o = String::from("\"");
    for c in s.chars() {
        match c {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o + "\""
}

fn named(v: &[u64]) -> String {
    format!("{{{}}}", PAY_COLS.iter().zip(v).map(|(n, x)| format!("{}:{x}", q(n))).collect::<Vec<_>>().join(","))
}

pub fn report(sh: &crate::Shared, wal: &str, decode: crate::Decode) -> String {
    let s = &sh.st;
    let m = s.m.lock().unwrap();
    let (u, sy) = cpu_times();
    let wall = match (m.first, m.last) {
        (Some(a), Some(b)) => b.duration_since(a).as_secs_f64(),
        _ => 0.0,
    };
    let g = |a: &AtomicU64| a.load(Relaxed);
    let (wb, wf) = sh.wal.as_ref().map_or((0, 0), |w| (w.stats.bytes.load(Relaxed), w.stats.fsyncs.load(Relaxed)));
    let types = m.by_type.iter().map(|(t, (b, r))| format!("\"{t}\":{{\"batches\":{b},\"rows\":{r}}}")).collect::<Vec<_>>().join(",");
    let schemas = m.schemas.iter().map(|(k, v)| format!("{}:{v}", q(k))).collect::<Vec<_>>().join(",");
    let enc = m.encodings.iter().map(|(k, v)| format!("{}:{v}", q(k))).collect::<Vec<_>>().join(",");
    let sig = s.streams_by_signal.iter().map(|a| a.load(Relaxed).to_string()).collect::<Vec<_>>().join(",");
    let c = &m.check;
    format!(
        "{{\"door\":\"otap\",\"decode\":\"{}\",\"wal\":{},\"requests\":{},\"wire_bytes\":{},\"arrow_ipc_bytes\":{},\"spans\":{},\"log_records\":{},\"metric_points\":{},\"batches\":{},\"wall_s\":{:.3},\"user_cpu_s\":{:.2},\"sys_cpu_s\":{:.2},\"peak_rss_bytes\":{},\"wal_bytes\":{wb},\"wal_fsyncs\":{wf},\"streams_opened\":{},\"streams_by_signal_traces_logs_metrics\":[{sig}],\"grpc_encodings\":{{{enc}}},\"schema_resets\":{},\"ipc_streams\":{},\"dict_batches\":{},\"dict_deltas\":{},\"dict_replacements\":{},\"max_dict_bytes_per_stream\":{},\"errors\":{},\"payload_types\":{{{types}}},\"schemas\":{{{schemas}}},\"rows\":{},\"sum_input_tokens\":{},\"sum_output_tokens\":{},\"nonnull\":{},\"payload_bytes\":{}}}",
        match decode { crate::Decode::Full => "full", crate::Decode::Ipc => "ipc", crate::Decode::None => "none" },
        q(wal),
        g(&s.requests),
        g(&s.wire_bytes),
        g(&s.ipc_bytes),
        g(&s.spans),
        g(&s.log_records),
        g(&s.metric_points),
        g(&s.batches),
        wall,
        u,
        sy,
        peak_rss(),
        g(&s.streams_opened),
        m.ctr[1],
        m.ctr[0],
        m.ctr[2],
        m.ctr[3],
        m.ctr[4],
        m.max_dict_bytes,
        g(&s.errors),
        c.rows,
        c.in_tok,
        c.out_tok,
        named(&c.nonnull),
        named(&c.bytes),
    )
}
