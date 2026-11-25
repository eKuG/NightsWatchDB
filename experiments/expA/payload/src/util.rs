//! Shared types, JSON output, process statistics, the check file and the verifier.
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::Path;
use std::time::Instant;

pub type R<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type Fields = Vec<(&'static str, String)>;

/// Payload attributes in table (and hashing) order.
pub const ATTRS: [&str; 6] = ["input_messages", "output_messages", "system_instructions", "tool_definitions", "tool_call_arguments", "tool_call_result"];
pub const ATTR_KEYS: [&str; 6] = ["gen_ai.input.messages", "gen_ai.output.messages", "gen_ai.system_instructions", "gen_ai.tool.definitions", "gen_ai.tool.call.arguments", "gen_ai.tool.call.result"];

pub struct SpanRec<'a> {
    pub trace: [u8; 16],
    pub span: [u8; 8],
    pub start_ns: u64,
    pub vals: [Option<&'a [u8]>; 6],
}

/// A payload store being written. Spans arrive in input order, grouped by trace.
pub trait Builder {
    fn begin_trace(&mut self, id: &[u8; 16], start_ns: u64) -> R<()>;
    fn span(&mut self, s: &SpanRec) -> R<()>;
    fn end_trace(&mut self) -> R<()>;
    /// Closes the files; returns (file kind, bytes on disk) and variant specific fields.
    fn finish(&mut self) -> R<(Vec<(String, u64)>, Fields)>;
}

/// Receiver of values read back from a store.
pub trait Visit {
    fn trace(&mut self, id: &[u8; 16]);
    fn value(&mut self, v: &[u8]);
}

/// A payload store opened for reading. Everything is opened and indexed in the constructor.
pub trait Store {
    fn rows(&self) -> u64;
    /// All values of every span of the trace, in row and attribute order. False if the trace is unknown.
    fn read_trace(&mut self, id: &[u8; 16], v: &mut dyn Visit) -> R<bool>;
    /// The values of one span (global row number).
    fn read_span(&mut self, row: u64, v: &mut dyn Visit) -> R<()>;
    /// Every value once, in row order; `trace` is called when a new trace starts.
    fn scan(&mut self, v: &mut dyn Visit) -> R<()>;
}

// ---- little-endian helpers ----

pub fn le32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}
pub fn le64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().expect("8 bytes"))
}

pub fn hex(id: &[u8]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn unhex(s: &str) -> R<[u8; 16]> {
    if s.len() != 32 || !s.is_ascii() {
        return Err(format!("bad trace id {s:?}").into());
    }
    let mut o = [0u8; 16];
    for i in 0..16 {
        o[i] = u8::from_str_radix(&s[2 * i..2 * i + 2], 16)?;
    }
    Ok(o)
}

/// Ids at positions 0, k, 2k, ... with k = max(1, distinct / n), at most n of them.
pub fn sample<T>(ids: &[T], n: usize) -> impl Iterator<Item = &T> {
    ids.iter().step_by((ids.len() / n.max(1)).max(1)).take(n)
}

/// Seeded generator (splitmix64).
pub struct Rng(pub u64);
impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

// ---- check.bin: per trace in arrival order, trace id (16), sha256 (32), byte count (u64 LE) ----

pub const CHECK_REC: usize = 56;

pub struct Check {
    pub order: Vec<[u8; 16]>,
    pub map: HashMap<[u8; 16], ([u8; 32], u64)>,
}

pub fn load_check(dir: &Path) -> R<Check> {
    let mut b = vec![];
    std::fs::File::open(dir.join("check.bin"))?.read_to_end(&mut b)?;
    if b.len() % CHECK_REC != 0 {
        return Err("check.bin has a partial record".into());
    }
    let (mut order, mut map) = (vec![], HashMap::new());
    for r in b.chunks_exact(CHECK_REC) {
        let id: [u8; 16] = r[..16].try_into()?;
        order.push(id);
        map.insert(id, (r[16..48].try_into()?, le64(r, 48)));
    }
    Ok(Check { order, map })
}

/// Hashes the values of one trace and compares with check.bin.
pub struct Verifier<'a> {
    check: &'a HashMap<[u8; 16], ([u8; 32], u64)>,
    cur: Option<([u8; 16], Sha256, u64)>,
    pub mismatches: u64,
    pub bytes: u64,
    pub traces: u64,
}

impl<'a> Verifier<'a> {
    pub fn new(check: &'a HashMap<[u8; 16], ([u8; 32], u64)>) -> Self {
        Self { check, cur: None, mismatches: 0, bytes: 0, traces: 0 }
    }
    pub fn end(&mut self) {
        if let Some((id, h, n)) = self.cur.take() {
            let d: [u8; 32] = h.finalize().into();
            match self.check.get(&id) {
                Some((s, c)) if *s == d && *c == n => {}
                _ => {
                    eprintln!("mismatch: trace {}", hex(&id));
                    self.mismatches += 1
                }
            }
            self.traces += 1;
        }
    }
}

impl Visit for Verifier<'_> {
    fn trace(&mut self, id: &[u8; 16]) {
        self.end();
        self.cur = Some((*id, Sha256::new(), 0));
    }
    fn value(&mut self, v: &[u8]) {
        if let Some((_, h, n)) = self.cur.as_mut() {
            h.update(v);
            *n += v.len() as u64;
        }
        self.bytes += v.len() as u64;
    }
}

// ---- JSON output and process statistics ----

pub fn q(s: &str) -> String {
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

pub fn obj(f: &[(&str, String)]) -> String {
    format!("{{{}}}", f.iter().map(|(k, v)| format!("{}:{}", q(k), v)).collect::<Vec<_>>().join(","))
}

pub fn named_u64(names: &[&str], v: &[u64]) -> String {
    obj(&names.iter().zip(v).map(|(n, x)| (*n, x.to_string())).collect::<Vec<_>>())
}

/// utime and stime of this process from /proc, in seconds.
fn cpu_times() -> (f64, f64) {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let f: Vec<&str> = s.rsplit_once(')').map_or("", |x| x.1).split_whitespace().collect();
    let t = |i: usize| f.get(i).and_then(|x| x.parse::<f64>().ok()).unwrap_or(0.0) / 100.0;
    (t(11), t(12))
}

fn peak_rss() -> u64 {
    std::fs::read_to_string("/proc/self/status").ok().and_then(|s| s.lines().find_map(|l| l.strip_prefix("VmHWM:")?.trim().strip_suffix("kB")?.trim().parse::<u64>().ok())).map_or(0, |k| k * 1024)
}

pub fn finish(task: &str, variant: &str, t0: Instant, threads: usize, f: Fields) {
    let (u, s) = cpu_times();
    let cg = std::fs::read_to_string("/sys/fs/cgroup/memory.peak").ok().and_then(|s| s.trim().parse::<u64>().ok()).map_or("null".to_string(), |v| v.to_string());
    let mut all: Fields = vec![
        ("task", q(task)),
        ("variant", q(variant)),
        ("wall_s", format!("{:.3}", t0.elapsed().as_secs_f64())),
        ("user_cpu_s", format!("{u:.2}")),
        ("sys_cpu_s", format!("{s:.2}")),
        ("peak_rss_bytes", peak_rss().to_string()),
        ("cgroup_peak_bytes", cg),
        ("threads", threads.to_string()),
    ];
    all.extend(f);
    println!("{}", obj(&all));
}

/// Latency list as p50/p90/p99/max by nearest rank.
pub fn latency(mut ms: Vec<f64>) -> Fields {
    ms.sort_by(f64::total_cmp);
    let p = |p: f64| if ms.is_empty() { 0.0 } else { ms[((p / 100.0 * ms.len() as f64).ceil() as usize).clamp(1, ms.len()) - 1] };
    vec![("p50_ms", format!("{:.3}", p(50.0))), ("p90_ms", format!("{:.3}", p(90.0))), ("p99_ms", format!("{:.3}", p(99.0))), ("max_ms", format!("{:.3}", p(100.0)))]
}

/// Counts the written bytes.
pub struct Counting<W: Write> {
    pub inner: W,
    pub n: u64,
}
impl<W: Write> Write for Counting<W> {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        let k = self.inner.write(b)?;
        self.n += k as u64;
        Ok(k)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}
