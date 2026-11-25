//! Shared helpers: error type, argument parsing, JSON output, process statistics, hashing, RNG.
use std::collections::HashMap;
use std::hash::Hasher;
use std::path::PathBuf;
use std::time::Instant;

pub type R<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub type Fields = Vec<(&'static str, String)>;

pub struct Args(pub HashMap<String, String>);

impl Args {
    pub fn parse(v: &[String]) -> R<Self> {
        let mut m = HashMap::new();
        let mut it = v.iter();
        while let Some(k) = it.next() {
            let k = k.strip_prefix("--").ok_or_else(|| format!("unexpected argument {k}"))?;
            m.insert(k.to_string(), it.next().ok_or_else(|| format!("--{k} needs a value"))?.clone());
        }
        Ok(Self(m))
    }
    pub fn get(&self, k: &str) -> R<&str> {
        self.0.get(k).map(String::as_str).ok_or_else(|| format!("--{k} is required").into())
    }
    pub fn num<T: std::str::FromStr>(&self, k: &str, default: T) -> R<T> {
        match self.0.get(k) {
            None => Ok(default),
            Some(s) => s.parse().map_err(|_| format!("--{k}: bad number {s:?}").into()),
        }
    }
    pub fn path(&self, k: &str) -> R<PathBuf> {
        Ok(self.get(k)?.into())
    }
}

pub fn le32(b: &[u8], at: usize) -> u32 {
    u32::from_le_bytes(b[at..at + 4].try_into().expect("4 bytes"))
}
pub fn le64(b: &[u8], at: usize) -> u64 {
    u64::from_le_bytes(b[at..at + 8].try_into().expect("8 bytes"))
}

/// Multiplicative hasher; keys are our own tokens, so flooding resistance is not needed.
#[derive(Default)]
pub struct Fx(u64);
impl Hasher for Fx {
    fn write(&mut self, bytes: &[u8]) {
        for c in bytes.chunks(8) {
            let mut w = [0u8; 8];
            w[..c.len()].copy_from_slice(c);
            self.0 = (self.0.rotate_left(5) ^ u64::from_le_bytes(w)).wrapping_mul(0x517c_c1b7_2722_0a95);
        }
        self.0 = (self.0.rotate_left(5) ^ bytes.len() as u64).wrapping_mul(0x517c_c1b7_2722_0a95);
    }
    fn finish(&self) -> u64 {
        self.0 ^ (self.0 >> 32)
    }
}
pub type FxMap<K, V> = HashMap<K, V, std::hash::BuildHasherDefault<Fx>>;

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

pub fn arr<T>(v: &[T], f: impl Fn(&T) -> String) -> String {
    format!("[{}]", v.iter().map(f).collect::<Vec<_>>().join(","))
}

pub fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// SHA-256 of the row ids as u32 little-endian.
pub fn rows_sha(rows: &[u32]) -> String {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    for r in rows {
        h.update(r.to_le_bytes());
    }
    hex(&h.finalize())
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

/// Prints the summary line: the common fields, then `f`.
pub fn finish(task: &str, variant: &str, t0: Instant, f: Fields) {
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
        ("threads", "1".into()),
    ];
    all.extend(f);
    println!("{}", obj(&all));
}

/// Nearest-rank percentile of a list of milliseconds.
pub fn pct(ms: &[f64], p: f64) -> f64 {
    let mut v = ms.to_vec();
    v.sort_by(f64::total_cmp);
    if v.is_empty() {
        return 0.0;
    }
    v[((p / 100.0 * v.len() as f64).ceil() as usize).clamp(1, v.len()) - 1]
}
