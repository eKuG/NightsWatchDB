mod dd;
mod input;
mod pq;
mod split;
#[cfg(test)]
mod tests;
mod util;
mod zl;

use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Instant;
use util::*;

struct Args(HashMap<String, String>);

impl Args {
    fn parse(v: &[String]) -> R<Self> {
        let mut m = HashMap::new();
        let mut it = v.iter();
        while let Some(k) = it.next() {
            let k = k.strip_prefix("--").ok_or_else(|| format!("unexpected argument {k}"))?;
            m.insert(k.to_string(), it.next().ok_or_else(|| format!("--{k} needs a value"))?.clone());
        }
        Ok(Self(m))
    }
    fn get(&self, k: &str) -> R<&str> {
        self.0.get(k).map(String::as_str).ok_or_else(|| format!("--{k} is required").into())
    }
    fn num<T: std::str::FromStr>(&self, k: &str, default: T) -> R<T> {
        match self.0.get(k) {
            None => Ok(default),
            Some(s) => s.parse().map_err(|_| format!("--{k}: bad number {s:?}").into()),
        }
    }
    fn path(&self, k: &str) -> R<PathBuf> {
        Ok(self.get(k)?.into())
    }
}

pub const VARIANTS: [&str; 4] = ["parquet", "parquet-tuned", "zstdlong", "dedup"];

fn make_builder(variant: &str, out: &Path, window_s: u64, tune: &str) -> R<Box<dyn Builder>> {
    Ok(match variant {
        "parquet" | "parquet-tuned" => {
            let mut o = if variant == "parquet" { pq::PqOpts::plain() } else { pq::PqOpts::tuned() };
            o.apply(tune)?;
            Box::new(pq::PqBuilder::new(out, &o)?)
        }
        "zstdlong" => Box::new(zl::ZlBuilder::new(out)?),
        "dedup" => Box::new(dd::DdBuilder::new(out, window_s)?),
        v => return Err(format!("unknown variant {v}").into()),
    })
}

fn open_store(variant: &str, dir: &Path) -> R<Box<dyn Store>> {
    Ok(match variant {
        "parquet" | "parquet-tuned" => Box::new(pq::PqStore::open(dir)?),
        "zstdlong" => Box::new(zl::ZlStore::open(dir)?),
        "dedup" => Box::new(dd::DdStore::open(dir)?),
        v => return Err(format!("unknown variant {v}").into()),
    })
}

pub struct Built {
    pub rows: u64,
    pub traces: u64,
    pub raw: [u64; 6],
    pub input_bytes: u64,
    pub files: Vec<(String, u64)>,
    pub extra: Fields,
}

/// Reads the input once, feeds the builder, and writes check.bin.
pub fn build_into(input: &Path, out: &Path, b: &mut dyn Builder) -> R<Built> {
    let mut check = std::io::BufWriter::new(std::fs::File::create(out.join("check.bin"))?);
    let mut cur: Option<([u8; 16], Sha256, u64)> = None;
    let (mut rows, mut traces, mut raw) = (0u64, 0u64, [0u64; 6]);
    let mut seen: HashSet<[u8; 16]> = HashSet::new();
    let end = |cur: &mut Option<([u8; 16], Sha256, u64)>, b: &mut dyn Builder, check: &mut dyn Write| -> R<()> {
        if let Some((id, h, n)) = cur.take() {
            b.end_trace()?;
            check.write_all(&id)?;
            check.write_all(&<[u8; 32]>::from(h.finalize()))?;
            check.write_all(&n.to_le_bytes())?;
        }
        Ok(())
    };
    let input_bytes = input::for_each_span(input, |s| {
        if cur.as_ref().map(|c| c.0) != Some(s.trace) {
            end(&mut cur, b, &mut check)?;
            if !seen.insert(s.trace) {
                return Err(format!("trace {} is not contiguous in the input", hex(&s.trace)).into());
            }
            b.begin_trace(&s.trace, s.start_ns)?;
            cur = Some((s.trace, Sha256::new(), 0));
            traces += 1;
        }
        let c = cur.as_mut().ok_or("no trace")?;
        for (i, v) in s.vals.iter().enumerate() {
            if let Some(v) = v {
                c.1.update(v);
                c.2 += v.len() as u64;
                raw[i] += v.len() as u64;
            }
        }
        rows += 1;
        b.span(s)
    })?;
    end(&mut cur, b, &mut check)?;
    check.flush()?;
    let (files, extra) = b.finish()?;
    Ok(Built { rows, traces, raw, input_bytes, files, extra })
}

fn build(a: &Args, variant: &str) -> R<Fields> {
    let (input, out) = (a.path("in")?, a.path("out")?);
    std::fs::create_dir_all(&out)?;
    let t0 = Instant::now();
    let mut b = make_builder(variant, &out, a.num("window-s", 900)?, a.0.get("tune").map_or("", String::as_str))?;
    let r = build_into(&input, &out, b.as_mut())?;
    let secs = t0.elapsed().as_secs_f64();
    let total: u64 = r.files.iter().map(|f| f.1).sum();
    let mut files: Vec<(&str, String)> = r.files.iter().map(|(n, s)| (n.as_str(), s.to_string())).collect();
    files.push(("total", total.to_string()));
    files.push(("check.bin_not_in_total", std::fs::metadata(out.join("check.bin"))?.len().to_string()));
    let mut f: Fields = vec![
        ("input_bytes", r.input_bytes.to_string()),
        ("input_mb_per_s", format!("{:.1}", r.input_bytes as f64 / 1e6 / secs)),
        ("rows", r.rows.to_string()),
        ("traces", r.traces.to_string()),
        ("raw_payload_bytes", named_u64(&ATTRS, &r.raw)),
        ("raw_payload_total", r.raw.iter().sum::<u64>().to_string()),
        ("bytes_on_disk", obj(&files)),
        ("ratio_raw_to_disk", format!("{:.2}", r.raw.iter().sum::<u64>() as f64 / total.max(1) as f64)),
    ];
    f.extend(r.extra);
    Ok(f)
}

fn sample_ids(a: &Args) -> R<Fields> {
    let c = load_check(&a.path("dir")?)?;
    let text: String = sample(&c.order, a.num("n", 500)?).map(|id| hex(id) + "\n").collect();
    std::fs::write(a.get("out")?, &text)?;
    Ok(vec![("distinct_traces", c.order.len().to_string()), ("written", text.lines().count().to_string())])
}

fn ms(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.0
}

fn open(a: &Args, variant: &str) -> R<Fields> {
    let dir = a.path("dir")?;
    let ids: Vec<[u8; 16]> = std::fs::read_to_string(a.get("ids")?)?.lines().filter(|l| !l.is_empty()).map(unhex).collect::<R<_>>()?;
    let check = load_check(&dir)?;
    let t0 = Instant::now();
    let mut store = open_store(variant, &dir)?;
    let setup = ms(t0);
    let (mut lat, mut missing) = (vec![], 0u64);
    let mut v = Verifier::new(&check.map);
    for id in &ids {
        let t = Instant::now();
        v.trace(id);
        if !store.read_trace(id, &mut v)? {
            missing += 1;
            eprintln!("trace {} not found", hex(id));
        }
        v.end();
        lat.push(ms(t));
    }
    let mut f: Fields = vec![("lookups", ids.len().to_string()), ("bytes_returned", v.bytes.to_string()), ("mismatches", (v.mismatches + missing).to_string()), ("open_setup_ms", format!("{setup:.1}"))];
    f.extend(latency(lat));
    Ok(f)
}

/// Sums the first eight bytes of the SHA-256 of each span's values, so variants can be compared.
struct SpanDigest {
    h: Sha256,
    bytes: u64,
}
impl Visit for SpanDigest {
    fn trace(&mut self, _: &[u8; 16]) {}
    fn value(&mut self, v: &[u8]) {
        self.h.update(v);
        self.bytes += v.len() as u64;
    }
}

fn span(a: &Args, variant: &str) -> R<Fields> {
    let dir = a.path("dir")?;
    let n: usize = a.num("n", 500)?;
    let mut rng = Rng(a.num("seed", 1)?);
    let mut store = open_store(variant, &dir)?;
    let rows = store.rows();
    if rows == 0 {
        return Err("empty store".into());
    }
    let (mut lat, mut bytes, mut digest) = (vec![], 0u64, 0u64);
    for _ in 0..n {
        let row = rng.next() % rows;
        let mut d = SpanDigest { h: Sha256::new(), bytes: 0 };
        let t = Instant::now();
        store.read_span(row, &mut d)?;
        lat.push(ms(t));
        bytes += d.bytes;
        digest = digest.wrapping_add(u64::from_le_bytes(d.h.finalize()[..8].try_into()?));
    }
    let mut f: Fields = vec![("lookups", n.to_string()), ("rows", rows.to_string()), ("bytes_returned", bytes.to_string()), ("span_digest", digest.to_string())];
    f.extend(latency(lat));
    Ok(f)
}

fn scan(a: &Args, variant: &str) -> R<Fields> {
    let dir = a.path("dir")?;
    let check = load_check(&dir)?;
    let mut store = open_store(variant, &dir)?;
    let mut v = Verifier::new(&check.map);
    let t0 = Instant::now();
    store.scan(&mut v)?;
    v.end();
    let secs = t0.elapsed().as_secs_f64();
    let missing = check.order.len() as u64 - v.traces.min(check.order.len() as u64);
    Ok(vec![
        ("bytes_returned", v.bytes.to_string()),
        ("mb_per_s", format!("{:.1}", v.bytes as f64 / 1e6 / secs)),
        ("traces", v.traces.to_string()),
        ("mismatches", (v.mismatches + missing).to_string()),
    ])
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let run = || -> R<()> {
        let task = argv.first().ok_or("usage: payload build|sample-ids|open|span|scan [--flag value]...")?;
        let a = Args::parse(&argv[1..])?;
        let threads = a.num("threads", std::thread::available_parallelism().map_or(1, |n| n.get()))?.max(1);
        let t0 = Instant::now();
        let variant = if task == "sample-ids" { "-".to_string() } else { a.get("variant")?.to_string() };
        // every task is single-threaded; --threads is accepted and reported as given
        let f = match task.as_str() {
            "build" => build(&a, &variant)?,
            "sample-ids" => sample_ids(&a)?,
            "open" => open(&a, &variant)?,
            "span" => span(&a, &variant)?,
            "scan" => scan(&a, &variant)?,
            t => return Err(format!("unknown subcommand {t}").into()),
        };
        let mut f = f;
        f.push(("threads_used", "1".into()));
        finish(task, &variant, t0, threads, f);
        Ok(())
    };
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
