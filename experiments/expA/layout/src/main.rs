//! Three physical layouts for one span table: cotrace, svc, traceid.
mod build;
mod decode;
mod idx;
mod query;
#[cfg(test)]
mod tests;

use query::{pct, Layout, Params};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

pub type R<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Fields = Vec<(&'static str, String)>;

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Variant {
    Cotrace,
    Svc,
    Traceid,
}

impl Variant {
    fn name(self) -> &'static str {
        match self {
            Variant::Cotrace => "cotrace",
            Variant::Svc => "svc",
            Variant::Traceid => "traceid",
        }
    }
}

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
    fn variant(&self) -> R<Variant> {
        match self.get("variant")? {
            "cotrace" => Ok(Variant::Cotrace),
            "svc" => Ok(Variant::Svc),
            "traceid" => Ok(Variant::Traceid),
            v => Err(format!("--variant: unknown {v:?} (cotrace|svc|traceid)").into()),
        }
    }
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

fn obj(f: &[(&str, String)]) -> String {
    format!("{{{}}}", f.iter().map(|(k, v)| format!("{}:{}", q(k), v)).collect::<Vec<_>>().join(","))
}

fn arr<T>(v: &[T], f: impl Fn(&T) -> String) -> String {
    format!("[{}]", v.iter().map(f).collect::<Vec<_>>().join(","))
}

/// utime and stime of this process from /proc, in seconds (clock tick resolution, 10 ms).
fn cpu_times() -> (f64, f64) {
    let s = std::fs::read_to_string("/proc/self/stat").unwrap_or_default();
    let f: Vec<&str> = s.rsplit_once(')').map_or("", |x| x.1).split_whitespace().collect();
    let t = |i: usize| f.get(i).and_then(|x| x.parse::<f64>().ok()).unwrap_or(0.0) / 100.0;
    (t(11), t(12))
}

fn peak_rss() -> u64 {
    std::fs::read_to_string("/proc/self/status").ok().and_then(|s| s.lines().find_map(|l| l.strip_prefix("VmHWM:")?.trim().strip_suffix("kB")?.trim().parse::<u64>().ok())).map_or(0, |k| k * 1024)
}

fn finish(task: &str, variant: Option<Variant>, t0: Instant, threads: usize, f: Fields) {
    let (u, s) = cpu_times();
    let mut all: Fields = vec![
        ("task", q(task)),
        ("variant", variant.map_or("null".into(), |v| q(v.name()))),
        ("wall_s", format!("{:.3}", t0.elapsed().as_secs_f64())),
        ("user_cpu_s", format!("{u:.2}")),
        ("sys_cpu_s", format!("{s:.2}")),
        ("peak_rss_bytes", peak_rss().to_string()),
        ("threads", threads.to_string()),
    ];
    all.extend(f);
    println!("{}", obj(&all));
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// p50, p90, p99 and max by nearest rank.
fn latency(mut v: Vec<f64>) -> Fields {
    v.sort_by(f64::total_cmp);
    vec![("p50_ms", format!("{:.3}", pct(&v, 50.0))), ("p90_ms", format!("{:.3}", pct(&v, 90.0))), ("p99_ms", format!("{:.3}", pct(&v, 99.0))), ("max_ms", format!("{:.3}", pct(&v, 100.0)))]
}

fn params_json(p: &Params) -> String {
    obj(&[("service", q(&p.service)), ("model", q(&p.model)), ("from_ns", p.from.to_string()), ("to_ns", p.to.to_string())])
}

/// Parameters from the data, unless given on the command line.
fn get_params(a: &Args, l: &Layout, threads: usize) -> R<Params> {
    let mut p = query::params(l, threads)?;
    if let Some(s) = a.0.get("service") {
        p.service = s.clone();
    }
    if let Some(m) = a.0.get("model") {
        p.model = m.clone();
    }
    Ok(p)
}

fn build(a: &Args, threads: usize) -> R<Fields> {
    let v = a.variant()?;
    let t0 = Instant::now();
    let r = build::run(&a.path("in")?, &a.path("out")?, v, a.num("window-s", 900)?, threads, a.num("sort-mem-mb", 256)?)?;
    let secs = t0.elapsed().as_secs_f64();
    let mut f: Fields = vec![
        ("rows", r.rows.to_string()),
        ("traces", r.traces.to_string()),
        ("segments", r.segments.to_string()),
        ("bytes_on_disk", obj(&r.bytes.iter().map(|(k, n)| (*k, n.to_string())).collect::<Vec<_>>())),
        ("input_bytes", r.input_bytes.to_string()),
        ("input_mb_per_s", format!("{:.1}", r.input_bytes as f64 / 1e6 / secs)),
    ];
    f.extend(r.notes);
    Ok(f)
}

fn params(a: &Args, threads: usize) -> R<Fields> {
    let l = Layout::open(&a.path("dir")?, a.variant()?)?;
    let p = query::params(&l, threads)?;
    Ok(vec![("params", params_json(&p))])
}

fn sample_ids(a: &Args, threads: usize) -> R<Fields> {
    let l = Layout::open(&a.path("dir")?, a.variant()?)?;
    let (distinct, ids) = query::sample_ids(&l, a.num("n", 500)?, threads)?;
    let text: String = ids.iter().map(|id| query::hex(id) + "\n").collect();
    std::fs::write(a.get("out")?, text)?;
    Ok(vec![("distinct_traces", distinct.to_string()), ("written", ids.len().to_string())])
}

fn open(a: &Args) -> R<Fields> {
    let ids: Vec<[u8; 16]> = std::fs::read_to_string(a.get("ids")?)?.lines().filter(|l| !l.is_empty()).map(query::unhex).collect::<R<_>>()?;
    let l = Layout::open(&a.path("dir")?, a.variant()?)?;
    let (mut lat, mut rows, mut missing, mut dig) = (vec![], 0u64, 0u64, 0u64);
    for id in &ids {
        let t0 = Instant::now();
        let r = l.open_trace(id)?;
        lat.push(ms(t0.elapsed()));
        match r {
            Some(b) => {
                let (n, d) = query::digest(&b);
                rows += n as u64;
                dig = dig.wrapping_add(d);
            }
            None => {
                missing += 1;
                eprintln!("warning: trace {} not found", query::hex(id));
            }
        }
    }
    let mut f: Fields = vec![("lookups", ids.len().to_string()), ("rows_returned", rows.to_string()), ("not_found", missing.to_string()), ("rows_digest", q(&format!("{dig:016x}")))];
    f.extend(latency(lat));
    Ok(f)
}

fn list(a: &Args, threads: usize, which: u8) -> R<Fields> {
    let l = Layout::open(&a.path("dir")?, a.variant()?)?;
    let p = get_params(a, &l, threads)?;
    let (mut runs, mut res) = (vec![], vec![]);
    for _ in 0..a.num("runs", 20)? {
        let t0 = Instant::now();
        res = if which == 1 { query::list1(&l, &p, threads)? } else { query::list2(&l, &p, threads)? };
        runs.push(ms(t0.elapsed()));
    }
    let mut f: Fields = vec![("params", params_json(&p)), ("returned", res.len().to_string()), ("result_sha256", q(&query::result_sha256(&res)))];
    if let Some(i) = res.first() {
        f.push(("newest", obj(&[("trace_id", q(&query::hex(&i.id))), ("start_ns", i.start.to_string()), ("duration_ns", i.dur.to_string()), ("root_name", q(&i.name))])));
    }
    f.push(("runs_ms", arr(&runs, |x| format!("{x:.3}"))));
    f.extend(latency(runs));
    Ok(f)
}

fn agg_json(r: &[query::AggRow]) -> String {
    arr(r, |(m, c, i, o)| obj(&[("model", m.as_deref().map_or("null".into(), q)), ("count", c.to_string()), ("input_tokens", i.to_string()), ("output_tokens", o.to_string())]))
}

fn agg(a: &Args, threads: usize) -> R<Fields> {
    let l = Layout::open(&a.path("dir")?, a.variant()?)?;
    let p = get_params(a, &l, threads)?;
    let (mut runs, mut res) = (vec![], vec![]);
    for _ in 0..a.num("runs", 20)? {
        let t0 = Instant::now();
        res = query::agg(&l, &p, threads)?;
        runs.push(ms(t0.elapsed()));
    }
    let mut f: Fields = vec![("params", params_json(&p)), ("result", agg_json(&res)), ("runs_ms", arr(&runs, |x| format!("{x:.3}")))];
    f.extend(latency(runs));
    Ok(f)
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let run = || -> R<()> {
        let task = argv.first().ok_or("usage: layout build|params|sample-ids|open|list1|list2|agg [--flag value]...")?;
        let a = Args::parse(&argv[1..])?;
        let threads = a.num("threads", std::thread::available_parallelism().map_or(1, |n| n.get()))?.max(1);
        let t0 = Instant::now();
        let f = match task.as_str() {
            "build" => build(&a, threads)?,
            "params" => params(&a, threads)?,
            "sample-ids" => sample_ids(&a, threads)?,
            "open" => open(&a)?,
            "list1" => list(&a, threads, 1)?,
            "list2" => list(&a, threads, 2)?,
            "agg" => agg(&a, threads)?,
            t => return Err(format!("unknown subcommand {t}").into()),
        };
        finish(task, a.variant().ok(), t0, threads, f);
        Ok(())
    };
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
