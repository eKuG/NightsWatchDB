mod convert;
mod index;
mod query;

use query::{pct, Table, Traces};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::{Duration, Instant};

pub type R<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
type Fields = Vec<(&'static str, String)>;

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

fn named(names: &[&str], v: &[u64]) -> String {
    obj(&names.iter().zip(v).map(|(n, x)| (*n, x.to_string())).collect::<Vec<_>>())
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

fn finish(task: &str, t0: Instant, threads: usize, f: Fields) {
    let (u, s) = cpu_times();
    let cg = std::fs::read_to_string("/sys/fs/cgroup/memory.peak").ok().and_then(|s| s.trim().parse::<u64>().ok()).map_or("null".to_string(), |v| v.to_string());
    let mut all: Fields = vec![
        ("lang", q("rust")),
        ("task", q(task)),
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

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn agg_json(r: &[query::AggRow]) -> String {
    arr(r, |(m, c, i, o)| obj(&[("model", m.as_deref().map_or("null".into(), q)), ("count", c.to_string()), ("input_tokens", i.to_string()), ("output_tokens", o.to_string())]))
}

fn convert(a: &Args, threads: usize) -> R<Fields> {
    let input = a.path("in")?;
    let out = a.path("out")?;
    let all = match a.0.get("stage").map_or("all", String::as_str) {
        "all" => true,
        "decode" => false,
        s => return Err(format!("--stage: {s}").into()),
    };
    if all {
        std::fs::create_dir_all(&out)?;
    }
    let t0 = Instant::now();
    let r = convert::run(&input, all.then_some(out.as_path()), threads, &AtomicBool::new(false), &AtomicU64::new(0))?;
    let secs = t0.elapsed().as_secs_f64();
    Ok(vec![
        ("input_bytes", r.input_bytes.to_string()),
        ("input_mb_per_s", format!("{:.1}", r.input_bytes as f64 / 1e6 / secs)),
        ("rows", r.st.rows.to_string()),
        ("sum_input_tokens", r.st.in_tok.to_string()),
        ("sum_output_tokens", r.st.out_tok.to_string()),
        ("nonnull", named(&convert::PAY_COLS, &r.st.nonnull)),
        ("payload_bytes", named(&convert::PAY_COLS, &r.st.bytes)),
        ("spans_parquet_bytes", r.spans_bytes.to_string()),
        ("payloads_parquet_bytes", r.pay_bytes.to_string()),
        ("bloom_filter", r.bloom.to_string()),
    ])
}

fn agg(a: &Args, threads: usize) -> R<Fields> {
    let t = Table::open(&a.path("dir")?.join("spans.parquet"))?;
    let (lo, hi) = query::start_range(&t)?;
    let (from, to) = query::window(lo, hi);
    let (mut runs, mut res) = (vec![], vec![]);
    for _ in 0..a.num("runs", 5)? {
        let t0 = Instant::now();
        res = query::agg(&t, from, to, threads)?;
        runs.push(ms(t0.elapsed()));
    }
    Ok(vec![("window", format!("[{from},{to}]")), ("result", agg_json(&res)), ("runs_ms", arr(&runs, |x| format!("{x:.3}")))])
}

fn sample_ids(a: &Args) -> R<Fields> {
    let t = Table::open(&a.path("dir")?.join("spans.parquet"))?;
    let ids = query::distinct_traces(&t)?;
    let text: String = query::sample(&ids, a.num("n", 200)?).map(|id| query::hex(id) + "\n").collect();
    std::fs::write(a.get("out")?, &text)?;
    Ok(vec![("distinct_traces", ids.len().to_string()), ("written", text.lines().count().to_string())])
}

fn read_ids(p: &str) -> R<Vec<[u8; 16]>> {
    std::fs::read_to_string(p)?.lines().filter(|l| !l.is_empty()).map(query::unhex).collect()
}

fn trace(a: &Args) -> R<Fields> {
    let ids = read_ids(a.get("ids")?)?;
    let tr = Traces::open(&a.path("dir")?)?;
    let (mut lat, mut rows, mut bytes) = (vec![], 0, 0);
    for id in &ids {
        let t0 = Instant::now();
        let (r, b) = tr.lookup(id)?;
        lat.push(ms(t0.elapsed()));
        if r == 0 {
            eprintln!("warning: trace {} not found", query::hex(id));
        }
        rows += r;
        bytes += b;
    }
    lat.sort_by(f64::total_cmp);
    Ok(vec![
        ("lookups", ids.len().to_string()),
        ("rows_returned", rows.to_string()),
        ("payload_bytes_returned", bytes.to_string()),
        ("p50_ms", format!("{:.3}", pct(&lat, 50.0))),
        ("p99_ms", format!("{:.3}", pct(&lat, 99.0))),
        ("max_ms", format!("{:.3}", pct(&lat, 100.0))),
        ("pruning", q(query::PRUNING)),
    ])
}

fn index(a: &Args, threads: usize) -> R<Fields> {
    let t = Table::open(&a.path("dir")?.join("payloads.parquet"))?;
    let t0 = Instant::now();
    let r = index::build(&t, &a.path("out")?, threads)?;
    Ok(vec![
        ("text_bytes", r.text_bytes.to_string()),
        ("text_mb_per_s", format!("{:.1}", r.text_bytes as f64 / 1e6 / t0.elapsed().as_secs_f64())),
        ("docs", r.docs.to_string()),
        ("distinct_tokens", r.distinct.to_string()),
        ("postings", r.postings.to_string()),
        ("index_bytes", r.index_bytes.to_string()),
        ("index_sha256", q(&r.sha)),
    ])
}

fn mixed(a: &Args, threads: usize) -> R<Fields> {
    let (input, dir) = (a.path("in")?, a.path("dir")?);
    let ids = read_ids(a.get("ids")?)?;
    let secs: f64 = a.num("seconds", 60.0)?;
    let spans = Table::open(&dir.join("spans.parquet"))?;
    let tr = Traces::open(&dir)?;
    let (lo, hi) = query::start_range(&spans)?;
    let (from, to) = query::window(lo, hi);
    let scratch = dir.join(".mixed-scratch");
    let (stop, progress) = (AtomicBool::new(false), AtomicU64::new(0));
    let t0 = Instant::now();
    let res = std::thread::scope(|s| -> R<(u64, Vec<f64>, Vec<f64>)> {
        let ingest = s.spawn(|| -> R<u64> {
            let mut passes = 0;
            while !stop.load(Relaxed) {
                std::fs::create_dir_all(&scratch)?;
                convert::run(&input, Some(&scratch), (threads - 1).max(1), &stop, &progress)?;
                std::fs::remove_dir_all(&scratch)?;
                passes += !stop.load(Relaxed) as u64; // a pass cut off by the deadline does not count
            }
            Ok(passes)
        });
        let query = s.spawn(|| -> R<(Vec<f64>, Vec<f64>)> {
            let (mut aggs, mut trs, mut n) = (vec![], vec![], 0);
            while !stop.load(Relaxed) {
                let t = Instant::now();
                query::agg(&spans, from, to, 1)?;
                aggs.push(ms(t.elapsed()));
                let t = Instant::now();
                tr.lookup(&ids[n % ids.len()])?;
                trs.push(ms(t.elapsed()));
                n += 1;
            }
            Ok((aggs, trs))
        });
        std::thread::sleep(Duration::from_secs_f64(secs));
        stop.store(true, Relaxed);
        let (a, t) = query.join().map_err(|_| "query worker panicked")??;
        Ok((ingest.join().map_err(|_| "ingest worker panicked")??, a, t))
    });
    let _ = std::fs::remove_dir_all(&scratch);
    let (passes, mut aggs, mut trs) = res?;
    aggs.sort_by(f64::total_cmp);
    trs.sort_by(f64::total_cmp);
    let wall = t0.elapsed().as_secs_f64();
    Ok(vec![
        ("ingest_passes", passes.to_string()),
        ("ingest_mb_per_s", format!("{:.1}", progress.load(Relaxed) as f64 / 1e6 / wall)),
        ("agg_p50_ms", format!("{:.3}", pct(&aggs, 50.0))),
        ("agg_p99_ms", format!("{:.3}", pct(&aggs, 99.0))),
        ("agg_max_ms", format!("{:.3}", pct(&aggs, 100.0))),
        ("agg_count", aggs.len().to_string()),
        ("trace_p50_ms", format!("{:.3}", pct(&trs, 50.0))),
        ("trace_p99_ms", format!("{:.3}", pct(&trs, 99.0))),
        ("trace_max_ms", format!("{:.3}", pct(&trs, 100.0))),
    ])
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let run = || -> R<()> {
        let task = argv.first().ok_or("usage: bake convert|agg|sample-ids|trace|index|mixed [--flag value]...")?;
        let a = Args::parse(&argv[1..])?;
        let threads = a.num("threads", std::thread::available_parallelism().map_or(1, |n| n.get()))?.max(1);
        let t0 = Instant::now();
        let f = match task.as_str() {
            "convert" => convert(&a, threads)?,
            "agg" => agg(&a, threads)?,
            "sample-ids" => sample_ids(&a)?,
            "trace" => trace(&a)?,
            "index" => index(&a, threads)?,
            "mixed" => mixed(&a, threads)?,
            t => return Err(format!("unknown subcommand {t}").into()),
        };
        finish(task, t0, threads, f);
        Ok(())
    };
    if let Err(e) = run() {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}
