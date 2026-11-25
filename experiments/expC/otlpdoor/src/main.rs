mod convert;
mod grpc;
mod lazy;
mod wal;

use convert::{Check, LogAcc, MetricAcc, TraceAcc, MAX_BYTES, MAX_ROWS, PAY_COLS};
use grpc::{Counters, Door, LogsK, MetricsK, Shared, Signal, TraceK, Work};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering::Relaxed;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::sync::{mpsc, Notify, Semaphore};

type R<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, PartialEq, Debug)]
enum Mode {
    Full,
    Lazy,
    None,
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
    fn str(&self, k: &str, default: &str) -> String {
        self.0.get(k).cloned().unwrap_or(default.into())
    }
    fn num<T: std::str::FromStr>(&self, k: &str, default: T) -> R<T> {
        match self.0.get(k) {
            None => Ok(default),
            Some(s) => s.parse().map_err(|_| format!("--{k}: bad number {s:?}").into()),
        }
    }
}

/// Converter thread: owns the builders, so batches span requests and close on the byte bound.
/// Idle for `idle_flush`: close the open batches so nothing sits in memory unbuilt.
struct ConvReport {
    traces: TraceAcc,
    logs: LogAcc,
    metrics: MetricAcc,
}

fn spawn_conv(rt: Handle, mut rx: mpsc::Receiver<Work>, mode: Mode) -> std::thread::JoinHandle<ConvReport> {
    std::thread::Builder::new()
        .name("convert".into())
        .spawn(move || {
            let (mut t, mut l, mut m) = (TraceAcc::new(MAX_ROWS, MAX_BYTES), LogAcc::new(), MetricAcc::new());
            loop {
                let w = match rt.block_on(async { tokio::time::timeout(Duration::from_millis(500), rx.recv()).await }) {
                    Ok(Some(w)) => w,
                    Ok(None) => break,
                    Err(_) => {
                        let _ = (t.close(), l.close(), m.close());
                        continue;
                    }
                };
                let r = match (w.signal, mode) {
                    (Signal::Traces, Mode::Lazy) => convert::lazy_traces(&w.data, &mut t),
                    (Signal::Traces, _) => convert::full_traces(&w.data, &mut t),
                    // logs and metrics have only the full path
                    (Signal::Logs, _) => l.push_request(&w.data),
                    (Signal::Metrics, _) => m.push_request(&w.data),
                };
                let _ = w.reply.send(r);
            }
            let _ = (t.close(), l.close(), m.close());
            ConvReport { traces: t, logs: l, metrics: m }
        })
        .unwrap()
}

/// utime and stime of this process from /proc, in seconds (10 ms ticks).
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
    format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
}

fn named(v: &[u64]) -> String {
    format!("{{{}}}", PAY_COLS.iter().zip(v).map(|(n, x)| format!("\"{n}\":{x}")).collect::<Vec<_>>().join(","))
}

fn check_json(c: &Check) -> String {
    format!("{{\"rows\":{},\"sum_input_tokens\":{},\"sum_output_tokens\":{},\"nonnull\":{},\"payload_bytes\":{}}}", c.rows, c.in_tok, c.out_tok, named(&c.nonnull), named(&c.bytes))
}

async fn serve(a: Args) -> R<()> {
    let listen = a.str("listen", "0.0.0.0:4317");
    let wal_arg = a.str("wal", "none");
    let mode = match a.str("decode", "lazy").as_str() {
        "full" => Mode::Full,
        "lazy" => Mode::Lazy,
        "none" => Mode::None,
        o => return Err(format!("--decode must be full, lazy or none, not {o}").into()),
    };
    let sync_ms: u64 = a.num("wal-sync-ms", 10)?;
    let roll: u64 = a.num("wal-roll-bytes", wal::ROLL_BYTES)?;
    let idle_s: f64 = a.num("exit-after-idle-s", 0.0)?;
    let cap: usize = a.num::<usize>("max-inflight-mib", 128)? << 20;
    let streams: u32 = a.num("max-concurrent-streams", 32)?;
    let counters = Arc::new(Counters::new());
    let rt = Handle::current();

    let (wal_tx, wal_h) = if wal_arg == "none" {
        (None, None)
    } else {
        let (tx, rx) = mpsc::channel(256);
        (Some(tx), Some(wal::spawn(rt.clone(), rx, PathBuf::from(&wal_arg), Duration::from_millis(sync_ms), roll)?))
    };
    let (conv_tx, conv_h) = if mode == Mode::None {
        (None, None)
    } else {
        let (tx, rx) = mpsc::channel(8);
        (Some(tx), Some(spawn_conv(rt.clone(), rx, mode)))
    };
    let shared = Arc::new(Shared { wal: wal_tx, conv: conv_tx, inflight: Semaphore::new(cap), inflight_cap: cap, counters: counters.clone() });

    let stop = Arc::new(Notify::new());
    if idle_s > 0.0 {
        let (c, stop) = (counters.clone(), stop.clone());
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let idle = c.now().saturating_sub(c.last_ns.load(Relaxed)) as f64 / 1e9;
                if c.requests.load(Relaxed) > 0 && c.active.load(Relaxed) == 0 && idle >= idle_s {
                    stop.notify_one();
                    return;
                }
            }
        });
    }
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let shutdown = {
        let stop = stop.clone();
        async move {
            tokio::select! {
                _ = term.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
                _ = stop.notified() => {}
            }
        }
    };
    let addr = listen.parse()?;
    eprintln!("otlpdoor listening on {addr} decode={mode:?} wal={wal_arg}");
    // Window sizes: the HTTP/2 defaults (64 KiB) would turn a multi-MiB request into hundreds of flow-control round trips.
    tonic::transport::Server::builder()
        .initial_stream_window_size(4 << 20)
        .initial_connection_window_size(16 << 20)
        .max_concurrent_streams(streams)
        .add_service(Door::<TraceK>::new(shared.clone()))
        .add_service(Door::<LogsK>::new(shared.clone()))
        .add_service(Door::<MetricsK>::new(shared.clone()))
        .serve_with_shutdown(addr, shutdown)
        .await?;
    drop(shared);

    let wr = wal_h.map(|h| h.join().map_err(|_| "wal thread panicked")).transpose()?;
    let cr = conv_h.map(|h| h.join().map_err(|_| "converter thread panicked")).transpose()?;
    let (user, sys) = cpu_times();
    let (first, last) = (counters.first_ns.load(Relaxed), counters.last_ns.load(Relaxed));
    let wall = if first == u64::MAX { 0.0 } else { last.saturating_sub(first) as f64 / 1e9 };
    let requests = counters.requests.load(Relaxed);
    let cg = std::fs::read_to_string("/sys/fs/cgroup/memory.peak").ok().and_then(|s| s.trim().parse::<u64>().ok()).map_or("null".to_string(), |v| v.to_string());
    // Counts and the check block come from the Arrow batches, so they are null when nothing was decoded.
    let (spans, logs, points, batches, check) = match &cr {
        Some(c) => (c.traces.check.rows.to_string(), c.logs.records.to_string(), c.metrics.points.to_string(), (c.traces.batches + c.logs.batches + c.metrics.batches).to_string(), check_json(&c.traces.check)),
        None => ("null".into(), "null".into(), "null".into(), "null".into(), "null".into()),
    };
    let w = wr.as_ref();
    let groups = w.map_or(0, |w| w.groups);
    let out = format!(
        "{{\"door\":\"otlp\",\"decode\":{},\"wal\":{},\"requests\":{requests},\"rejected\":{},\"wire_bytes\":{},\"max_request_bytes\":{},\"spans\":{spans},\"log_records\":{logs},\"metric_points\":{points},\"batches\":{batches},\"wall_s\":{wall:.3},\"user_cpu_s\":{user:.2},\"sys_cpu_s\":{sys:.2},\"peak_rss_bytes\":{},\"cgroup_peak_bytes\":{cg},\"wal_bytes\":{},\"wal_fsyncs\":{},\"wal_groups\":{groups},\"wal_mean_group\":{:.2},\"wal_max_group\":{},\"wal_files\":{},\"check\":{check}}}",
        q(&format!("{mode:?}").to_lowercase()),
        q(&wal_arg),
        counters.rejected.load(Relaxed),
        counters.wire_bytes.load(Relaxed),
        counters.max_request.load(Relaxed),
        peak_rss(),
        w.map_or(0, |w| w.bytes),
        w.map_or(0, |w| w.fsyncs),
        if groups > 0 { requests as f64 / groups as f64 } else { 0.0 },
        w.map_or(0, |w| w.max_group),
        w.map_or(0, |w| w.files),
    );
    println!("{out}");
    if let Some(p) = a.0.get("stats-file") {
        std::fs::write(p, format!("{out}\n"))?;
    }
    Ok(())
}

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let res = (|| -> R<()> {
        if argv.first().map(String::as_str) != Some("serve") {
            return Err("usage: otlpdoor serve --listen ADDR [--wal DIR|none] [--decode full|lazy|none] [--wal-sync-ms N] [--stats-file PATH] [--exit-after-idle-s N] [--threads N] [--max-inflight-mib N] [--max-concurrent-streams N]".into());
        }
        let a = Args::parse(&argv[1..])?;
        let mut b = tokio::runtime::Builder::new_multi_thread();
        b.enable_all();
        if let Some(n) = a.0.get("threads") {
            b.worker_threads(n.parse()?);
        }
        b.build()?.block_on(serve(a))
    })();
    if let Err(e) = res {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests;
