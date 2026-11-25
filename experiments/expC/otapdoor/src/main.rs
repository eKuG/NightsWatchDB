//! OTel-Arrow (OTAP) receiver for the ingest experiment. `otapdoor serve ...` is the receiver;
//! `otapdoor expect ...` prints the rows an OTLP generator file should produce, for diffing against `serve --dump-rows`.
mod attrs;
mod cols;
mod expect;
mod ipc;
mod proto;
mod stats;
mod trace;
mod wal;

use proto::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, Streaming};

pub type R<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Copy, PartialEq)]
pub enum Decode {
    /// decode the IPC payloads and convert traces to the target tables
    Full,
    /// decode the IPC payloads to record batches only
    Ipc,
    /// receive, log and acknowledge only
    None,
}

pub struct Args(pub HashMap<String, String>);

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
    pub fn get(&self, k: &str) -> Option<&str> {
        self.0.get(k).map(String::as_str)
    }
    fn num<T: std::str::FromStr>(&self, k: &str, default: T) -> R<T> {
        match self.get(k) {
            None => Ok(default),
            Some(s) => s.parse().map_err(|_| format!("--{k}: bad number {s:?}").into()),
        }
    }
}

/// The stream's dictionaries grew past the configured bound; the stream is closed so the sender starts a fresh one.
#[derive(Debug)]
struct DictLimit(u64, u64);
impl std::fmt::Display for DictLimit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "stream dictionaries hold {} bytes, over the limit of {}", self.0, self.1)
    }
}
impl std::error::Error for DictLimit {}

pub struct Shared {
    max_dict: u64,
    decode: Decode,
    wal: Option<wal::Wal>,
    pub st: stats::Stats,
    dump: Option<Mutex<std::io::BufWriter<std::fs::File>>>,
}

impl Shared {
    /// Decodes and converts one message; everything it learns goes into the statistics.
    fn process(&self, signal: u8, state: &mut ipc::StreamState, msg: &BatchArrowRecords) -> R<()> {
        if self.decode == Decode::None {
            return Ok(());
        }
        let mut c = ipc::Ctr::default();
        let parts = state.consume(msg, &mut c);
        self.st.add_ipc(&c);
        let parts = parts?;
        if c.dict_bytes > self.max_dict {
            return Err(Box::new(DictLimit(c.dict_bytes, self.max_dict)));
        }
        let rows_of = |t: i32| parts.iter().filter(|p| p.0 == t).map(|p| p.1.num_rows() as u64).sum::<u64>();
        for (t, b) in &parts {
            self.st.add_payload(*t, b.num_rows() as u64);
        }
        match signal {
            TRACES => {
                if self.decode == Decode::Full {
                    let get = |t: i32| parts.iter().find(|p| p.0 == t).map(|p| &p.1);
                    let mut sink = trace::Sink::new();
                    if self.dump.is_some() {
                        sink.dump = Some(vec![]);
                    }
                    if let Some(spans) = get(SPANS) {
                        trace::convert(spans, get(SPAN_ATTRS), get(RESOURCE_ATTRS), &mut sink)?;
                    }
                    sink.close()?;
                    self.st.add_check(&sink.check, sink.batches);
                    if let (Some(d), Some(buf)) = (&self.dump, &sink.dump) {
                        std::io::Write::write_all(&mut *d.lock().unwrap(), buf)?;
                    }
                } else {
                    self.st.spans.fetch_add(rows_of(SPANS), Relaxed);
                }
            }
            LOGS_SIGNAL => {
                self.st.log_records.fetch_add(rows_of(LOGS), Relaxed);
            }
            _ => {
                let n = [NUMBER_DATA_POINTS, SUMMARY_DATA_POINTS, HISTOGRAM_DATA_POINTS, EXP_HISTOGRAM_DATA_POINTS].iter().map(|t| rows_of(*t)).sum::<u64>();
                self.st.metric_points.fetch_add(n, Relaxed);
            }
        }
        Ok(())
    }
}

impl StreamHandler for Shared {
    fn saw_encoding(&self, name: &str) {
        self.st.add_encoding(name);
    }
    fn open(self: Arc<Self>, signal: u8, mut inbound: Streaming<Inbound>) -> ReceiverStream<Result<BatchStatus, Status>> {
        self.st.streams_opened.fetch_add(1, Relaxed);
        self.st.streams_by_signal[signal as usize].fetch_add(1, Relaxed);
        let (resp, out) = mpsc::channel::<Result<BatchStatus, Status>>(64);
        tokio::spawn(async move {
            // acknowledgements go out in arrival order, each once its log record is durable
            let (pend_tx, mut pend_rx) = mpsc::channel::<(i64, Option<oneshot::Receiver<Result<(), String>>>)>(256);
            let resp2 = resp.clone();
            let acker = tokio::spawn(async move {
                while let Some((id, wait)) = pend_rx.recv().await {
                    let (code, msg) = match wait {
                        None => (STATUS_OK, String::new()),
                        Some(w) => match w.await {
                            Ok(Ok(())) => (STATUS_OK, String::new()),
                            Ok(Err(e)) => (STATUS_UNAVAILABLE, e),
                            Err(_) => (STATUS_UNAVAILABLE, "wal gone".to_string()),
                        },
                    };
                    if resp2.send(Ok(BatchStatus { batch_id: id, status_code: code, status_message: msg })).await.is_err() {
                        break;
                    }
                }
            });
            let mut state = ipc::StreamState::default();
            let result: Result<(), Status> = async {
                loop {
                    let item = match inbound.message().await {
                        Ok(Some(i)) => i,
                        Ok(None) => return Ok(()),
                        Err(s) => return Err(s),
                    };
                    self.st.on_request(item.wire.len() as u64, item.msg.arrow_payloads.iter().map(|p| p.record.len() as u64).sum());
                    // decode first: a request that cannot be decoded is not accepted, so it is not logged either
                    let r = tokio::task::block_in_place(|| self.process(signal, &mut state, &item.msg));
                    self.st.touch();
                    if let Err(e) = r {
                        self.st.errors.fetch_add(1, Relaxed);
                        let m = format!("batch {}: {e}", item.msg.batch_id);
                        eprintln!("otapdoor: breaking the stream: {m}");
                        return Err(if e.is::<DictLimit>() { Status::resource_exhausted(m) } else { Status::internal(m) });
                    }
                    let wait = match &self.wal {
                        Some(w) => Some(w.append(item.wire.clone()).await),
                        None => None,
                    };
                    if pend_tx.send((item.msg.batch_id, wait)).await.is_err() {
                        return Err(Status::internal("ack task gone"));
                    }
                }
            }
            .await;
            // the client closed its side (or the stream failed): acknowledge everything already accepted, then end
            drop(pend_tx);
            let _ = acker.await;
            if let Err(s) = result {
                let _ = resp.send(Err(s)).await;
            }
        });
        ReceiverStream::new(out)
    }
}

async fn serve(a: Args) -> R<()> {
    let listen = a.get("listen").ok_or("--listen is required")?.parse()?;
    let decode = match a.get("decode").unwrap_or("full") {
        "full" => Decode::Full,
        "ipc" => Decode::Ipc,
        "none" => Decode::None,
        o => return Err(format!("--decode: unknown mode {o:?} (full, ipc, none)").into()),
    };
    let wal_arg = a.get("wal").unwrap_or("none").to_string();
    let wal = match wal_arg.as_str() {
        "none" => None,
        dir => Some(wal::Wal::open(&PathBuf::from(dir), a.num("wal-sync-ms", 10)?)?),
    };
    let dump = match a.get("dump-rows") {
        Some(p) => Some(Mutex::new(std::io::BufWriter::new(std::fs::File::create(p)?))),
        None => None,
    };
    let sh = Arc::new(Shared { max_dict: a.num::<u64>("max-dict-mib", 512)? << 20, decode, wal, st: stats::Stats::default(), dump });
    let idle: u64 = a.num("exit-after-idle-s", 0)?;
    let stats_file = a.get("stats-file").map(str::to_string);

    let wal_arg2 = wal_arg.clone();
    let finish = {
        let sh = sh.clone();
        move |why: &str| {
            eprintln!("otapdoor: shutting down ({why})");
            if let Some(d) = &sh.dump {
                let _ = std::io::Write::flush(&mut *d.lock().unwrap());
            }
            let j = stats::report(&sh, &wal_arg2, decode);
            if let Some(p) = &stats_file {
                let _ = std::fs::write(p, &j);
            }
            println!("{j}");
            std::process::exit(0);
        }
    };
    let f2 = finish.clone();
    let shi = sh.clone();
    tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()).unwrap();
        tokio::select! {
            _ = term.recv() => f2("SIGTERM"),
            _ = tokio::signal::ctrl_c() => f2("SIGINT"),
            _ = async {
                loop {
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    if idle > 0 && shi.st.idle_for().is_some_and(|d| d >= Duration::from_secs(idle)) { break; }
                }
            } => f2("idle"),
        }
    });

    let h: Arc<dyn StreamHandler> = sh;
    eprintln!("otapdoor: listening on {listen}, decode={}, wal={wal_arg}", a.get("decode").unwrap_or("full"));
    tonic::transport::Server::builder()
        .add_service(ArrowService::<TRACES>(h.clone()))
        .add_service(ArrowService::<LOGS_SIGNAL>(h.clone()))
        .add_service(ArrowService::<METRICS>(h))
        .serve(listen)
        .await?;
    Ok(())
}

fn main() -> R<()> {
    let argv: Vec<String> = std::env::args().collect();
    let a = Args::parse(&argv[2.min(argv.len())..])?;
    match argv.get(1).map(String::as_str) {
        Some("serve") => tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(serve(a)),
        Some("expect") => expect::run(&a),
        _ => Err("usage: otapdoor serve --listen ADDR [--wal DIR|none] [--wal-sync-ms N] [--decode full|ipc|none] [--stats-file P] [--exit-after-idle-s N] [--max-dict-mib N] [--dump-rows P]\n       otapdoor expect --in TRACEGEN_FILE --out ROWS_FILE".into()),
    }
}

#[allow(unused)]
fn _unused(_: AtomicU64) {}
