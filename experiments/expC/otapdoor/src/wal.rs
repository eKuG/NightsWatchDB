//! Write-ahead log with group commit. Record: u32 little-endian length, u32 little-endian CRC32C of the body, body.
//! One writer thread appends in arrival order and calls fsync at most once per sync interval; a request is
//! acknowledged (its oneshot fires) only after the fsync that covers it returns.
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::oneshot;

pub const ROLL_BYTES: u64 = 256 << 20;

type Ack = oneshot::Sender<Result<(), String>>;

#[derive(Default)]
pub struct WalStats {
    pub bytes: AtomicU64,
    pub fsyncs: AtomicU64,
    pub files: AtomicU64,
}

pub struct Wal {
    tx: SyncSender<(bytes::Bytes, Ack)>,
    pub stats: Arc<WalStats>,
    thread: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

pub fn frame_header(body: &[u8]) -> [u8; 8] {
    let mut h = [0u8; 8];
    h[..4].copy_from_slice(&(body.len() as u32).to_le_bytes());
    h[4..].copy_from_slice(&crc32c::crc32c(body).to_le_bytes());
    h
}

struct Writer {
    dir: PathBuf,
    roll: u64,
    file: Option<File>,
    size: u64,
    seq: u64,
    stats: Arc<WalStats>,
}

impl Writer {
    fn open_next(&mut self) -> io::Result<()> {
        self.seq += 1;
        let p = self.dir.join(format!("wal-{:08}.log", self.seq));
        self.file = Some(OpenOptions::new().create_new(true).append(true).open(p)?);
        self.size = 0;
        self.stats.files.fetch_add(1, Relaxed);
        Ok(())
    }
    fn sync(&mut self) -> io::Result<()> {
        if let Some(f) = &self.file {
            f.sync_all()?;
            self.stats.fsyncs.fetch_add(1, Relaxed);
        }
        Ok(())
    }
    /// Appends one record; returns true when it first closed (and synced) a full file, so the caller can release
    /// the waiters collected so far.
    fn append(&mut self, body: &[u8]) -> io::Result<bool> {
        let n = 8 + body.len() as u64;
        let mut rolled = false;
        if self.file.is_none() {
            self.open_next()?;
        } else if self.size > 0 && self.size + n > self.roll {
            self.sync()?;
            self.open_next()?;
            rolled = true;
        }
        let f = self.file.as_mut().unwrap();
        f.write_all(&frame_header(body))?;
        f.write_all(body)?;
        self.size += n;
        self.stats.bytes.fetch_add(n, Relaxed);
        Ok(rolled)
    }
}

impl Wal {
    pub fn open(dir: &Path, sync_ms: u64) -> io::Result<Wal> {
        Wal::open_with(dir, sync_ms, ROLL_BYTES)
    }

    pub fn open_with(dir: &Path, sync_ms: u64, roll: u64) -> io::Result<Wal> {
        std::fs::create_dir_all(dir)?;
        let stats = Arc::new(WalStats::default());
        // a handful of requests may queue; their bodies are already in memory, so keep this small
        let (tx, rx) = sync_channel::<(bytes::Bytes, Ack)>(8);
        let mut w = Writer { dir: dir.to_path_buf(), roll, file: None, size: 0, seq: 0, stats: stats.clone() };
        let interval = Duration::from_millis(sync_ms);
        let thread = std::thread::Builder::new().name("wal".into()).spawn(move || run(&mut w, rx, interval)).unwrap();
        Ok(Wal { tx, stats, thread: std::sync::Mutex::new(Some(thread)) })
    }

    /// Queues a record; the returned receiver resolves once it is durable.
    pub async fn append(&self, body: bytes::Bytes) -> oneshot::Receiver<Result<(), String>> {
        let (a, r) = oneshot::channel();
        let mut item = (body, a);
        loop {
            match self.tx.try_send(item) {
                Ok(()) => return r,
                Err(TrySendError::Full(i)) => {
                    item = i;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                Err(TrySendError::Disconnected((_, a))) => {
                    let _ = a.send(Err("wal closed".into()));
                    return r;
                }
            }
        }
    }

    /// Stops accepting records, flushes and syncs what is queued.
    pub fn close(self) {
        let Wal { tx, thread, .. } = self;
        drop(tx);
        let t = thread.lock().unwrap().take();
        if let Some(t) = t {
            let _ = t.join();
        }
    }
}

fn release(w: &mut Writer, waiters: &mut Vec<Ack>, r: io::Result<()>) {
    let r = r.map_err(|e| e.to_string());
    let _ = w;
    for a in waiters.drain(..) {
        let _ = a.send(r.clone());
    }
}

fn run(w: &mut Writer, rx: Receiver<(bytes::Bytes, Ack)>, interval: Duration) {
    let mut waiters: Vec<Ack> = vec![];
    let mut deadline: Option<Instant> = None;
    loop {
        let got = match deadline {
            None => rx.recv().map_err(|_| RecvTimeoutError::Disconnected),
            Some(d) => rx.recv_timeout(d.saturating_duration_since(Instant::now())),
        };
        match got {
            Ok((body, ack)) => {
                match w.append(&body) {
                    Ok(rolled) => {
                        if rolled {
                            // the closed file was synced during the roll; everything before this record is durable
                            release(w, &mut waiters, Ok(()));
                        }
                        waiters.push(ack);
                        deadline.get_or_insert_with(|| Instant::now() + interval);
                    }
                    Err(e) => {
                        let _ = ack.send(Err(e.to_string()));
                    }
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                let r = w.sync();
                release(w, &mut waiters, r);
                deadline = None;
            }
            Err(RecvTimeoutError::Disconnected) => {
                let r = w.sync();
                release(w, &mut waiters, r);
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read_all(dir: &Path) -> Vec<Vec<u8>> {
        let mut files: Vec<_> = std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).collect();
        files.sort();
        let mut out = vec![];
        for f in files {
            let d = std::fs::read(f).unwrap();
            let mut o = 0;
            while o < d.len() {
                let n = u32::from_le_bytes(d[o..o + 4].try_into().unwrap()) as usize;
                let crc = u32::from_le_bytes(d[o + 4..o + 8].try_into().unwrap());
                let body = d[o + 8..o + 8 + n].to_vec();
                assert_eq!(crc, crc32c::crc32c(&body), "crc mismatch");
                out.push(body);
                o += 8 + n;
            }
        }
        out
    }

    #[test]
    fn crc32c_check_value() {
        // the standard CRC-32C check value
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
        let h = frame_header(b"123456789");
        assert_eq!(h, [9, 0, 0, 0, 0x83, 0x92, 0x06, 0xE3]);
    }

    #[tokio::test]
    async fn frames_group_commit_and_roll() {
        let dir = std::env::temp_dir().join(format!("otapdoor-wal-test-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // roll at 100 bytes so that a few records span several files
        let wal = Wal::open_with(&dir, 5, 100).unwrap();
        let bodies: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 40]).collect();
        let mut waits = vec![];
        for b in &bodies {
            waits.push(wal.append(b.clone().into()).await);
        }
        for w in waits {
            w.await.unwrap().unwrap();
        }
        // everything acknowledged is on disk already
        assert_eq!(read_all(&dir), bodies);
        assert!(wal.stats.files.load(Relaxed) >= 5, "files {}", wal.stats.files.load(Relaxed));
        // a batch of appends shares fsyncs: far fewer syncs than records plus rolls
        assert!(wal.stats.fsyncs.load(Relaxed) <= 10 + 1);
        assert_eq!(wal.stats.bytes.load(Relaxed), 10 * 48);
        wal.close();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
