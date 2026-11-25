//! Write-ahead log with group commit. One dedicated thread owns the file. Records are
//! [u32 LE length][u32 LE CRC32C of the request bytes][request bytes]. A request is
//! acknowledged only after the fdatasync that covers it has returned.
use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::{mpsc, oneshot};

pub const ROLL_BYTES: u64 = 256 << 20;

pub struct Item {
    pub data: Bytes,
    pub ack: oneshot::Sender<Result<(), String>>,
}

#[derive(Default, Debug)]
pub struct WalReport {
    pub bytes: u64,
    pub fsyncs: u64,
    pub groups: u64,
    pub max_group: u64,
    pub records: u64,
    pub files: u64,
}

pub fn header(data: &[u8]) -> [u8; 8] {
    let mut h = [0u8; 8];
    h[..4].copy_from_slice(&(data.len() as u32).to_le_bytes());
    h[4..].copy_from_slice(&crc32c::crc32c(data).to_le_bytes());
    h
}

struct Log {
    dir: PathBuf,
    next: u32,
    w: BufWriter<File>,
    size: u64,
    roll: u64,
    rep: WalReport,
}

impl Log {
    fn open(dir: &Path, roll: u64) -> std::io::Result<Log> {
        std::fs::create_dir_all(dir)?;
        // never append to a file left by an earlier process
        let next = std::fs::read_dir(dir)?
            .filter_map(|e| e.ok()?.file_name().to_str()?.strip_prefix("wal-")?.strip_suffix(".log")?.parse::<u32>().ok())
            .max()
            .map_or(0, |n| n + 1);
        let mut l = Log { dir: dir.into(), next, w: BufWriter::with_capacity(1 << 20, Self::create(dir, next)?), size: 0, roll, rep: WalReport::default() };
        l.next += 1;
        l.rep.files = 1;
        l.rep.fsyncs += 1; // directory entry of the first file
        std::fs::File::open(dir)?.sync_all()?;
        Ok(l)
    }

    fn create(dir: &Path, n: u32) -> std::io::Result<File> {
        OpenOptions::new().create_new(true).append(true).open(dir.join(format!("wal-{n:06}.log")))
    }

    fn sync(&mut self) -> std::io::Result<()> {
        self.w.flush()?;
        self.w.get_ref().sync_data()?;
        self.rep.fsyncs += 1;
        Ok(())
    }

    fn append(&mut self, data: &[u8]) -> std::io::Result<()> {
        let n = 8 + data.len() as u64;
        if self.size > 0 && self.size + n > self.roll {
            // the old file must be durable before anything lands in the next one
            self.sync()?;
            self.w = BufWriter::with_capacity(1 << 20, Self::create(&self.dir, self.next)?);
            std::fs::File::open(&self.dir)?.sync_all()?;
            self.rep.fsyncs += 1;
            self.next += 1;
            self.size = 0;
            self.rep.files += 1;
        }
        self.w.write_all(&header(data))?;
        self.w.write_all(data)?;
        self.size += n;
        self.rep.bytes += n;
        self.rep.records += 1;
        Ok(())
    }
}

/// Starts the writer thread. `sync` is the minimum time between fsyncs: a request that
/// arrives when the last fsync is older goes out at once; otherwise the group waits and
/// collects whatever else arrives until the interval has passed.
pub fn spawn(rt: Handle, mut rx: mpsc::Receiver<Item>, dir: PathBuf, sync: Duration, roll: u64) -> std::io::Result<std::thread::JoinHandle<WalReport>> {
    let mut log = Log::open(&dir, roll)?;
    Ok(std::thread::Builder::new().name("wal".into()).spawn(move || {
        let mut last_sync = Instant::now() - sync;
        let mut failed: Option<String> = None;
        while let Some(first) = rx.blocking_recv() {
            let mut group = vec![first];
            let deadline = last_sync + sync;
            loop {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                match rt.block_on(async { tokio::time::timeout(deadline - now, rx.recv()).await }) {
                    Ok(Some(i)) => group.push(i),
                    _ => break,
                }
            }
            while let Ok(i) = rx.try_recv() {
                group.push(i);
            }
            let mut res = match &failed {
                Some(e) => Err(e.clone()),
                None => Ok(()),
            };
            if res.is_ok() {
                res = group.iter().try_for_each(|i| log.append(&i.data)).and_then(|_| log.sync()).map_err(|e| e.to_string());
            }
            if let Err(e) = &res {
                failed = Some(e.clone());
            } else {
                log.rep.groups += 1;
                log.rep.max_group = log.rep.max_group.max(group.len() as u64);
            }
            last_sync = Instant::now();
            for i in group {
                let _ = i.ack.send(res.clone());
            }
        }
        let _ = log.sync();
        log.rep
    })?)
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn read_all(dir: &Path) -> Vec<Vec<u8>> {
        let mut names: Vec<_> = std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().path()).collect();
        names.sort();
        let mut out = vec![];
        for p in names {
            let b = std::fs::read(p).unwrap();
            let mut o = 0;
            while o < b.len() {
                let n = u32::from_le_bytes(b[o..o + 4].try_into().unwrap()) as usize;
                let crc = u32::from_le_bytes(b[o + 4..o + 8].try_into().unwrap());
                let d = &b[o + 8..o + 8 + n];
                assert_eq!(crc, crc32c::crc32c(d), "crc mismatch");
                out.push(d.to_vec());
                o += 8 + n;
            }
        }
        out
    }

    fn tmp(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("otlpdoor-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    }

    #[test]
    fn crc32c_check_value() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
        let h = header(b"abc");
        assert_eq!(&h[..4], &3u32.to_le_bytes());
        assert_eq!(&h[4..], &crc32c::crc32c(b"abc").to_le_bytes());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn frames_groups_and_rolls() {
        let dir = tmp("wal");
        let (tx, rx) = mpsc::channel(64);
        // roll after 100 bytes so the 40-byte records spread over several files
        let h = spawn(Handle::current(), rx, dir.clone(), Duration::from_millis(20), 100).unwrap();
        let mut acks = vec![];
        let want: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i; 40]).collect();
        for d in &want {
            let (a, r) = oneshot::channel();
            tx.send(Item { data: Bytes::from(d.clone()), ack: a }).await.unwrap();
            acks.push(r);
        }
        for a in acks {
            a.await.unwrap().unwrap();
        }
        // every acknowledged record is already readable from the files
        assert_eq!(read_all(&dir), want);
        drop(tx);
        let rep = tokio::task::spawn_blocking(move || h.join().unwrap()).await.unwrap();
        assert_eq!(rep.records, 10);
        assert!(rep.files >= 5, "{rep:?}");
        assert!(rep.groups < 10, "requests should have been grouped: {rep:?}");
        assert!(rep.max_group > 1);
        assert_eq!(rep.bytes, 10 * 48);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn waits_for_the_window_and_writes_before_ack() {
        let dir = tmp("wal-window");
        let (tx, rx) = mpsc::channel(64);
        let h = spawn(Handle::current(), rx, dir.clone(), Duration::from_millis(40), ROLL_BYTES).unwrap();
        let mut acks = vec![];
        for i in 0..20u8 {
            let (a, r) = oneshot::channel();
            tx.send(Item { data: Bytes::from(vec![i; 10]), ack: a }).await.unwrap();
            acks.push(r);
            tokio::time::sleep(Duration::from_millis(3)).await;
        }
        for (i, a) in acks.into_iter().enumerate() {
            a.await.unwrap().unwrap();
            // the moment an ack arrives the record must already be written
            assert!(read_all(&dir).iter().any(|r| r[0] == i as u8));
        }
        drop(tx);
        let rep = tokio::task::spawn_blocking(move || h.join().unwrap()).await.unwrap();
        assert_eq!(rep.records, 20);
        assert!(rep.groups >= 2 && rep.groups <= 5, "{rep:?}");
        std::fs::remove_dir_all(dir).unwrap();
    }
}
