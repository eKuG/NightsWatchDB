//! `zstdlong`: one zstd frame per trace (level 3, long-distance matching, window log 27).
//!
//! Files
//!   zl.data  frames back to back.
//!   zl.idx   "ZLX1", u64 trace count, u64 span count, then per trace
//!            { trace_id[16], frame_off u64, frame_len u64, raw_len u64, first_row u64, spans u32 }
//!            (52 bytes), then per span (global row order) its u64 start offset inside the
//!            uncompressed frame of its trace.
//! Uncompressed frame content, per span, six attributes in table order, each
//!            0x00                      value absent
//!            0x01, u32 len, bytes      value present
use crate::util::*;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use zstd::stream::{read::Decoder, write::Encoder};

const LEVEL: i32 = 3;
const WINDOW_LOG: u32 = 27;
const MAGIC: &[u8; 4] = b"ZLX1";
const TREC: usize = 52;

struct Trace {
    id: [u8; 16],
    off: u64,
    len: u64,
    raw: u64,
    first: u64,
    spans: u32,
}

pub struct ZlBuilder {
    dir: PathBuf,
    out: Option<Counting<BufWriter<File>>>,
    enc: Option<Encoder<'static, Counting<BufWriter<File>>>>,
    raw: u64,
    frame_start: u64,
    rows: u64,
    first_row: u64,
    id: [u8; 16],
    traces: Vec<Trace>,
    span_offs: Vec<u64>,
}

impl ZlBuilder {
    pub fn new(dir: &Path) -> R<Self> {
        let out = Counting { inner: BufWriter::with_capacity(1 << 20, File::create(dir.join("zl.data"))?), n: 0 };
        Ok(Self { dir: dir.into(), out: Some(out), enc: None, raw: 0, frame_start: 0, rows: 0, first_row: 0, id: [0; 16], traces: vec![], span_offs: vec![] })
    }
    fn put(&mut self, b: &[u8]) -> R<()> {
        self.enc.as_mut().ok_or("no open frame")?.write_all(b)?;
        self.raw += b.len() as u64;
        Ok(())
    }
}

impl Builder for ZlBuilder {
    fn begin_trace(&mut self, id: &[u8; 16], _: u64) -> R<()> {
        let out = self.out.take().ok_or("frame already open")?;
        self.frame_start = out.n;
        let mut e = Encoder::new(out, LEVEL)?;
        e.long_distance_matching(true)?;
        e.window_log(WINDOW_LOG)?;
        self.enc = Some(e);
        (self.raw, self.first_row, self.id) = (0, self.rows, *id);
        Ok(())
    }
    fn span(&mut self, s: &SpanRec) -> R<()> {
        self.span_offs.push(self.raw);
        for v in s.vals {
            match v {
                None => self.put(&[0])?,
                Some(b) => {
                    self.put(&[1])?;
                    self.put(&(b.len() as u32).to_le_bytes())?;
                    self.put(b)?;
                }
            }
        }
        self.rows += 1;
        Ok(())
    }
    fn end_trace(&mut self) -> R<()> {
        let out = self.enc.take().ok_or("no open frame")?.finish()?;
        self.traces.push(Trace { id: self.id, off: self.frame_start, len: out.n - self.frame_start, raw: self.raw, first: self.first_row, spans: (self.rows - self.first_row) as u32 });
        self.out = Some(out);
        Ok(())
    }
    fn finish(&mut self) -> R<(Vec<(String, u64)>, Fields)> {
        let mut out = self.out.take().ok_or("frame still open")?;
        out.flush()?;
        let mut w = BufWriter::new(File::create(self.dir.join("zl.idx"))?);
        w.write_all(MAGIC)?;
        w.write_all(&(self.traces.len() as u64).to_le_bytes())?;
        w.write_all(&self.rows.to_le_bytes())?;
        for t in &self.traces {
            w.write_all(&t.id)?;
            for x in [t.off, t.len, t.raw, t.first] {
                w.write_all(&x.to_le_bytes())?;
            }
            w.write_all(&t.spans.to_le_bytes())?;
        }
        for o in &self.span_offs {
            w.write_all(&o.to_le_bytes())?;
        }
        w.flush()?;
        let sz = |n: &str| std::fs::metadata(self.dir.join(n)).map(|m| m.len());
        Ok((vec![("zl.data".into(), sz("zl.data")?), ("zl.idx".into(), sz("zl.idx")?)], vec![]))
    }
}

pub struct ZlStore {
    file: File,
    traces: Vec<Trace>,
    by_id: HashMap<[u8; 16], usize>,
    span_offs: Vec<u64>,
    scratch: Vec<u8>,
}

type Frame = Decoder<'static, BufReader<std::io::Take<File>>>;

impl ZlStore {
    pub fn open(dir: &Path) -> R<Self> {
        let mut b = vec![];
        File::open(dir.join("zl.idx"))?.read_to_end(&mut b)?;
        if b.len() < 20 || &b[..4] != MAGIC {
            return Err("bad zl.idx".into());
        }
        let (nt, ns) = (le64(&b, 4) as usize, le64(&b, 12) as usize);
        if b.len() != 20 + nt * TREC + ns * 8 {
            return Err("zl.idx has the wrong size".into());
        }
        let traces: Vec<Trace> = (0..nt)
            .map(|i| {
                let r = 20 + i * TREC;
                Trace { id: b[r..r + 16].try_into().expect("16 bytes"), off: le64(&b, r + 16), len: le64(&b, r + 24), raw: le64(&b, r + 32), first: le64(&b, r + 40), spans: le32(&b, r + 48) }
            })
            .collect();
        let span_offs = (0..ns).map(|i| le64(&b, 20 + nt * TREC + i * 8)).collect();
        let by_id = traces.iter().enumerate().map(|(i, t)| (t.id, i)).collect();
        Ok(Self { file: File::open(dir.join("zl.data"))?, traces, by_id, span_offs, scratch: vec![] })
    }

    fn frame(&self, t: &Trace) -> R<Frame> {
        let mut f = self.file.try_clone()?;
        f.seek(SeekFrom::Start(t.off))?;
        let mut d = Decoder::with_buffer(BufReader::with_capacity(256 << 10, f.take(t.len)))?;
        d.window_log_max(WINDOW_LOG)?; // the decoder refuses frames with a larger window than its limit
        Ok(d)
    }

    fn skip(d: &mut Frame, mut n: u64, buf: &mut Vec<u8>) -> R<()> {
        buf.resize(1 << 20, 0);
        while n > 0 {
            let k = (n as usize).min(buf.len());
            d.read_exact(&mut buf[..k])?;
            n -= k as u64;
        }
        Ok(())
    }

    /// Reads the six attributes of the next span from the decoder.
    fn span(d: &mut Frame, buf: &mut Vec<u8>, v: &mut dyn Visit) -> R<()> {
        for _ in 0..6 {
            let mut p = [0u8; 1];
            d.read_exact(&mut p)?;
            match p[0] {
                0 => {}
                1 => {
                    let mut l = [0u8; 4];
                    d.read_exact(&mut l)?;
                    buf.resize(u32::from_le_bytes(l) as usize, 0);
                    d.read_exact(buf)?;
                    v.value(buf);
                }
                x => return Err(format!("bad presence byte {x}").into()),
            }
        }
        Ok(())
    }

    fn trace_values(&mut self, ti: usize, v: &mut dyn Visit) -> R<()> {
        let mut d = self.frame(&self.traces[ti])?;
        for _ in 0..self.traces[ti].spans {
            Self::span(&mut d, &mut self.scratch, v)?;
        }
        Ok(())
    }
}

impl Store for ZlStore {
    fn rows(&self) -> u64 {
        self.span_offs.len() as u64
    }
    fn read_trace(&mut self, id: &[u8; 16], v: &mut dyn Visit) -> R<bool> {
        match self.by_id.get(id) {
            None => Ok(false),
            Some(&ti) => self.trace_values(ti, v).map(|_| true),
        }
    }
    fn read_span(&mut self, row: u64, v: &mut dyn Visit) -> R<()> {
        let ti = self.traces.partition_point(|t| t.first <= row) - 1;
        let mut d = self.frame(&self.traces[ti])?;
        Self::skip(&mut d, self.span_offs[row as usize], &mut self.scratch)?;
        Self::span(&mut d, &mut self.scratch, v)
    }
    fn scan(&mut self, v: &mut dyn Visit) -> R<()> {
        for ti in 0..self.traces.len() {
            v.trace(&self.traces[ti].id.clone());
            self.trace_values(ti, v)?;
        }
        Ok(())
    }
}
