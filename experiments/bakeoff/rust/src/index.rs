//! Inverted index over output_messages + tool_call_result.
use crate::query::Table;
use crate::R;
use arrow::array::*;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::io::Write;

/// Byte -> lowercased token byte, or 0 for a separator.
const LUT: [u8; 256] = {
    let mut t = [0u8; 256];
    let mut i = 0;
    while i < 256 {
        let b = i as u8;
        t[i] = match b {
            b'a'..=b'z' | b'0'..=b'9' | 0x80..=0xff => b,
            b'A'..=b'Z' => b + 32,
            _ => 0,
        };
        i += 1;
    }
    t
};

/// Multiplicative hasher; keys are our own tokens so flooding resistance is not needed.
#[derive(Default)]
pub struct Fx(u64);
impl Hasher for Fx {
    fn write(&mut self, bytes: &[u8]) {
        for c in bytes.chunks(8) {
            let mut w = [0u8; 8];
            w[..c.len()].copy_from_slice(c);
            self.0 = (self.0.rotate_left(5) ^ u64::from_le_bytes(w)).wrapping_mul(0x517c_c1b7_2722_0a95);
        }
    }
    fn finish(&self) -> u64 {
        self.0 ^ (self.0 >> 32)
    }
}

#[derive(Default)]
pub struct Index {
    map: HashMap<Vec<u8>, Vec<u32>, BuildHasherDefault<Fx>>,
    cur: Vec<u8>,
    pub postings: u64,
}

impl Index {
    /// Adds one document whose text is the concatenation of `parts` (a token may span a part boundary).
    pub fn add_doc(&mut self, doc: u32, parts: &[&[u8]]) {
        let mut cur = std::mem::take(&mut self.cur);
        for part in parts {
            for &b in *part {
                let c = LUT[b as usize];
                if c != 0 {
                    if cur.len() < 65 {
                        cur.push(c); // 65 bytes means "longer than 64": discarded at the end
                    }
                } else {
                    self.end_token(doc, &mut cur);
                }
            }
        }
        self.end_token(doc, &mut cur);
        self.cur = cur;
    }

    fn end_token(&mut self, doc: u32, cur: &mut Vec<u8>) {
        if !cur.is_empty() && cur.len() <= 64 {
            match self.map.get_mut(cur.as_slice()) {
                Some(v) => {
                    if *v.last().unwrap() != doc {
                        v.push(doc);
                        self.postings += 1;
                    }
                }
                None => {
                    self.map.insert(cur.clone(), vec![doc]);
                    self.postings += 1;
                }
            }
        }
        cur.clear();
    }

    /// Appends `other`, whose documents all come after this index's.
    pub fn merge(&mut self, other: Index) {
        self.postings += other.postings;
        for (tok, mut ids) in other.map {
            self.map.entry(tok).or_default().append(&mut ids);
        }
    }

    pub fn distinct(&self) -> usize {
        self.map.len()
    }

    /// Writes the index in the specified format; returns bytes written.
    pub fn encode(&self, w: &mut impl Write) -> R<u64> {
        let mut entries: Vec<_> = self.map.iter().collect();
        entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
        let (mut buf, mut total) = (Vec::new(), 0u64);
        for (tok, ids) in entries {
            buf.clear();
            varint(&mut buf, tok.len() as u64);
            buf.extend_from_slice(tok);
            varint(&mut buf, ids.len() as u64);
            let mut prev = 0;
            for &id in ids {
                varint(&mut buf, (id - prev) as u64);
                prev = id;
            }
            w.write_all(&buf)?;
            total += buf.len() as u64;
        }
        Ok(total)
    }
}

pub fn varint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push(v as u8 | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

struct HashWriter<W>(W, Sha256);
impl<W: Write> Write for HashWriter<W> {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.1.update(b);
        self.0.write(b)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.0.flush()
    }
}

pub struct IndexReport {
    pub text_bytes: u64,
    pub docs: u64,
    pub distinct: usize,
    pub postings: u64,
    pub index_bytes: u64,
    pub sha: String,
}

/// Indexes the text of one row group into `ix`, numbering documents from `doc`.
fn index_group(t: &Table, rg: usize, mut doc: u32, ix: &mut Index) -> R<u64> {
    let mut text = 0;
    for b in t.group_iter(rg, &["output_messages", "tool_call_result"], 256)? {
        let b = b?;
        let (o, r) = (b.column_by_name("output_messages").unwrap().as_binary::<i64>(), b.column_by_name("tool_call_result").unwrap().as_binary::<i64>());
        for i in 0..b.num_rows() {
            let (x, y): (&[u8], &[u8]) = (if o.is_valid(i) { o.value(i) } else { b"" }, if r.is_valid(i) { r.value(i) } else { b"" });
            text += (x.len() + y.len()) as u64;
            ix.add_doc(doc, &[x, y]);
            doc += 1;
        }
    }
    Ok(text)
}

/// Reads payloads.parquet one row group at a time. Each worker indexes a contiguous run of row groups into its own
/// index; the partial indexes are merged in document order, so the output does not depend on `threads`.
pub fn build(t: &Table, out: &std::path::Path, threads: usize) -> R<IndexReport> {
    let n = t.group_count();
    let per = n.div_ceil(threads.max(1)).max(1);
    let parts: Vec<R<(Index, u64, u32)>> = std::thread::scope(|s| {
        let hs: Vec<_> = (0..n.div_ceil(per))
            .map(|w| {
                s.spawn(move || -> R<(Index, u64, u32)> {
                    let (lo, hi) = (w * per, ((w + 1) * per).min(n));
                    let first: u32 = (0..lo).map(|g| t.group_rows(g) as u32).sum();
                    let (mut ix, mut text, mut doc) = (Index::default(), 0, first);
                    for rg in lo..hi {
                        text += index_group(t, rg, doc, &mut ix)?;
                        doc += t.group_rows(rg) as u32;
                    }
                    Ok((ix, text, doc))
                })
            })
            .collect();
        hs.into_iter().map(|h| h.join().expect("index worker")).collect()
    });
    let (mut ix, mut text, mut docs) = (Index::default(), 0u64, 0u32);
    for p in parts {
        let (part, tx, end) = p?;
        ix.merge(part);
        text += tx;
        docs = end;
    }
    let mut w = HashWriter(std::io::BufWriter::with_capacity(1 << 20, std::fs::File::create(out)?), Sha256::new());
    let index_bytes = ix.encode(&mut w)?;
    w.flush()?;
    let sha = w.1.finalize().iter().map(|b| format!("{b:02x}")).collect();
    Ok(IndexReport { text_bytes: text, docs: docs as u64, distinct: ix.distinct(), postings: ix.postings, index_bytes, sha })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(docs: &[&[&[u8]]]) -> Vec<u8> {
        let mut ix = Index::default();
        for (i, d) in docs.iter().enumerate() {
            ix.add_doc(i as u32, d);
        }
        let mut out = vec![];
        ix.encode(&mut out).unwrap();
        out
    }

    #[test]
    fn worked_example() {
        let got = encoded(&[&[b"Error: file_not_found (code 404)"], &[b"error error OK"], &[b"404"]]);
        let want: Vec<u8> = [
            "03 34 30 34 02 00 02",
            "04 63 6f 64 65 01 00",
            "05 65 72 72 6f 72 02 00 01",
            "04 66 69 6c 65 01 00",
            "05 66 6f 75 6e 64 01 00",
            "03 6e 6f 74 01 00",
            "02 6f 6b 01 01",
        ]
        .iter()
        .flat_map(|l| l.split(' ').map(|h| u8::from_str_radix(h, 16).unwrap()))
        .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn tokenizer_rules() {
        // high bytes are token bytes, 64 is the longest kept token, parts join without a separator
        let long65 = vec![b'a'; 65];
        let long64 = vec![b'b'; 64];
        let got = encoded(&[&[&long65, b" ", &long64, b" caf\xc3\xa9 \xc3\x89"], &[b"ab", b"cd ef"]]);
        let mut want = vec![4, b'a', b'b', b'c', b'd', 1, 1];
        want.push(64);
        want.extend(&long64);
        want.extend([1, 0]);
        want.extend([5, b'c', b'a', b'f', 0xc3, 0xa9, 1, 0]);
        want.extend([2, b'e', b'f', 1, 1]);
        want.extend([2, 0xc3, 0x89, 1, 0]);
        assert_eq!(got, want);
    }

    #[test]
    fn merge_matches_single_pass() {
        let docs: [&[u8]; 4] = [b"a b", b"b c", b"a c a", b"d"];
        let whole = encoded(&docs.map(|d| [d]).iter().map(|d| &d[..]).collect::<Vec<_>>());
        let (mut x, mut y) = (Index::default(), Index::default());
        docs[..2].iter().enumerate().for_each(|(i, d)| x.add_doc(i as u32, &[d]));
        docs[2..].iter().enumerate().for_each(|(i, d)| y.add_doc(i as u32 + 2, &[d]));
        x.merge(y);
        let mut out = vec![];
        x.encode(&mut out).unwrap();
        assert_eq!((out, x.postings), (whole, 7));
    }

    #[test]
    fn varint_multibyte() {
        let mut b = vec![];
        varint(&mut b, 300);
        varint(&mut b, 0);
        varint(&mut b, 127);
        varint(&mut b, 128);
        assert_eq!(b, [0xac, 0x02, 0, 0x7f, 0x80, 0x01]);
        // delta encoding of ids across a multibyte gap
        let mut ix = Index::default();
        ix.add_doc(0, &[b"x"]);
        ix.add_doc(1000, &[b"x"]);
        let mut out = vec![];
        ix.encode(&mut out).unwrap();
        assert_eq!(out, [1, b'x', 2, 0, 0xe8, 0x07]);
    }
}
