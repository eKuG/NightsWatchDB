use crate::build;
use crate::codec::*;
use crate::qry::*;
use crate::scan::scan;
use crate::search::*;
use crate::seg::*;
use crate::store::Store;
use crate::tok::*;
use crate::util::*;
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

fn tmp(name: &str) -> PathBuf {
    let d = std::env::temp_dir().join(format!("textindex-test-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    std::fs::create_dir_all(&d).unwrap();
    d
}

// ---- tokeniser ----

fn toks(b: &[u8]) -> Vec<Vec<u8>> {
    let mut v = vec![];
    tokenize(b, |t, long| v.push(if long { b"<long>".to_vec() } else { t.to_vec() }));
    v
}

#[test]
fn tokenizer_worked_example() {
    // the bake-off example, encoded in the bake-off format
    let docs: [&[u8]; 3] = [b"Error: file_not_found (code 404)", b"error error OK", b"404"];
    let mut m: BTreeMap<Vec<u8>, Vec<u32>> = BTreeMap::new();
    for (i, d) in docs.iter().enumerate() {
        tokenize(d, |t, _| {
            let l = m.entry(t.to_vec()).or_default();
            if l.last() != Some(&(i as u32)) {
                l.push(i as u32);
            }
        });
    }
    let mut got = vec![];
    for (t, ids) in &m {
        put_varint(&mut got, t.len() as u64);
        got.extend(t);
        put_varint(&mut got, ids.len() as u64);
        let mut prev = 0;
        for &id in ids {
            put_varint(&mut got, (id - prev) as u64);
            prev = id;
        }
    }
    let want: Vec<u8> = ["03 34 30 34 02 00 02", "04 63 6f 64 65 01 00", "05 65 72 72 6f 72 02 00 01", "04 66 69 6c 65 01 00", "05 66 6f 75 6e 64 01 00", "03 6e 6f 74 01 00", "02 6f 6b 01 01"]
        .iter()
        .flat_map(|l| l.split(' ').map(|h| u8::from_str_radix(h, 16).unwrap()))
        .collect();
    assert_eq!(got, want);
}

#[test]
fn tokenizer_rules() {
    assert_eq!(toks(b"Caf\xc3\xa9 X-1 \xc3\x89"), vec![b"caf\xc3\xa9".to_vec(), b"x".to_vec(), b"1".to_vec(), b"\xc3\x89".to_vec()]);
    let (a64, a65) = (vec![b'a'; 64], vec![b'a'; 65]);
    assert_eq!(toks(&a64), vec![a64.clone()]);
    assert_eq!(toks(&a65), vec![b"<long>".to_vec()]);
    let mut t = a65.clone();
    t.extend(b" ok ");
    t.extend(&a64);
    assert_eq!(toks(&t), vec![b"<long>".to_vec(), b"ok".to_vec(), a64.clone()]);
    assert!(toks(b"").is_empty() && toks(b" \x00,[]").is_empty());
    // a long token still separates the tokens around it for adjacency
    let mut p = b"foo ".to_vec();
    p.extend(&a65);
    p.extend(b" bar foo bar");
    assert!(has_phrase(&p, b"foo", b"bar"));
    assert!(!has_phrase(&p[..p.len() - 8], b"foo", b"bar"));
    assert!(has_phrase(b"x foo-FOO y", b"foo", b"foo"));
    assert!(!has_phrase(b"foo", b"foo", b"foo"));
}

// ---- codecs ----

fn lcg(s: &mut u64) -> u32 {
    *s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
    (*s >> 33) as u32
}

#[test]
fn varints() {
    let mut b = vec![];
    for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
        put_varint(&mut b, v);
    }
    let mut p = 0;
    for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
        assert_eq!(get_varint(&b, &mut p), v);
    }
}

#[test]
fn bitpacking_widths_and_lengths() {
    let mut s = 7u64;
    for w in [0u8, 1, 7, 8, 17, 32] {
        for n in [1usize, 8, 9, 128, 129, 1000] {
            let max = if w == 0 { 0 } else { ((1u64 << w) - 1) as u32 };
            let mut vals: Vec<u32> = (0..n).map(|_| if w == 0 { 0 } else { (lcg(&mut s) as u64 * 7 % (max as u64 + 1)) as u32 }).collect();
            vals[n / 2] = max;
            assert_eq!(width_of(&vals), w);
            let mut b = vec![];
            pack(&vals, w, &mut b);
            assert_eq!(b.len(), (n * w as usize).div_ceil(8));
            let mut back = vec![0u32; n];
            unpack(&b, w, &mut back);
            assert_eq!(back, vals, "w={w} n={n}");
        }
    }
}

/// A one-term segment file for list tests.
fn one_list(dir: &Path, ids: &[u32]) -> Segment {
    let p = dir.join("one.tix");
    write_segment(&p, 0, u32::MAX, &mut std::iter::once((&b"t"[..], ids))).unwrap();
    Segment::open(&p).unwrap()
}

#[test]
fn posting_lists_all_widths_and_lengths() {
    let d = tmp("lists");
    let mut s = 99u64;
    for w in [0u8, 1, 7, 8, 17, 32] {
        for n in [1usize, 8, 9, 128, 129, 1000] {
            // gaps below 2^w, one of them exactly 2^w - 1; for width 32 the first id is large
            let mut ids = vec![];
            let mut cur: u32 = if w == 32 { 0xF000_0000 } else { 0 };
            for i in 0..n {
                let g = if w == 0 || w == 32 { 0 } else if i == n / 2 { (1u32 << w) - 1 } else { lcg(&mut s) % (1u32 << w) };
                if i > 0 || w != 32 {
                    cur = if i == 0 { g } else { cur + g + 1 };
                }
                ids.push(cur);
            }
            if ids.windows(2).any(|p| p[0] >= p[1]) {
                continue; // overflow of the generator, not of the codec
            }
            let seg = one_list(&d, &ids);
            let e = seg.lookup(b"t").unwrap().unwrap();
            assert_eq!(e.df as usize, n);
            assert_eq!(seg.list(&e).unwrap(), ids, "w={w} n={n}");
            // cursor: step through every id, and jump over gaps
            let mut c = seg.cursor(&e).unwrap();
            for &id in &ids {
                assert_eq!(c.seek(id).unwrap(), Some(id));
            }
            assert_eq!(c.seek(u32::MAX).unwrap().filter(|&x| x < u32::MAX), None);
            let mut c = seg.cursor(&e).unwrap();
            let mut got = vec![];
            let mut t = 0;
            while let Some(x) = c.seek(t).unwrap() {
                got.push(x);
                t = x + 1;
            }
            assert_eq!(got, ids);
        }
    }
    // wide-open check of a block's width byte: 128 consecutive ids need width 0
    let mut b = vec![];
    encode_list(&(5..5 + 128).collect::<Vec<u32>>(), &mut b);
    assert_eq!(b[SKIP], 3, "first gap is 5 so width is 3");
    let mut b = vec![];
    encode_list(&(0..128).collect::<Vec<u32>>(), &mut b);
    assert_eq!((b[SKIP], b.len()), (0, SKIP + 1), "consecutive ids from 0 need no bits");
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn skip_data_is_used_by_intersection() {
    let d = tmp("skip");
    let big: Vec<u32> = (0..100_000).map(|i| i * 3).collect();
    let small = vec![9u32, 30_000, 150_000, 299_997, 299_998];
    let p = d.join("s.tix");
    let mut terms = vec![(&b"big"[..], &big[..]), (&b"small"[..], &small[..])];
    terms.sort();
    write_segment(&p, 0, 300_000, &mut terms.into_iter()).unwrap();
    let seg = Segment::open(&p).unwrap();
    let mut cs = vec![seg.cursor(&seg.lookup(b"big").unwrap().unwrap()).unwrap(), seg.cursor(&seg.lookup(b"small").unwrap().unwrap()).unwrap()];
    let got = intersect(&mut cs).unwrap();
    let want: Vec<u32> = small.iter().copied().filter(|x| x % 3 == 0).collect();
    assert_eq!(got, want);
    let blocks_total = 100_000usize.div_ceil(BLOCK) as u64;
    let big_c = cs.iter().find(|c| c.df == 100_000).unwrap();
    assert!(big_c.blocks_read <= 5 && big_c.blocks_read < blocks_total / 100, "read {} of {blocks_total} blocks", big_c.blocks_read);
    // three lists, one empty intersection
    let mut cs = vec![seg.cursor(&seg.lookup(b"big").unwrap().unwrap()).unwrap(), seg.cursor(&seg.lookup(b"small").unwrap().unwrap()).unwrap(), seg.cursor(&seg.lookup(b"small").unwrap().unwrap()).unwrap()];
    assert_eq!(intersect(&mut cs).unwrap(), want);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn front_coding_round_trip() {
    let terms: Vec<&[u8]> = vec![b"a", b"ab", b"abc", b"abcd", b"b", b"ba", b"bat", b"bata", b"c", b"cat", b"caterpillar", b"d", b"zzz"];
    let (mut buf, mut prev) = (vec![], vec![]);
    for t in &terms {
        put_term(&mut buf, &prev, t);
        prev = t.to_vec();
    }
    let (mut p, mut cur, mut back) = (0, vec![], vec![]);
    while p < buf.len() {
        get_term(&buf, &mut p, &mut cur);
        back.push(cur.clone());
    }
    assert_eq!(back, terms);
    // shared prefix 0 ("b" after "abcd") and the whole previous term ("ab" after "a") are in the list;
    // check the encoded form of both: (shared, suffix length)
    let mut b = vec![];
    put_term(&mut b, b"abcd", b"b");
    assert_eq!(b, [0, 1, b'b']);
    let mut b = vec![];
    put_term(&mut b, b"ab", b"abcd");
    assert_eq!(b, [2, 2, b'c', b'd']);
}

#[test]
fn dictionary_lookup_prefix_over_many_blocks() {
    let d = tmp("dict");
    let mut words: Vec<Vec<u8>> = (0..1000u32).map(|i| format!("w{:03}x{}", i / 7, i % 7).into_bytes()).collect();
    words.extend((0..40).map(|i| format!("zz{i:02}").into_bytes()));
    words.sort();
    let lists: Vec<Vec<u32>> = (0..words.len()).map(|i| (0..(i % 20) as u32 + 1).map(|k| k * 5 + i as u32 % 3).collect()).collect();
    let p = d.join("d.tix");
    write_segment(&p, 3, 500, &mut words.iter().zip(&lists).map(|(w, l)| (w.as_slice(), l.as_slice()))).unwrap();
    let seg = Segment::open(&p).unwrap();
    assert_eq!((seg.doc_lo, seg.doc_hi), (3, 500));
    for (w, l) in words.iter().zip(&lists) {
        let e = seg.lookup(w).unwrap().unwrap();
        assert_eq!(&seg.list(&e).unwrap(), l);
    }
    for miss in [&b"a"[..], b"w000", b"w999x9", b"zzzz", b"~"] {
        assert!(seg.lookup(miss).unwrap().is_none());
    }
    for pre in [&b"w0"[..], b"w10", b"w142x", b"zz", b"zz1", b"w", b"q", b"w999"] {
        let want: Vec<&Vec<u32>> = words.iter().zip(&lists).filter(|(w, _)| w.starts_with(pre)).map(|(_, l)| l).collect();
        let got: Vec<Vec<u32>> = seg.prefix(pre).unwrap().iter().map(|e| seg.list(e).unwrap()).collect();
        assert_eq!(got.iter().collect::<Vec<_>>(), want, "prefix {:?}", String::from_utf8_lossy(pre));
    }
    let mut n = 0;
    merge_terms(std::slice::from_ref(&seg), &mut |_, _, _| n += 1).unwrap();
    assert_eq!(n, words.len());
    let _ = std::fs::remove_dir_all(&d);
}

// ---- synthetic store and brute force ----

enum V {
    Absent,
    Whole(Vec<u8>),
    Arr(Vec<Vec<u8>>),
}

/// Writes a dedup store (formats in expA/payload/README.md) with small blocks.
fn write_store(dir: &Path, rows: &[[V; 6]], per_trace: usize, block: usize) {
    let (mut ids, mut pieces): (HashMap<Vec<u8>, u32>, Vec<Vec<u8>>) = (HashMap::new(), vec![]);
    let mut refs = vec![];
    let mut offs = vec![];
    let mut intern = |b: &[u8]| -> u32 {
        *ids.entry(b.to_vec()).or_insert_with(|| {
            pieces.push(b.to_vec());
            pieces.len() as u32 - 1
        })
    };
    for r in rows {
        offs.push(refs.len() as u64);
        for v in r {
            match v {
                V::Absent => refs.extend(0xFFFF_FFFFu32.to_le_bytes()),
                V::Whole(b) => {
                    refs.extend(0x4000_0000u32.to_le_bytes());
                    refs.extend(intern(b).to_le_bytes());
                }
                V::Arr(a) => {
                    refs.extend((a.len() as u32).to_le_bytes());
                    for b in a {
                        refs.extend(intern(b).to_le_bytes());
                    }
                }
            }
        }
    }
    offs.push(refs.len() as u64);
    let (mut blocks, mut btab, mut ptab, mut cur) = (vec![], vec![], vec![], vec![]);
    let flush = |cur: &mut Vec<u8>, blocks: &mut Vec<u8>, btab: &mut Vec<(u64, u32, u32)>| {
        if !cur.is_empty() {
            let c = zstd::bulk::compress(cur, 1).unwrap();
            btab.push((blocks.len() as u64, c.len() as u32, cur.len() as u32));
            blocks.extend(c);
            cur.clear();
        }
    };
    for p in &pieces {
        if !cur.is_empty() && cur.len() + p.len() > block {
            flush(&mut cur, &mut blocks, &mut btab);
        }
        ptab.push([btab.len() as u32, cur.len() as u32, p.len() as u32]);
        cur.extend(p);
    }
    flush(&mut cur, &mut blocks, &mut btab);
    let ntr = rows.len().div_ceil(per_trace);
    let mut idx = b"DDX1".to_vec();
    for n in [ntr, rows.len(), pieces.len(), btab.len()] {
        idx.extend((n as u64).to_le_bytes());
    }
    for t in 0..ntr {
        idx.extend([t as u8 + 1; 16]);
        idx.extend(((t * per_trace) as u64).to_le_bytes());
        idx.extend((per_trace.min(rows.len() - t * per_trace) as u32).to_le_bytes());
    }
    offs.iter().for_each(|o| idx.extend(o.to_le_bytes()));
    ptab.iter().flatten().for_each(|x| idx.extend(x.to_le_bytes()));
    for (o, c, r) in &btab {
        idx.extend(o.to_le_bytes());
        idx.extend(c.to_le_bytes());
        idx.extend(r.to_le_bytes());
    }
    std::fs::write(dir.join("dd.idx"), idx).unwrap();
    std::fs::write(dir.join("dd.refs"), refs).unwrap();
    std::fs::write(dir.join("dd.blocks"), blocks).unwrap();
}

fn naive(b: &[u8]) -> Vec<Option<Vec<u8>>> {
    let (mut out, mut cur) = (vec![], vec![]);
    for &c in b.iter().chain(std::iter::once(&0u8)) {
        let m = c.to_ascii_lowercase();
        if m.is_ascii_lowercase() || m.is_ascii_digit() || m >= 0x80 {
            cur.push(m);
        } else if !cur.is_empty() {
            out.push(if cur.len() > 64 { None } else { Some(std::mem::take(&mut cur)) });
            cur.clear();
        }
    }
    out
}

fn pieces_of(r: &[V; 6]) -> Vec<&[u8]> {
    r.iter()
        .flat_map(|v| match v {
            V::Absent => vec![],
            V::Whole(b) => vec![b.as_slice()],
            V::Arr(a) => a.iter().map(|x| x.as_slice()).collect(),
        })
        .collect()
}

/// The span text: values in order, arrays as `[` + elements joined by `,` + `]`, one 0x00 between attributes.
fn text_of(r: &[V; 6]) -> Vec<u8> {
    let parts: Vec<Vec<u8>> = r
        .iter()
        .map(|v| match v {
            V::Absent => vec![],
            V::Whole(b) => b.clone(),
            V::Arr(a) => [b"[".to_vec(), a.join(&b","[..]), b"]".to_vec()].concat(),
        })
        .collect();
    parts.join(&0u8)
}

fn brute(rows: &[[V; 6]], q: &Query) -> Vec<u32> {
    let mut out = vec![];
    for (i, r) in rows.iter().enumerate() {
        let set: Vec<Vec<u8>> = naive(&text_of(r)).into_iter().flatten().collect();
        let has = |t: &str| set.iter().any(|x| x == t.as_bytes());
        let a = &q.args;
        let hit = match q.op().unwrap() {
            Op::Term => has(&a[0]),
            Op::And => a.iter().all(|t| has(t)),
            Op::Or => has(&a[0]) || has(&a[1]),
            Op::Not => has(&a[0]) && !has(&a[1]),
            Op::Prefix => set.iter().any(|x| x.starts_with(a[0].as_bytes())),
            Op::Phrase => pieces_of(r).iter().any(|p| naive(p).windows(2).any(|w| w[0].as_deref() == Some(a[0].as_bytes()) && w[1].as_deref() == Some(a[1].as_bytes()))),
        };
        if hit {
            out.push(i as u32);
        }
    }
    out
}

fn synthetic(seed: u64) -> (Vec<[V; 6]>, Vec<String>) {
    let mut rng = Rng(seed);
    let mut vocab: Vec<Vec<u8>> = ["alpha", "Beta", "GAMMA", "delta", "error", "Error", "timeout", "user", "assistant", "tool", "result", "caf\u{e9}", "日本語", "x1", "404", "a", "json"].iter().map(|s| s.as_bytes().to_vec()).collect();
    vocab.push(vec![b'q'; 64]);
    vocab.push(vec![b'q'; 65]);
    vocab.push(vec![b'Q'; 70]);
    for _ in 0..70 {
        let n = 4 + rng.next() as usize % 6;
        vocab.push((0..n).map(|_| b'a' + (rng.next() % 26) as u8).collect());
    }
    let seps: [&[u8]; 8] = [b" ", b", ", b"\"", b"{", b"}:", b"-", b"\n", b"\x00"];
    let mut pool: Vec<Vec<u8>> = vec![];
    for i in 0..45 {
        let n = if i == 0 { 5 } else { rng.next() as usize % 16 };
        let mut p = vec![];
        for _ in 0..n {
            let k = (rng.next() % (1 + rng.next() % vocab.len() as u64)) as usize; // skewed: low ids are common
            p.extend(&vocab[k]);
            p.extend(seps[rng.next() as usize % seps.len()]);
        }
        pool.push(p);
    }
    let pick = |rng: &mut Rng| pool[(rng.next() % pool.len() as u64) as usize].clone();
    let mut rows = vec![];
    for _ in 0..70 {
        let mut r: [V; 6] = Default::default();
        for v in r.iter_mut() {
            *v = match rng.next() % 4 {
                0 => V::Absent,
                1 => V::Whole(pick(&mut rng)),
                _ => V::Arr((0..rng.next() % 4).map(|_| pick(&mut rng)).collect()),
            };
        }
        rows.push(r);
    }
    let words = vocab.iter().filter_map(|w| String::from_utf8(w.clone()).ok()).map(|w| w.to_ascii_lowercase()).collect();
    (rows, words)
}
impl Default for V {
    fn default() -> Self {
        V::Absent
    }
}

fn test_queries(rows: &[[V; 6]], words: &[String]) -> Vec<Query> {
    let mut rng = Rng(5);
    let w = |rng: &mut Rng| words[(rng.next() % (1 + rng.next() % words.len() as u64)) as usize].clone();
    let mut out = vec![];
    for (class, n) in [("rare", 1), ("common", 1), ("and2", 2), ("and3", 3), ("or2", 2), ("not", 2)] {
        for i in 0..12 {
            out.push(Query { id: format!("{class}-{i}"), class: class.into(), args: (0..n).map(|_| w(&mut rng)).collect() });
        }
    }
    out.push(Query { id: "rare-none".into(), class: "rare".into(), args: vec!["zzzzzzzz".into()] });
    out.push(Query { id: "not-none".into(), class: "not".into(), args: vec!["alpha".into(), "zzzzzzzz".into()] });
    for i in 0..12 {
        let x = w(&mut rng);
        let k = if i % 3 == 0 { 1 } else { 2.min(x.len()) };
        let k = (0..=k.min(x.len())).rev().find(|&k| x.is_char_boundary(k) && k > 0).unwrap_or(x.len());
        out.push(Query { id: format!("prefix-{i}"), class: "prefix".into(), args: vec![x[..k].to_string()] });
    }
    // phrases: real adjacent pairs from pieces, shuffled pairs, repeated tokens, pairs across a long token
    let mut real = vec![];
    for r in rows.iter().take(30) {
        for p in pieces_of(r) {
            for win in naive(p).windows(2) {
                if let [Some(a), Some(b)] = win {
                    if let (Ok(a), Ok(b)) = (String::from_utf8(a.clone()), String::from_utf8(b.clone())) {
                        real.push((a, b));
                    }
                }
            }
        }
    }
    for i in 0..30 {
        let (a, b) = if i % 3 == 2 { (w(&mut rng), w(&mut rng)) } else { real[(rng.next() % real.len() as u64) as usize].clone() };
        out.push(Query { id: format!("phrase-{i}"), class: "phrase".into(), args: vec![a, b] });
    }
    out.push(Query { id: "phrase-rep".into(), class: "phrase".into(), args: vec!["error".into(), "error".into()] });
    out
}

fn build_idx(store: &Path, out: &Path, piece: bool, dps: u32, budget: u64) -> build::Report {
    build::run(&build::Opts { store: store.into(), out: out.into(), piece, docs_per_segment: dps, budget, cache: 300 }).unwrap()
}

#[test]
fn all_classes_match_brute_force_both_variants() {
    let d = tmp("e2e");
    let (rows, words) = synthetic(3);
    let sd = d.join("store");
    std::fs::create_dir_all(&sd).unwrap();
    write_store(&sd, &rows, 4, 200);
    let qs = test_queries(&rows, &words);
    let want: Vec<Vec<u32>> = qs.iter().map(|q| brute(&rows, q)).collect();
    assert!(want.iter().filter(|w| !w.is_empty()).count() > qs.len() / 2, "queries should mostly match something");
    assert!(want.iter().any(|w| w.len() > 5));
    // scan agrees with the independent brute force
    let mut st = Store::open(&sd, 300).unwrap();
    let sc = scan(&mut st, &qs).unwrap();
    for (i, q) in qs.iter().enumerate() {
        assert_eq!(sc.rows[i], want[i], "scan {}", q.id);
    }
    let span_text: u64 = rows.iter().map(|r| text_of(r).len() as u64).sum();
    assert_eq!(sc.text_bytes, span_text);
    for (piece, dps, budget) in [(true, 7u32, 1u64 << 30), (true, 1000, 1), (false, 7, 1 << 30), (false, 1000, 1), (false, 70, 1 << 30)] {
        let idx = d.join(format!("idx-{piece}-{dps}-{budget}"));
        let r = build_idx(&sd, &idx, piece, dps, budget);
        if !piece {
            assert_eq!(r.text_bytes, span_text);
        }
        assert_eq!(r.docs, if piece { st.pieces() } else { st.rows() } as u64);
        if budget == 1 {
            assert!(r.segments > r.docs / 2 && r.early_closed == r.segments, "a 1 byte budget closes a segment at almost every document (empty documents hold no memory)");
        }
        let mut s = Searcher::open(&idx, Store::open(&sd, 300).unwrap(), piece).unwrap();
        for (i, q) in qs.iter().enumerate() {
            let o = s.run(q).unwrap();
            assert_eq!(o.rows, want[i], "{} piece={piece} dps={dps} budget={budget}", q.id);
            if q.class == "phrase" {
                assert!(o.candidates.is_some() && o.verified_bytes.is_some());
            }
        }
    }
    // the queries derived from the piece index are valid and answered correctly by both variants
    let segs = open_segments(&d.join("idx-true-7-1073741824")).unwrap();
    let mut st = Store::open(&sd, 300).unwrap();
    let dq = derive(&segs, &mut st, 1).expect("derive on the synthetic store");
    {
        let f = d.join("q.json");
        write_queries(&f, &dq).unwrap();
        let back = read_queries(&f).unwrap();
        assert_eq!(back.len(), 90);
        for q in &back {
            let w = brute(&rows, q);
            for (piece, name) in [(true, "idx-true-7-1073741824"), (false, "idx-false-7-1073741824")] {
                let mut s = Searcher::open(&d.join(name), Store::open(&sd, 300).unwrap(), piece).unwrap();
                assert_eq!(s.run(q).unwrap().rows, w, "derived {}", q.id);
            }
        }
    }
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn queries_round_trip_through_the_file() {
    let d = tmp("qfile");
    let qs = vec![
        Query { id: "rare-0".into(), class: "rare".into(), args: vec!["abcd".into()] },
        Query { id: "and3-0".into(), class: "and3".into(), args: vec!["a1".into(), "b2".into(), "c3".into()] },
        Query { id: "prefix-0".into(), class: "prefix".into(), args: vec!["abcd".into()] },
        Query { id: "phrase-0".into(), class: "phrase".into(), args: vec!["a".into(), "b".into()] },
    ];
    write_queries(&d.join("q.json"), &qs).unwrap();
    let back = read_queries(&d.join("q.json")).unwrap();
    assert_eq!(back.iter().map(|q| (&q.id, &q.class, &q.args)).collect::<Vec<_>>(), qs.iter().map(|q| (&q.id, &q.class, &q.args)).collect::<Vec<_>>());
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn sorted_merges() {
    assert_eq!(union(&[1, 3, 5], &[2, 3, 6, 7]), vec![1, 2, 3, 5, 6, 7]);
    assert_eq!(diff(&[1, 2, 3, 4, 9], &[2, 4, 10]), vec![1, 3, 9]);
    assert_eq!(union_all(vec![vec![5], vec![1, 5], vec![], vec![2, 9]]), vec![1, 2, 5, 9]);
    assert_eq!(bits_to_rows(&[0b101, 1 << 63]), vec![0, 2, 127]);
}
