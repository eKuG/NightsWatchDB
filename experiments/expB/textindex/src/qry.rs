//! The query set: model, file format, and derivation from the piece index.
use crate::seg::*;
use crate::store::Store;
use crate::tok::tokenize;
use crate::util::*;
use std::collections::HashSet;
use std::path::Path;

#[derive(Clone, Debug, PartialEq)]
pub enum Op {
    Term,
    And,
    Or,
    Not,
    Prefix,
    Phrase,
}

#[derive(Clone, Debug)]
pub struct Query {
    pub id: String,
    pub class: String,
    /// terms for term/and/or/not (in written order), the prefix for prefix, the two tokens for phrase
    pub args: Vec<String>,
}

pub const CLASSES: [&str; 9] = ["rare", "mid", "common", "and2", "and3", "or2", "not", "prefix", "phrase"];

impl Query {
    pub fn op(&self) -> R<Op> {
        Ok(match self.class.as_str() {
            "rare" | "mid" | "common" => Op::Term,
            "and2" | "and3" => Op::And,
            "or2" => Op::Or,
            "not" => Op::Not,
            "prefix" => Op::Prefix,
            "phrase" => Op::Phrase,
            c => return Err(format!("unknown query class {c}").into()),
        })
    }
}

// ---- file format ----

fn json_of(q: &Query) -> String {
    let key = match q.class.as_str() {
        "prefix" => "prefix",
        "phrase" => "phrase",
        _ => "terms",
    };
    let val = if key == "prefix" { json_str(&q.args[0]) } else { arr(&q.args, |s| json_str(s)) };
    format!("{{\"id\":{},\"class\":{},\"{key}\":{val}}}", json_str(&q.id), json_str(&q.class))
}

fn json_str(s: &str) -> String {
    q(s)
}

pub fn write_queries(path: &Path, qs: &[Query]) -> R<()> {
    let body = qs.iter().map(|x| format!("  {}", json_of(x))).collect::<Vec<_>>().join(",\n");
    std::fs::write(path, format!("{{\"queries\":[\n{body}\n]}}\n"))?;
    Ok(())
}

#[derive(Debug)]
enum J {
    Obj(Vec<(String, J)>),
    Arr(Vec<J>),
    Str(String),
    Other,
}

struct P<'a> {
    b: &'a [u8],
    i: usize,
}
impl P<'_> {
    fn ws(&mut self) {
        while self.i < self.b.len() && self.b[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }
    fn eat(&mut self, c: u8) -> R<()> {
        self.ws();
        if self.b.get(self.i) == Some(&c) {
            self.i += 1;
            Ok(())
        } else {
            Err(format!("json: expected {:?} at byte {}", c as char, self.i).into())
        }
    }
    fn string(&mut self) -> R<String> {
        self.eat(b'"')?;
        let mut out = vec![];
        loop {
            let c = *self.b.get(self.i).ok_or("json: unterminated string")?;
            self.i += 1;
            match c {
                b'"' => return Ok(String::from_utf8(out)?),
                b'\\' => {
                    let e = *self.b.get(self.i).ok_or("json: bad escape")?;
                    self.i += 1;
                    match e {
                        b'n' => out.push(b'\n'),
                        b't' => out.push(b'\t'),
                        b'r' => out.push(b'\r'),
                        b'u' => {
                            let h = std::str::from_utf8(self.b.get(self.i..self.i + 4).ok_or("json: bad \\u")?)?;
                            let c = char::from_u32(u32::from_str_radix(h, 16)?).unwrap_or('\u{fffd}');
                            out.extend_from_slice(c.to_string().as_bytes());
                            self.i += 4;
                        }
                        e => out.push(e),
                    }
                }
                c => out.push(c),
            }
        }
    }
    fn value(&mut self) -> R<J> {
        self.ws();
        match self.b.get(self.i).ok_or("json: unexpected end")? {
            b'{' => {
                self.i += 1;
                let mut m = vec![];
                self.ws();
                if self.b.get(self.i) == Some(&b'}') {
                    self.i += 1;
                    return Ok(J::Obj(m));
                }
                loop {
                    let k = self.string()?;
                    self.eat(b':')?;
                    m.push((k, self.value()?));
                    self.ws();
                    match self.b.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b'}') => {
                            self.i += 1;
                            return Ok(J::Obj(m));
                        }
                        _ => return Err("json: bad object".into()),
                    }
                }
            }
            b'[' => {
                self.i += 1;
                let mut v = vec![];
                self.ws();
                if self.b.get(self.i) == Some(&b']') {
                    self.i += 1;
                    return Ok(J::Arr(v));
                }
                loop {
                    v.push(self.value()?);
                    self.ws();
                    match self.b.get(self.i) {
                        Some(b',') => self.i += 1,
                        Some(b']') => {
                            self.i += 1;
                            return Ok(J::Arr(v));
                        }
                        _ => return Err("json: bad array".into()),
                    }
                }
            }
            b'"' => Ok(J::Str(self.string()?)),
            _ => {
                while self.i < self.b.len() && !b",]} \n\t\r".contains(&self.b[self.i]) {
                    self.i += 1;
                }
                Ok(J::Other)
            }
        }
    }
}

pub fn read_queries(path: &Path) -> R<Vec<Query>> {
    let text = std::fs::read(path)?;
    let J::Obj(top) = (P { b: &text, i: 0 }).value()? else { return Err("queries file: not an object".into()) };
    let Some((_, J::Arr(items))) = top.into_iter().find(|(k, _)| k == "queries") else { return Err("queries file: no \"queries\" array".into()) };
    let mut out = vec![];
    for it in items {
        let J::Obj(m) = it else { return Err("queries file: bad entry".into()) };
        let (mut id, mut class, mut args) = (String::new(), String::new(), vec![]);
        for (k, v) in m {
            match (k.as_str(), v) {
                ("id", J::Str(s)) => id = s,
                ("class", J::Str(s)) => class = s,
                ("prefix", J::Str(s)) => args = vec![s],
                ("terms" | "phrase", J::Arr(a)) => args = a.into_iter().map(|x| if let J::Str(s) = x { s } else { String::new() }).collect(),
                _ => {}
            }
        }
        let qy = Query { id, class, args };
        let want = match qy.op()? {
            Op::Term | Op::Prefix => 1,
            Op::And => if qy.class == "and2" { 2 } else { 3 },
            Op::Or | Op::Not | Op::Phrase => 2,
        };
        if qy.args.len() != want || qy.args.iter().any(|a| a.is_empty()) {
            return Err(format!("query {}: expected {want} non-empty arguments", qy.id).into());
        }
        out.push(qy);
    }
    Ok(out)
}

// ---- derivation ----

fn candidate(t: &[u8]) -> bool {
    (4..=20).contains(&t.len()) && t.iter().all(u8::is_ascii_lowercase)
}

/// The 10 candidates closest to `target` by (|df - target|, term).
struct Best {
    target: u64,
    v: Vec<(u64, Vec<u8>)>,
}
impl Best {
    fn offer(&mut self, term: &[u8], df: u64) {
        let d = df.abs_diff(self.target);
        if self.v.len() == 10 && (d, term) >= (self.v[9].0, self.v[9].1.as_slice()) {
            return;
        }
        self.v.push((d, term.to_vec()));
        self.v.sort();
        self.v.truncate(10);
    }
}

/// Builds the fixed query set from the piece index (`segs`) and the store.
pub fn derive(segs: &[Segment], store: &mut Store, seed: u64) -> R<Vec<Query>> {
    let np = store.pieces() as u64;
    // targets: df 2, 0.1% and 5% of the pieces, rounded to the nearest piece count
    let targets = [2, (np as f64 * 0.001).round() as u64, (np as f64 * 0.05).round() as u64];
    let mut best: Vec<Best> = targets.iter().map(|&t| Best { target: t, v: vec![] }).collect();
    merge_terms(segs, &mut |t, df, _| {
        if candidate(t) {
            best.iter_mut().for_each(|b| b.offer(t, df));
        }
    })?;
    let words: Vec<Vec<String>> = best.iter().map(|b| b.v.iter().map(|(_, t)| String::from_utf8_lossy(t).into_owned()).collect()).collect();
    if words.iter().any(|w| w.len() < 10) {
        return Err("fewer than 10 candidate terms for a df target; the store is too small".into());
    }
    let (rare, mid, common) = (&words[0], &words[1], &words[2]);
    let mut out = vec![];
    let mut push = |class: &str, i: usize, args: Vec<String>| out.push(Query { id: format!("{class}-{i}"), class: class.into(), args });
    for i in 0..10 {
        push("rare", i, vec![rare[i].clone()]);
    }
    for i in 0..10 {
        push("mid", i, vec![mid[i].clone()]);
    }
    for i in 0..10 {
        push("common", i, vec![common[i].clone()]);
    }
    for i in 0..10 {
        push("and2", i, vec![mid[i].clone(), common[i].clone()]);
    }
    for i in 0..10 {
        push("and3", i, vec![mid[i].clone(), common[i].clone(), common[(i + 1) % 10].clone()]);
    }
    for i in 0..10 {
        push("or2", i, vec![rare[i].clone(), mid[i].clone()]);
    }
    for i in 0..10 {
        push("not", i, vec![common[i].clone(), mid[i].clone()]);
    }
    for i in 0..10 {
        push("prefix", i, vec![mid[i][..4].to_string()]);
    }
    // phrase: pick a random piece and position; keep two consecutive candidate tokens
    let (mut rng, mut seen, mut got, mut tries) = (Rng(seed), HashSet::new(), vec![], 0u64);
    while got.len() < 10 {
        tries += 1;
        if tries > 5_000_000 {
            return Err("could not find 10 distinct phrases".into());
        }
        let piece = store.piece((rng.next() % np) as u32)?;
        let mut n = 0u64;
        tokenize(piece.bytes(), |_, _| n += 1);
        if n < 2 {
            continue;
        }
        let at = rng.next() % (n - 1);
        let (mut i, mut pair): (u64, [Option<Vec<u8>>; 2]) = (0, [None, None]);
        tokenize(piece.bytes(), |t, long| {
            if i == at || i == at + 1 {
                pair[(i - at) as usize] = (!long && candidate(t)).then(|| t.to_vec());
            }
            i += 1;
        });
        if let [Some(a), Some(b)] = pair {
            if seen.insert((a.clone(), b.clone())) {
                got.push((String::from_utf8_lossy(&a).into_owned(), String::from_utf8_lossy(&b).into_owned()));
            }
        }
    }
    for (i, (a, b)) in got.into_iter().enumerate() {
        push("phrase", i, vec![a, b]);
    }
    Ok(out)
}
