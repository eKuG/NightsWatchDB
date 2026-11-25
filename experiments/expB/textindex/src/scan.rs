//! `scan`: the answer key. One pass over all rows, rebuilding each span from the store, tokenising
//! it and evaluating every query against that row. No index, no piece_rows.
//! Phrase adjacency is checked inside each piece (tokens cannot span pieces).
use crate::qry::*;
use crate::store::Store;
use crate::tok::tokenize;
use crate::util::*;
use std::path::Path;
use std::time::Instant;

pub struct Scanned {
    pub rows: Vec<Vec<u32>>,
    pub text_bytes: u64,
}

/// Evaluates `qs` over every row. Returns the matching rows per query.
pub fn scan(store: &mut Store, qs: &[Query]) -> R<Scanned> {
    // distinct query terms and prefixes get small ids; per row a stamp says whether one was seen
    let mut term_ids: FxMap<Vec<u8>, usize> = FxMap::default();
    let mut prefix_ids: FxMap<Vec<u8>, usize> = FxMap::default();
    let mut args: Vec<Vec<usize>> = vec![];
    for q in qs {
        let m = if q.op()? == Op::Prefix { &mut prefix_ids } else { &mut term_ids };
        let mut v = vec![];
        for a in &q.args {
            let n = m.len();
            v.push(*m.entry(a.as_bytes().to_vec()).or_insert(n));
        }
        args.push(v);
    }
    let (tmin, tmax) = (term_ids.keys().map(Vec::len).min().unwrap_or(0), term_ids.keys().map(Vec::len).max().unwrap_or(0));
    let mut plens: Vec<usize> = prefix_ids.keys().map(Vec::len).collect();
    plens.sort_unstable();
    plens.dedup();
    let phrases: Vec<(usize, usize, usize)> = qs.iter().enumerate().filter(|(_, q)| q.class == "phrase").map(|(i, _)| (i, args[i][0], args[i][1])).collect();

    let mut tseen = vec![u32::MAX; term_ids.len()];
    let mut pseen = vec![u32::MAX; prefix_ids.len()];
    let mut phseen = vec![u32::MAX; qs.len()];
    let mut res: Vec<Vec<u32>> = vec![vec![]; qs.len()];
    let mut text = 0u64;
    let ops: Vec<Op> = qs.iter().map(Query::op).collect::<R<_>>()?;
    store.clear_cache();
    store.for_rows(4 << 20, &mut |s, row, pieces, extra| {
        text += extra;
        for &p in pieces {
            let pc = s.piece(p)?;
            text += pc.bytes().len() as u64;
            let mut prev: Option<usize> = None;
            tokenize(pc.bytes(), |t, long| {
                let mut cur = None;
                if !long {
                    if t.len() >= tmin && t.len() <= tmax {
                        if let Some(&i) = term_ids.get(t) {
                            tseen[i] = row;
                            cur = Some(i);
                        }
                    }
                    for &l in &plens {
                        if t.len() >= l {
                            if let Some(&i) = prefix_ids.get(&t[..l]) {
                                pseen[i] = row;
                            }
                        }
                    }
                }
                if let (Some(a), Some(b)) = (prev, cur) {
                    for &(qi, x, y) in &phrases {
                        if a == x && b == y {
                            phseen[qi] = row;
                        }
                    }
                }
                prev = cur;
            });
        }
        for (qi, op) in ops.iter().enumerate() {
            let has = |k: usize| tseen[args[qi][k]] == row;
            let hit = match op {
                Op::Term => has(0),
                Op::And => (0..args[qi].len()).all(has),
                Op::Or => has(0) || has(1),
                Op::Not => has(0) && !has(1),
                Op::Prefix => pseen[args[qi][0]] == row,
                Op::Phrase => phseen[qi] == row,
            };
            if hit {
                res[qi].push(row);
            }
        }
        Ok(())
    })?;
    Ok(Scanned { rows: res, text_bytes: text })
}

pub fn scan_cmd(store_dir: &Path, queries: &Path, cache: usize) -> R<()> {
    let t0 = Instant::now();
    let qs = read_queries(queries)?;
    let mut store = Store::open(store_dir, cache)?;
    let row_trace = store.row_trace();
    let t = Instant::now();
    let r = scan(&mut store, &qs)?;
    let scan_s = t.elapsed().as_secs_f64();
    for (q, rows) in qs.iter().zip(&r.rows) {
        let mut tr = 0u64;
        let mut last = u32::MAX;
        for &x in rows {
            if row_trace[x as usize] != last {
                tr += 1;
                last = row_trace[x as usize];
            }
        }
        println!("{}", obj(&[("id", crate::util::q(&q.id)), ("rows", rows.len().to_string()), ("traces", tr.to_string()), ("rows_sha256", crate::util::q(&rows_sha(rows)))]));
    }
    finish("scan", "scan", t0, vec![("queries", qs.len().to_string()), ("rows", store.rows().to_string()), ("scan_s", format!("{scan_s:.3}")), ("text_bytes", r.text_bytes.to_string()), ("text_mb_per_s", format!("{:.2}", r.text_bytes as f64 / 1e6 / scan_s.max(1e-9)))]);
    Ok(())
}
